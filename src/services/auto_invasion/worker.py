from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    ChannelsTooMuchError,
    FloodWaitError,
    UserChannelsTooMuchError,
    UserPrivacyRestrictedError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from src.db.repositories.auto_invasion_repository import AutoInvasionRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.session import SessionOwnerType, TelethonSession
from src.services.auto_invasion.backoff_calculator import (
    get_between_joins_delay,
    get_cycle_pause,
)
from src.services.auto_invasion.captcha_solver import solve_captcha
from src.services.auto_invasion.link_parser import parse_group_link
from src.services.telethon_manager import TelethonSessionManager


class AutoInvasionWorker:
    def __init__(
        self,
        invasion_repository: AutoInvasionRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
    ) -> None:
        self._invasion_repository = invasion_repository
        self._session_repository = session_repository
        self._session_manager = session_manager
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._group_locks: dict[str, asyncio.Lock] = {}

    async def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def activate(self, user_id: int) -> None:
        now = datetime.now(timezone.utc)
        await self._invasion_repository.set_active(user_id, True, started_at=now)
        if not self._task or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._worker_loop())

    async def deactivate(self, user_id: int) -> None:
        await self._invasion_repository.set_active(user_id, False)

    async def refresh_groups_for_session(self, user_id: int, session_id: str, groups: list[dict], *, replace: bool = False) -> None:
        """Refresh invasion groups tracking after broadcast_groups are updated.
        
        This should be called whenever broadcast_groups are changed to keep invasion_groups in sync.
        Syncs ALL sessions for the user since broadcast_groups are often loaded for all sessions.
        If `replace` is True, reset join status for the provided links to force re-check.
        """
        try:
            # Normalize links from the new groups
            normalized_links = []
            for group in groups:
                link_key = self._normalize_group_link(group)
                if link_key:
                    normalized_links.append(link_key)
            # Deduplicate links to avoid redundant operations
            if normalized_links:
                normalized_links = list(dict.fromkeys(normalized_links))
            
            # Get all sessions for this user to sync them all
            sessions = await self._get_user_sessions(user_id)
            # Clean up any entries under stale/invalid session_ids
            try:
                valid_session_ids = [s.session_id for s in sessions]
                if valid_session_ids:
                    await self._invasion_repository.cleanup_user_sessions(user_id, valid_session_ids)
            except Exception:
                pass

            for session in sessions:
                session_id = session.session_id
                # Sync: remove entries for groups that are no longer in broadcast_groups
                await self._invasion_repository.sync_session_groups(user_id, session_id, normalized_links)
                
                # Add new groups to invasion tracking
                for link_key in normalized_links:
                    await self._invasion_repository.add_group(user_id, session_id, link_key)

                # If full replacement, reset join status to re-validate membership
                if replace and normalized_links:
                    await self._invasion_repository.reset_join_status_for_session(user_id, session_id, normalized_links)
        except Exception:
            pass

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                active_users = await self._invasion_repository.get_active_users()
                if active_users:
                    tasks = [self._process_user(user_id) for user_id in active_users]
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # Random delay between 1-3 minutes before next processing cycle
                pause_seconds = random.uniform(60, 180)
                await asyncio.sleep(pause_seconds)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(10)

    async def _process_user(self, user_id: int) -> None:
        try:
            sessions = await self._get_user_sessions(user_id)
            if not sessions:
                return

            for session in sessions:
                groups = (session.metadata or {}).get("broadcast_groups", [])
                
                # Normalize links for all groups
                normalized_links = []
                for group in groups:
                    link_key = self._normalize_group_link(group)
                    if link_key:
                        normalized_links.append(link_key)
                
                # Sync invasion_groups with current broadcast_groups (removes outdated entries)
                await self._invasion_repository.sync_session_groups(user_id, session.session_id, normalized_links)
                
                # Add current groups to invasion tracking
                for link_key in normalized_links:
                    await self._invasion_repository.add_group(
                        user_id,
                        session.session_id,
                        link_key,
                    )

            has_pending = await self._invasion_repository.has_unjoined_groups(user_id)
            if not has_pending:
                return

            tasks = [self._process_session(user_id, session) for session in sessions]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    continue
        except Exception:
            pass

    async def _process_session(self, user_id: int, session: TelethonSession) -> None:
        try:
            groups = (session.metadata or {}).get("broadcast_groups", [])
            if not groups:
                return

            # Build list of unjoined groups with their normalized links
            unjoined_groups = []
            for group in groups:
                try:
                    link_key = self._normalize_group_link(group)
                    if not link_key:
                        continue

                    is_joined = await self._invasion_repository.is_group_joined(
                        user_id,
                        session.session_id,
                        link_key,
                    )
                    if not is_joined:
                        unjoined_groups.append((group, link_key))
                except Exception:
                    continue

            if not unjoined_groups:
                return

            # Process groups in cycles: 2-3 groups per cycle with 5-10 sec delays, then 15-20 min pause
            groups_to_retry = unjoined_groups.copy()
            
            while self._running:
                # If all groups processed, rebuild the list of unjoined groups
                if not groups_to_retry:
                    groups_to_retry = []
                    for group in groups:
                        try:
                            link_key = self._normalize_group_link(group)
                            if not link_key:
                                continue

                            is_joined = await self._invasion_repository.is_group_joined(
                                user_id,
                                session.session_id,
                                link_key,
                            )
                            if not is_joined:
                                groups_to_retry.append((group, link_key))
                        except Exception:
                            continue
                    
                    # If no groups left to retry, exit
                    if not groups_to_retry:
                        break
                
                # Pick 2-3 random groups for this cycle
                cycle_size = min(random.randint(2, 3), len(groups_to_retry))
                groups_in_cycle = groups_to_retry[:cycle_size]
                
                joined_count = 0
                for group, link_key in groups_in_cycle:
                    if not self._running:
                        return
                    
                    try:
                        # Double-check still unjoined
                        is_joined = await self._invasion_repository.is_group_joined(
                            user_id,
                            session.session_id,
                            link_key,
                        )
                        if is_joined:
                            # Already joined, remove from retry list
                            groups_to_retry = [g for g in groups_to_retry if g[1] != link_key]
                            continue

                        # Try to join this group
                        actually_joined = await self._process_group(
                            user_id,
                            session,
                            group,
                            link_key,
                        )
                        
                        if actually_joined:
                            joined_count += 1
                        
                        # Remove from current retry list (will be re-checked in next full cycle if needed)
                        groups_to_retry = [g for g in groups_to_retry if g[1] != link_key]
                        
                        # Delay before next join attempt (5-10 sec)
                        if groups_in_cycle.index((group, link_key)) < len(groups_in_cycle) - 1:
                            await asyncio.sleep(get_between_joins_delay())
                    
                    except Exception:
                        # Error during join - remove from current list, will retry in next full cycle
                        groups_to_retry = [g for g in groups_to_retry if g[1] != link_key]
                        continue
                
                # After processing cycle, pause 15-20 minutes before next cycle
                if self._running:
                    pause_seconds = get_cycle_pause()
                    await asyncio.sleep(pause_seconds)
        
        except Exception:
            pass

    def _get_group_lock(self, link: str) -> asyncio.Lock:
        """Get or create a lock for a specific group link to prevent concurrent join attempts."""
        if link not in self._group_locks:
            self._group_locks[link] = asyncio.Lock()
        return self._group_locks[link]

    async def _process_group(
        self,
        user_id: int,
        session: TelethonSession,
        group: dict,
        link_key: str,
    ) -> bool:
        """Process a group join attempt.
        
        Returns:
            True if actually joined a new group
            False if couldn't join (already joined, error, etc.) - will be retried in next cycle
        """
        link = group.get("link")
        username = group.get("username")
        if not link_key and not username:
            return False

        parsed = parse_group_link(link or link_key)
        if not parsed:
            return False

        # Acquire lock for this group to prevent concurrent join attempts from different sessions
        group_lock = self._get_group_lock(link_key)
        async with group_lock:
            # Double-check: session might have joined while waiting for lock
            is_already_joined = await self._invasion_repository.is_group_joined(
                user_id,
                session.session_id,
                link_key,
            )
            if is_already_joined:
                return False

            client = await self._get_user_client(session)
            if not client or not client.is_connected():
                return False

            # Check if already a member of this group before attempting to join
            already_member = False
            try:
                if parsed.link_type == "public":
                    entity = await client.get_entity(parsed.username)
                    # Check if already participant
                    try:
                        participant = await client.get_permissions(entity)
                        if participant and not participant.is_banned:
                            # Already a member, mark as joined and return False
                            already_member = True
                    except Exception:
                        pass  # Not a member or can't check, proceed to join
            except Exception:
                # Can't get entity - invalid link, mark as joined to skip it in future
                await self._invasion_repository.mark_joined(user_id, session.session_id, link_key)
                return False

            if already_member:
                # Just update DB, no actual join happened
                await self._invasion_repository.mark_joined(user_id, session.session_id, link_key)
                return False

            # Small delay before actual join attempt
            await asyncio.sleep(0.5)

            joined = False
            try:
                if parsed.link_type == "public":
                    await client(JoinChannelRequest(parsed.username))
                else:
                    await client(ImportChatInviteRequest(parsed.invite_hash))
                joined = True
            except (UserPrivacyRestrictedError, ChannelPrivateError):
                # Can't join this group, mark as joined to skip it in future
                await self._invasion_repository.mark_joined(user_id, session.session_id, link_key)
                return False
            except (ChannelsTooMuchError, UserChannelsTooMuchError):
                # Hit the limit of joined channels - pause for 15-25 minutes
                pause_seconds = random.uniform(15 * 60, 25 * 60)
                await asyncio.sleep(pause_seconds)
                return False
            except FloodWaitError as error:
                await asyncio.sleep(error.seconds + 10)
                return False
            except Exception:
                # Unknown error, skip for now but don't mark as joined (will retry in next cycle)
                return False

            if not joined:
                return False

            if link_key:
                await self._invasion_repository.mark_joined(user_id, session.session_id, link_key)

            await asyncio.sleep(random.uniform(2, 5))

            chat_id: Optional[int] = None
            try:
                if parsed.link_type == "public":
                    entity = await client.get_entity(parsed.username)
                    chat_id = entity.id
            except Exception:
                chat_id = None

            if chat_id is None:
                return True  # Joined but can't solve captcha, still count as actual join

            try:
                await solve_captcha(client, chat_id)
            except Exception:
                pass

            return True  # Actually joined a new group

    async def _get_user_sessions(self, user_id: int) -> list[TelethonSession]:
        try:
            return await self._session_repository.get_active_sessions_for_owner(
                user_id,
                SessionOwnerType.USER,
            )
        except Exception:
            return []

    async def _get_user_client(self, session: TelethonSession) -> Optional[TelegramClient]:
        try:
            client = await self._session_manager.acquire_shared_client(session)
            if client and client.is_connected():
                return client
        except Exception:
            return None
        return None

    @staticmethod
    def _normalize_group_link(group: dict) -> Optional[str]:
        link = group.get("link")
        if isinstance(link, str) and link.strip():
            return link.strip()

        username = group.get("username")
        if isinstance(username, str) and username.strip():
            normalized = username.strip()
            if normalized.startswith("@"):
                normalized = normalized[1:]
            if normalized:
                return f"https://t.me/{normalized}"
        return None
