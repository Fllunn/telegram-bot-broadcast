from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParsedLink:
    link: str
    link_type: str
    username: Optional[str] = None
    invite_hash: Optional[str] = None


def parse_group_link(link: str) -> Optional[ParsedLink]:
    link = link.strip()
    
    public_pattern = r"https?://t\.me/([a-zA-Z0-9_]{5,32})$"
    match = re.match(public_pattern, link)
    if match:
        username = match.group(1)
        return ParsedLink(link=link, link_type="public", username=username)
    
    private_pattern1 = r"https?://t\.me/\+([a-zA-Z0-9_-]+)$"
    match = re.match(private_pattern1, link)
    if match:
        invite_hash = match.group(1)
        return ParsedLink(link=link, link_type="private", invite_hash=invite_hash)
    
    private_pattern2 = r"https?://t\.me/joinchat/([a-zA-Z0-9_-]+)$"
    match = re.match(private_pattern2, link)
    if match:
        invite_hash = match.group(1)
        return ParsedLink(link=link, link_type="private", invite_hash=invite_hash)
    
    return None
