# Telegram Broadcast Bot

> Production-ready Telegram broadcast assistant built with [Telethon](https://github.com/LonamiWebs/Telethon), MongoDB, and async Python. Supports multi-account campaigns, periodic auto-broadcasts, and rich operator workflows in Russian.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Setup](#database-setup)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Best Practices](#best-practices)
- [Logging and Debugging](#logging-and-debugging)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview
This project delivers a Telegram operator bot capable of managing multiple user accounts, manual broadcasts, and scheduled auto-broadcasts from a single interface. Operators authenticate user accounts, upload group lists from Excel, craft content, and trigger campaigns directly in Telegram while the backend persists state in MongoDB and enforces delivery safeguards.

## Features
- **Multi-account management** – onboard accounts via phone/SMS or QR, verify health, and deactivate safely.
- **Manual broadcasts** – per-account text/image templates, deduplicated group delivery, live progress UI, cancellation support, and retries on recoverable errors.
- **Auto-broadcast engine** – interval-based scheduler with jitter, Mongo-backed locks, pause/resume/stop controls, and per-cycle notifications.
- **Group list tooling** – upload `.xlsx`/`.xls` files, deduplicate by username/link/chat ID, preview saved lists, validate membership, and keep per-account stats.
- **Russian operator UX** – all bot prompts, confirmations, and progress updates localized in Russian.
- **Observability** – structured logging, runtime account status cache, and Mongo persistence for tasks, sessions, and user metadata.

## Architecture
```
src/
├── app.py                   # Dependency wiring and lifecycle
├── main.py                  # CLI entrypoint (python -m src)
├── bot/                     # Telethon command handlers and keyboards
│   ├── application.py       # Bot bootstrap (client + context)
│   ├── commands/            # /start, /broadcast, /auto_schedule, etc.
│   ├── context.py           # Shared repositories/services for handlers
│   └── keyboards.py         # Reply and inline keyboards (RU captions)
├── config/                  # Settings and broadcast timing defaults
│   ├── settings.py          # Pydantic settings sourced from .env
│   └── broadcast_settings.py# Delay/batch constants for manual sends
├── db/                      # Mongo client and repositories
│   ├── client.py            # AsyncIOMotor connection manager
│   └── repositories/        # Users, sessions, auto tasks, accounts
├── models/                  # Pydantic domain models (users, sessions, tasks)
├── services/                # Business logic (account status, auto engine)
│   ├── telethon_manager.py  # Session lifecycle + temporary clients
│   ├── account_status.py    # Health checks & cache for account status
│   ├── broadcast_state.py   # FSM for manual broadcast flows
│   ├── groups_state.py      # FSM for group uploads/previews
│   └── auto_broadcast/      # Scheduler, runner, payload utilities
└── utils/logging.py         # DictConfig-based logging setup
```
Key Mongo collections (defaults) live under: `users`, `telethon_sessions`, `auto_broadcast_tasks`, `auto_accounts`.

## Prerequisites
- **Python ≥ 3.10** (3.11 recommended for best Telethon compatibility).
- **MongoDB 5.x+** reachable from the bot host.
- A **Telegram API ID/Hash** and **Bot Token** created via [my.telegram.org](https://my.telegram.org/apps) and [BotFather](https://t.me/BotFather).
- libffi/libssl headers when building Telethon on Linux (install via package manager).

## Installation
1. **Clone the repository** (or open in VS Code/GitHub Codespaces).
2. **Create a virtual environment** (Windows Git Bash commands shown):
   ```bash
   python -m venv .venv
   source .venv/Scripts/activate
   ```
   > PowerShell: `./.venv/Scripts/Activate.ps1`, CMD: `./.venv/Scripts/activate.bat`.
3. **Install Python dependencies**:
   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```
4. **Copy environment template** and fill in credentials:
   ```bash
   cp .env.example .env
   ```

## Configuration
Application settings are sourced from `.env` (loaded via `pydantic-settings`).

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_API_ID` | ✅ | – | Telegram API ID for Telethon clients. |
| `TELEGRAM_API_HASH` | ✅ | – | Telegram API hash paired with the API ID. |
| `TELEGRAM_BOT_TOKEN` | ✅ | – | Bot token issued by BotFather. |
| `MONGO_DSN` | ✅ | – | MongoDB connection string (`mongodb://user:pass@host:port/db`). |
| `MONGO_DATABASE` | ✅ | – | Database name where collections will be created. |
| `APP_NAME` | ❌ | `telegram-broadcast-bot` | Used for Mongo driver telemetry and worker names. |
| `BOT_SESSION_NAME` | ❌ | `bot_session` | Filename for the bot’s saved session (`bot_session.session`). |
| `USER_COLLECTION` | ❌ | `users` | Mongo collection storing bot users (operators). |
| `SESSION_COLLECTION` | ❌ | `telethon_sessions` | Collection for stored Telethon user sessions. |
| `AUTO_TASK_COLLECTION` | ❌ | `auto_broadcast_tasks` | Collection for auto-broadcast definitions. |
| `AUTO_ACCOUNT_COLLECTION` | ❌ | `auto_accounts` | Collection for account runtime status. |
| `AUTO_TASK_POLL_INTERVAL` | ❌ | `15` | Seconds between scheduler polls for due auto tasks. |
| `AUTO_TASK_LOCK_TTL` | ❌ | `180` | Seconds a worker keeps an auto-task lock before refresh. |
| `LOG_LEVEL` | ❌ | `INFO` | Root logger level (`DEBUG`, `INFO`, etc.). |
| `ACCOUNT_STATUS_CONCURRENCY` | ❌ | `10` | Parallel Telethon checks when validating accounts. |
| `ACCOUNT_STATUS_TIMEOUT_SECONDS` | ❌ | `2.0` | Timeout per account health check. |
| `ACCOUNT_STATUS_CACHE_TTL_SECONDS` | ❌ | `20.0` | Cache TTL for account status snapshot. |
| `ACCOUNT_STATUS_DB_REFRESH_SECONDS` | ❌ | `180.0` | How often account health is refreshed from Mongo. |

> Sensitive values (`TELEGRAM_API_HASH`, `TELEGRAM_BOT_TOKEN`, Mongo credentials) should not be committed to VC. Store secrets securely in deployment environments.

## Database Setup
No manual migrations are required: repositories call `ensure_indexes()` on boot.

| Collection | Indexes | Purpose |
|------------|---------|---------|
| `users` | `telegram_id` (unique) | Track operators interacting with the bot. |
| `telethon_sessions` | `session_id` (unique), `(owner_id, owner_type)` | Persist Telethon session strings, broadcast materials, group lists, and status metadata. |
| `auto_broadcast_tasks` | `task_id` (unique), `(user_id, status)`, `next_run_ts`, `enabled`, `locked_by` | Manage scheduled tasks, worker locks, and cycle stats. |
| `auto_accounts` | `account_id` (unique), `(owner_id, status)`, `cooldown_until` | Cache account runtime state for auto broadcasts. |

Ensure the Mongo user has read/write access to the configured database. For production, enable authentication, TLS, and appropriate backup/monitoring.

## Quick Start
1. Activate the virtual environment.
2. Confirm `.env` contains valid Telegram and Mongo credentials.
3. Launch the bot:
   ```bash
   python -m src
   ```
4. Open Telegram and start a conversation with your bot. Use `/start` to display the main menu and register your operator profile.

> Telethon persists the bot session in `bot_session.session`. Keep this file alongside `.env` on the deployment host.

## Usage
### Command Reference Snapshot
| Purpose | Command or Button |
|---------|-------------------|
| Start menu & onboarding | `/start` |
| Authenticate account (SMS) | `/login_phone` |
| Authenticate account (QR) | `/login_qr` |
| List & detach accounts | `/accounts` |
| Add/replace broadcast text | `/add_text` |
| Add/replace broadcast image | `/add_image` |
| Upload group lists | `/upload_groups` |
| View saved groups | `/view_groups` |
| Preview materials | `/view_broadcast` |
| Launch manual broadcast | `/broadcast` |
| Configure auto-broadcast | `Авторассылка` button or `/auto_schedule` |
| Auto-broadcast status | `Статус авторассылки` button or `/auto_status` |
| Pause/resume/stop auto tasks | `/auto_pause`, `/auto_resume`, `/auto_stop` |
| Toggle cycle notifications | `/auto_notify_on`, `/auto_notify_off` |

### Manual Broadcast Walkthrough
1. **Add content** (text and/or image):
   ```text
   Вы: /add_text
   Бот: Для каких аккаунтов сохранить текст рассылки? Выберите нужный вариант ниже.
   Вы: Все аккаунты
   Бот: Отправьте текст, который будем использовать для рассылки по всем аккаунтам.
   Вы: ⚡️ Новое спецпредложение! Подробности ниже.
   Бот: Текст для рассылки сохранён. Вы можете изменить его командой /add_text или продолжить с выбранными аккаунтами.
   ```
   Add an image similarly with `/add_image` (bot prompts you to send a photo or document).

2. **Upload groups** (per account or all accounts):
   ```text
   Вы: /upload_groups
   Бот: Выберите, для каких аккаунтов загрузить список групп.
   Вы: Один аккаунт → @sales_manager (+7 999 888‑77‑66)
   Бот: Отправьте Excel-файл (.xlsx или .xls) с колонками «Название», «Username», «Ссылка».
   Вы: [загружает файл groups.xlsx]
   Бот: Найдено 180 строк (уникальных групп: 142). Заменить текущий список? [✅ Да / ❌ Нет]
   ```
   After confirmation the bot stores deduplicated groups and records stats per account.

3. **Launch the broadcast**:
   ```text
   Вы: /broadcast
   Бот: Выберите, с каких аккаунтов отправлять рассылку. [Один аккаунт / Все аккаунты]
   ...
   Бот: Будет отправлено в 142 уникальные группы. Материалы: текст — есть, картинка — нет. Оценочное время: ≈ 18 мин. Готовы начать?
   Вы: ✅ Начать
   Бот: Рассылка запущена
   Бот: Отправлено: 57 / 142 · Успешно: 55 · Неудачно: 2 · Текущий аккаунт: @sales_manager · Текущий чат: @telegram_group
   ```
   Use the inline button **«❌ Отмена рассылки»** to stop gracefully. Final summary is delivered when completed or canceled.

### Auto-Broadcast Walkthrough
1. **Open the scheduler**:
   ```text
   Вы: Авторассылка
   Бот: Выберите режим: • Один аккаунт • Все аккаунты
   ```
2. **Choose scope and interval**:
   ```text
   Бот: Как часто повторять рассылку? Укажите интервал в формате ЧЧ:ММ:СС. Максимум — 168:00:00.
   Вы: 02:30:00
   Бот: Проверяем списки групп и материалы... Всё готово. Запускать авторассылку каждые 2 ч 30 мин?
   Вы: ✅ Создать
   ```
3. **Monitor and control**:
   ```text
   Вы: /auto_status
   Бот: Активные авторассылки:
   • task_7f03d945 — каждые 02:30:00, следующий запуск ≈ 14:20 (МСК). Отправлено: 480, Ошибок: 3.
   Вы: /auto_pause task_7f03d945
   Бот: Задача поставлена на паузу.
   Вы: /auto_resume task_7f03d945
   Бот: Задача возобновлена.
   ```
   Enable per-cycle notifications via `/auto_notify_on <task_id>` to receive a Russian summary after each run (sent by the bot).

### Group File Uploads
- Accepted formats: `.xlsx`, `.xls` (first sheet is used).
- Expected columns (case-insensitive): `Название`, `Username`, `Ссылка`. Extra columns are ignored; header rows are auto-detected.
- Links are normalized (`https://t.me/...`), usernames sanitized (no `@`), and duplicates removed per account using chat ID/username/link precedence.
- Groups marked as `is_member = false` in metadata are skipped automatically.
- Per-account stats stored in `metadata.broadcast_groups_stats` (file rows, unique groups, actual targets) and used for progress estimates.

### Managing Accounts
- `/login_phone` walks through SMS code and optional 2FA password.
- `/login_qr` generates rotating QR codes (with refresh button) and supports ignored IDs to prevent reconnecting existing accounts.
- `/accounts` shows live status per account, performs background health refresh, and exposes inline «Отвязать» actions.
- When an account loses access during a broadcast, the bot deactivates it, logs the reason, and prompts you to reauthenticate.

## Best Practices
- **Separate workloads per account**: keep marketing, support, and sales accounts distinct to avoid Telegram rate limits. Use `/upload_groups` scope selection to tailor group lists.
- **Verify membership before campaigns**: the bot skips groups flagged as inaccessible; periodically refresh lists by rerunning `/upload_groups`.
- **Avoid duplicates**: the deduplication engine merges entries by chat ID, username, and normalized link. For multi-account broadcasts, ensure Excel files contain unique rows to shorten runtime.
- **Throttle auto-broadcasts wisely**: choose intervals longer than the estimated manual cycle (`AUTO_TASK_LOCK_TTL` + broadcast duration) to avoid overlapping runs.
- **Track bot logs**: set `LOG_LEVEL=DEBUG` during staging to inspect Telethon responses and Mongo operations. Reset to `INFO` for production.
- **Back up session files**: `bot_session.session` and Mongo collections are critical for continuity. Store encrypted backups off-host.

## Logging and Debugging
- Logging is configured via `utils.logging.configure_logging()`. Output format: `timestamp | level | logger | message`.
- Adjust verbosity with `LOG_LEVEL`. Example for verbose troubleshooting:
  ```bash
  LOG_LEVEL=DEBUG python -m src
  ```
- Telethon exceptions are logged with context (user ID, account label, chat identity). Search for keywords such as `AuthKeyUnregisteredError` or `DialogsFetchError` to spot account or access issues.
- Mongo errors are surfaced in the console; ensure the DSN is reachable and credentials are valid.

## Troubleshooting
- **Duplicate or missing group sends**: re-upload the Excel file to regenerate deduplicated lists; check bot output for skipped groups (`нет доступа`).
- **Login failures**: verify the phone format (`+79998887766`), 2FA password, and ensure the account is not banned (`Телеграм отклонил номер`). If QR sign-in stalls, press «🔄 Обновить QR» to refresh.
- **Auto-broadcast not firing**: confirm `/auto_status` shows the task as `running`; check Mongo `auto_broadcast_tasks.next_run_ts` and ensure the worker process is running with `AUTO_TASK_POLL_INTERVAL` < interval.
- **Immediate auto stop**: Telegram may throttle accounts; look for messages like `Аккаунт ... стал неактивным`. Re-login (`/login_phone`) to restore the session.
- **Mongo connectivity**: if the bot exits immediately, validate `MONGO_DSN` and network access. TLS-required clusters need `?tls=true` and CA certs.
- **Unicode errors on Windows console**: run in PowerShell/Core with UTF-8: `chcp 65001` before launching.

## Contributing
1. Fork the repository and create a feature branch.
2. Follow existing code style (type hints, dataclasses, async/await discipline).
3. Add in-line comments only when logic is non-obvious (keep them concise).
4. Provide manual test notes or unit tests for new behaviour (especially around broadcasts and Mongo repositories).
5. Submit a PR describing the feature and any env/database changes.

## License
No license file is provided. All rights reserved by the repository owner. Contact the maintainer before redistributing or deploying commercially.
