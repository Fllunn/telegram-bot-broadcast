# Telegram Broadcast Bot

Boilerplate for a production-ready Telegram bot that manages multiple user accounts via [Telethon](https://github.com/LonamiWebs/Telethon) and stores state in MongoDB.

## Features

- Async architecture built on top of Telethon
- MongoDB persistence (Motor) for user, session, and auto-task data
- Centralized settings management powered by `.env`
- Modular design (`bot/`, `db/`, `services/`, `models/`, `utils/`)
- Structured logging configuration
- Multi-account onboarding via phone number and QR code (with 2FA support)
- Manual broadcast workflow with per-account text, image storage, and progress tracking
- Excel-based group list uploads with validation and inline preview commands
- Periodic auto-broadcast scheduler with MongoDB persistence, crash-safe restarts, and runtime controls (pause/resume/stop)

## Getting Started

1. **Install dependencies**

   ```bash
   python -m venv .venv
   ```

   Activate the virtual environment:

   - macOS/Linux (bash/zsh): `source .venv/bin/activate`
   - Windows PowerShell: `.\.venv\Scripts\Activate.ps1`
   - Windows Command Prompt: `.\.venv\Scripts\activate.bat`
   - Windows Git Bash: `source .venv/Scripts/activate`

   Once activated, install Python packages with `python -m pip install -r requirements.txt`.

2. **Configure environment**

   ```bash
   cp .env.example .env
   # Fill in TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN, etc.
   ```

   Required variables:

   - `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_BOT_TOKEN`
   - `MONGO_DSN`, `MONGO_DATABASE`
   - Optional overrides: `AUTO_TASK_COLLECTION`, `AUTO_ACCOUNT_COLLECTION`, `AUTO_TASK_POLL_INTERVAL`, `AUTO_TASK_LOCK_TTL`

3. **Run the bot**

   ```bash
   python -m src
   ```

## Auto Broadcast Controls

- `Автозадача` (кнопка в меню) — запустить мастера настройки периодической рассылки и выбрать аккаунты.
- `/auto_status` — показать активные автозадачи, следующий запуск и статистику.
- `/auto_pause <task_id>` и `/auto_resume <task_id>` — временно остановить или возобновить автозадачу.
- `/auto_stop <task_id>` — остановить автозадачу окончательно.
- `/auto_notify_on <task_id>` и `/auto_notify_off <task_id>` — включить или отключить уведомления о каждом цикле.

## Project Layout

```
src/
├── app.py                  # Application wiring and lifecycle management
├── main.py                 # CLI entrypoint
├── __init__.py
├── bot/
│   ├── __init__.py
│   ├── application.py      # Bot runtime built on Telethon
│   ├── router.py           # Command registration orchestrator
│   └── commands/
│       ├── __init__.py
│       ├── account.py
│       ├── auto_broadcast.py
│       ├── broadcast.py
│       ├── cancel.py
│       ├── groups.py
│       ├── help.py
│       └── start.py
├── config/
│   ├── __init__.py
│   └── settings.py         # Pydantic-based settings management
├── db/
│   ├── __init__.py
│   ├── client.py           # Mongo connection manager
│   └── repositories/
│       ├── __init__.py
│       ├── account_repository.py
│       ├── auto_broadcast_task_repository.py
│       ├── session_repository.py
│       └── user_repository.py
├── models/
│   ├── __init__.py
│   ├── auto_broadcast.py
│   ├── session.py
│   └── user.py
├── services/
│   ├── __init__.py
│   ├── auto_broadcast/
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── runner.py
│   │   └── supervisor.py
│   ├── auth_state.py
│   ├── broadcast_state.py
│   ├── groups_state.py
│   └── telethon_manager.py
└── utils/
   ├── __init__.py
   └── logging.py
```

## Next Steps

- Extend command handlers with broadcast-specific business logic
- Wire the project into your deployment and observability stack
- Add automated tests covering onboarding and broadcast scenarios

Refer to inline comments for guidance on where to plug in your implementation.
