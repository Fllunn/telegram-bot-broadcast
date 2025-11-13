# Telegram Broadcast Bot

Boilerplate for a production-ready Telegram bot that manages multiple user accounts via [Telethon](https://github.com/LonamiWebs/Telethon) and stores state in MongoDB.

## Features

- Async architecture built on top of Telethon
- MongoDB persistence (Motor) for user and session data
- Centralized settings management powered by `.env`
- Modular design (`bot/`, `db/`, `services/`, `models/`, `utils/`)
- Structured logging configuration
- Multi-account onboarding via phone number and QR code (with 2FA support)
- Broadcast workflow with per-account text and image storage plus confirmation prompts
- In-bot preview of saved broadcast materials (text and images)

## Getting Started

1. **Install dependencies**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure environment**

   ```bash
   cp .env.example .env
   # Fill in TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN, etc.
   ```

3. **Run the bot**

   ```bash
   python -m src
   ```

## Project Layout

```
src/
├── app.py                 # Application wiring and lifecycle management
├── main.py                # CLI entrypoint
├── __init__.py
├── bot/
│   ├── __init__.py
│   ├── application.py     # Bot runtime built on Telethon
│   ├── router.py          # Command registration orchestrator
│   └── commands/
│       ├── __init__.py
│       ├── account.py     # Placeholder handlers for account management workflow
│       ├── help.py
│       └── start.py
├── config/
│   ├── __init__.py
│   └── settings.py        # Pydantic-based settings management
├── db/
│   ├── __init__.py
│   ├── client.py          # Mongo connection manager
│   └── repositories/
│       ├── __init__.py
│       ├── session_repository.py
│       └── user_repository.py
├── models/
│   ├── __init__.py
│   ├── session.py
│   └── user.py
├── services/
│   ├── __init__.py
│   └── telethon_manager.py  # Session lifecycle utilities for Telethon clients
└── utils/
    ├── __init__.py
    └── logging.py
```

## Next Steps

- Extend command handlers with broadcast-specific business logic
- Wire the project into your deployment and observability stack
- Add automated tests covering onboarding and broadcast scenarios

Refer to inline comments for guidance on where to plug in your implementation.
