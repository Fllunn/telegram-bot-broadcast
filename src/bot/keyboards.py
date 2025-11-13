from __future__ import annotations

from telethon import Button

LOGIN_PHONE_LABEL = "Подключить аккаунт через номер"
LOGIN_QR_LABEL = "Подключить аккаунт через QR-код"
ACCOUNTS_LABEL = "Посмотреть все аккаунты"
ADD_TEXT_LABEL = "Добавить текст для рассылки"
ADD_IMAGE_LABEL = "Добавить картинку для рассылки"


def build_main_menu_keyboard() -> list[list[Button]]:
    """Reply keyboard with primary account management actions."""
    return [
        [Button.text(LOGIN_PHONE_LABEL, resize=True)],
        [Button.text(LOGIN_QR_LABEL, resize=True)],
        [Button.text(ACCOUNTS_LABEL, resize=True)],
        [Button.text(ADD_TEXT_LABEL, resize=True)],
        [Button.text(ADD_IMAGE_LABEL, resize=True)],
    ]
