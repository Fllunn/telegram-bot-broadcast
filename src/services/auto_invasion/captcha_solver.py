from __future__ import annotations

import asyncio
import re
from typing import Optional

from telethon import TelegramClient
from telethon.tl.custom import Message
from telethon.tl.types import MessageEntityCode


async def solve_captcha(client: TelegramClient, chat_id: int) -> bool:
    try:
        messages = await client.get_messages(chat_id, limit=5)
        
        for msg in messages:
            if not msg:
                continue
                
            if msg.reply_markup and msg.reply_markup.rows:
                success = await _try_click_button(client, msg)
                if success:
                    return True
            
            if msg.message:
                answer = _try_solve_text_captcha(msg.message)
                if answer:
                    await asyncio.sleep(1)
                    await client.send_message(chat_id, answer)
                    await asyncio.sleep(2)
                    return True
        
        return False
    except Exception:
        return False


async def _try_click_button(client: TelegramClient, msg: Message) -> bool:
    try:
        positive_keywords = [
            "✅", "✔", "✔️", "☑", "☑️", "👍", "👌", "✓",
            "да", "yes", "join", "вступить", "я не робот", "not a robot", "confirm",
            "продолжить", "далее", "ok", "подтвердить", "принять", "согласен", "войти",
            "continue", "next", "accept", "agree", "enter", "start", "proceed",
            "i'm not a robot", "human", "verify", "verification", "check", "press",
            "нажми", "кликни", "tap", "click"
        ]
        
        negative_keywords = ["нет", "no", "я робот", "i'm a robot", "bot", "cancel", "отмена"]
        
        all_buttons = []
        for row in msg.reply_markup.rows:
            for button in row.buttons:
                all_buttons.append(button)
        
        for button in all_buttons:
            button_text = button.text.lower()
            
            if any(neg in button_text for neg in negative_keywords):
                continue
            
            if any(keyword in button_text for keyword in positive_keywords):
                await asyncio.sleep(1)
                await msg.click(data=button.data if hasattr(button, "data") else None)
                await asyncio.sleep(2)
                return True
        
        if len(all_buttons) == 1:
            button = all_buttons[0]
            button_text = button.text.lower()
            if not any(neg in button_text for neg in negative_keywords):
                await asyncio.sleep(1)
                await msg.click(data=button.data if hasattr(button, "data") else None)
                await asyncio.sleep(2)
                return True
        
        return False
    except Exception:
        return False


def _try_solve_text_captcha(text: str) -> Optional[str]:
    text_lower = text.lower()
    
    math_pattern = r"(\d+)\s*[\+\-\*\/×÷]\s*(\d+)"
    match = re.search(math_pattern, text)
    if match:
        num1 = int(match.group(1))
        num2 = int(match.group(2))
        operator = text[match.start():match.end()]
        
        if "+" in operator:
            return str(num1 + num2)
        elif "-" in operator or "−" in operator:
            return str(num1 - num2)
        elif "*" in operator or "×" in operator:
            return str(num1 * num2)
        elif "/" in operator or "÷" in operator:
            if num2 != 0:
                return str(num1 // num2)
    
    return None
