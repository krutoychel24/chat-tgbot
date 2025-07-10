import asyncio
import json
import logging
import os
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, TelegramObject

TOKEN = ""
DOCS_URL = "https://telegra.ph/WombatCombat---help-06-28"

DB_FILE = "wombat.db"
CHART_FILE = "chart.png"
GROW_COOLDOWN_HOURS = 24
TAG_COOLDOWN_SECONDS = 10
EVENT_CHANCE = 20
SPAM_COOLDOWN_SECONDS = 2
DUEL_ACCEPT_TIMEOUT_SECONDS = 60
PRESTIGE_REQUIREMENT = 100 

HUMILIATION_PHRASES = [
    "Молчать, приговоренный!", "Осужденный не имеет права голоса.", "Слово тебе не давали, червь.",
    "Твое место у параши, а не в командах.", "Петушок, не чирикай!",
    "Тише будь, а то вилкой в глаз кольну.", "Твой жалкий лепет здесь никого не интересует."
]

dp = Dispatcher()
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
user_last_message_time = {}


# --- Database Functions ---
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER, chat_id INTEGER, first_name TEXT, username TEXT,
                    size INTEGER DEFAULT 0, nickname TEXT, last_growth TEXT, status TEXT DEFAULT 'normal',
                    condemned_by INTEGER, punishment_end_time TEXT, executed_at TEXT,
                    size_before_execution INTEGER DEFAULT 0, medals INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, chat_id)
                )
            ''')
        try:
            # Backwards compatibility check for 'medals' column
            cursor.execute("SELECT medals FROM users LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN medals INTEGER DEFAULT 0")
            logging.info("Column 'medals' added to 'users' table.")

        cursor.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id INTEGER PRIMARY KEY, last_event TEXT, last_tag_time TEXT,
                    active_duel_json TEXT, active_trial_json TEXT
                )
            ''')
        conn.commit()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def db_query(query, params=(), fetchone=False, fetchall=False, commit=True):
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = None
        if fetchone:
            result = cursor.fetchone()
        if fetchall:
            result = cursor.fetchall()
        if commit:
            conn.commit()
    return result


# A simple middleware to prevent command spamming from a single user.
@dp.message.middleware()
async def anti_spam_middleware(
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.Message,
        data: Dict[str, Any]
) -> Any:
    user_id = event.from_user.id
    current_time = datetime.now()
    if user_id in user_last_message_time:
        time_diff = current_time - user_last_message_time[user_id]
        if time_diff < timedelta(seconds=SPAM_COOLDOWN_SECONDS):
            return
    user_last_message_time[user_id] = current_time
    return await handler(event, data)


# Helper to get target user ID from a reply or a @username mention.
async def get_target_id_from_message(message: types.Message, chat_id: int) -> str | None:
    if message.reply_to_message and not message.reply_to_message.from_user.is_bot:
        return str(message.reply_to_message.from_user.id)
    if message.entities:
        for entity in message.entities:
            if entity.type in ['mention', 'text_mention']:
                if entity.type == 'text_mention' and entity.user:
                    return str(entity.user.id)
                elif entity.type == 'mention':
                    mentioned_username = message.text[entity.offset + 1:entity.offset + entity.length].lower()
                    user = db_query("SELECT user_id FROM users WHERE LOWER(username) = ? AND chat_id = ?",
                                    (mentioned_username, chat_id), fetchone=True)
                    if user:
                        return str(user['user_id'])
    return None


@dp.message(CommandStart())
async def command_start_handler(message: types.Message) -> None:
    chat_id, user_id = message.chat.id, message.from_user.id
    first_name, username = message.from_user.first_name, message.from_user.username
    user = db_query("SELECT 1 FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user:
        initial_growth = random.randint(1, 10)
        db_query(
            "INSERT INTO users (chat_id, user_id, first_name, username, size, last_growth, status, medals) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, first_name, username, initial_growth, datetime.now().isoformat(), 'normal', 0)
        )
        await message.answer(
            f"Привет, {first_name}! 👋\nЗдарова! Твой вомбат сразу вырос на {initial_growth} см!\n"
            f"<b>Текущий размер: {initial_growth} см.</b>\nНапиши /help, чтобы увидеть список команд."
        )
    else:
        db_query("UPDATE users SET first_name = ?, username = ? WHERE user_id = ? AND chat_id = ?",
                 (first_name, username, user_id, chat_id))
        await message.answer(f"{first_name}, ты уже в игре! 😉")


@dp.message(Command("help"))
async def command_help_handler(message: types.Message):
    text = "Полная документация по боту доступна по ссылке ниже.\nFull documentation for the bot is available at the link below."
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📜 Документация / Documentation", url=DOCS_URL)]])
    await message.answer(text, reply_markup=keyboard, disable_web_page_preview=True)


async def handle_humiliation(message: types.Message, user_data: dict):
    if user_data and user_data.get("status") == "condemned":
        await message.reply(random.choice(HUMILIATION_PHRASES))
        return True
    return False


@dp.message(Command("grow"))
async def command_grow_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data: await message.answer("Сначала напиши /start"); return
    if await handle_humiliation(message, user_data): return
    last_growth_str = user_data.get("last_growth")
    if last_growth_str:
        last_growth_time = datetime.fromisoformat(last_growth_str)
        cooldown = timedelta(hours=GROW_COOLDOWN_HOURS)
        if datetime.now() < last_growth_time + cooldown:
            time_left = (last_growth_time + cooldown) - datetime.now()
            h, rem = divmod(int(time_left.total_seconds()), 3600);
            m, _ = divmod(rem, 60)
            await message.answer(f"Следующий рост будет доступен через: {h} ч. {m} мин.")
            return

    medals = user_data.get("medals", 0)
    if random.randint(1, 100) <= 5:
        growth = 0
    else:
        if medals > 0:
            bonus = medals - 1
            growth = random.randint(5 + bonus, 20 + bonus)
        else:
            growth = random.randint(1, 10)

    if growth == 0:
        db_query("UPDATE users SET last_growth = ? WHERE user_id = ? AND chat_id = ?",
                 (datetime.now().isoformat(), user_id, chat_id))
        await message.answer("К сожалению, твой вомбат сегодня не вырос. Попробуй завтра!")
    else:
        new_size = user_data.get("size", 0) + growth
        db_query("UPDATE users SET size = ?, last_growth = ? WHERE user_id = ? AND chat_id = ?",
                 (new_size, datetime.now().isoformat(), user_id, chat_id))
        await message.answer(f"Твой вомбат вырос на +{growth} см! 📏\nТеперь его длина: {new_size} см.")


@dp.message(Command("prestige"))
async def command_prestige_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer("Сначала напиши /start")
        return
    if await handle_humiliation(message, user_data):
        return

    current_size = user_data.get("size", 0)

    if current_size >= PRESTIGE_REQUIREMENT:
        new_medals = user_data.get("medals", 0) + 1
        new_size = 5
        db_query(
            "UPDATE users SET size = ?, medals = medals + 1 WHERE user_id = ? AND chat_id = ?",
            (new_size, user_id, chat_id)
        )
        await message.answer(
            f"🏅 <b>ПРЕСТИЖ!</b> 🏅\n\n"
            f"Поздравляем, ты достиг {PRESTIGE_REQUIREMENT} см и сбросил свой рост ради медали!\n"
            f"Твой размер теперь: <b>{new_size} см</b>.\n"
            f"Всего медалей: <b>{new_medals}</b>."
        )
    else:
        needed = PRESTIGE_REQUIREMENT - current_size
        await message.answer(
            f"Для сброса и получения медали нужно достичь {PRESTIGE_REQUIREMENT} см.\n"
            f"Тебе не хватает еще {needed} см."
        )


@dp.message(Command("top"))
async def command_top_handler(message: types.Message):
    chat_id = message.chat.id
    sorted_users = db_query("SELECT * FROM users WHERE chat_id = ? ORDER BY size DESC LIMIT 15", (chat_id,),
                            fetchall=True)
    if not sorted_users: await message.answer("В этом чате еще никто не играет."); return
    players = [user.get('nickname') or user.get('first_name', 'Unknown') for user in sorted_users]
    sizes = [user.get('size', 0) for user in sorted_users]
    plt.style.use('dark_background');
    fig, ax = plt.subplots()
    bars = ax.barh(players, sizes, color='#0088cc')
    ax.invert_yaxis();
    ax.set_xlabel('Размер (см)');
    ax.set_title('Топ вомбатов этого чата')
    ax.bar_label(bars, fmt='%d см', label_type='edge', color='white', padding=5)
    fig.tight_layout();
    plt.savefig(CHART_FILE, dpi=200, bbox_inches='tight');
    plt.close(fig)
    chart = FSInputFile(CHART_FILE)
    await message.answer_photo(chart, caption="Вот текущий рейтинг 🏆")
    if os.path.exists(CHART_FILE): os.remove(CHART_FILE)


@dp.message(Command("nickname"))
async def command_nickname_handler(message: types.Message, command: CommandObject):
    chat_id, user_id = message.chat.id, message.from_user.id
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data: await message.answer("Сначала напиши /start"); return
    if await handle_humiliation(message, user_data): return
    new_nickname = command.args
    if new_nickname:
        if len(new_nickname) > 20: await message.answer("Слишком длинное имя!"); return
        db_query("UPDATE users SET nickname = ? WHERE user_id = ? AND chat_id = ?", (new_nickname, user_id, chat_id))
        await message.answer(f"Отлично! Теперь твоего вомбата зовут: <b>{new_nickname}</b>")
    else:
        await message.answer("Чтобы дать имя, напиши команду так: /nickname [новое имя]")


@dp.message(Command("me"))
async def command_me_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data: await message.answer("Сначала напиши /start"); return
    all_users = db_query("SELECT * FROM users WHERE chat_id = ? ORDER BY size DESC", (chat_id,), fetchall=True)
    rank = next((i + 1 for i, u in enumerate(all_users) if u['user_id'] == user_id), len(all_users))

    response = f"👤 <b>Твой профиль:</b>\n"
    if user_data.get('nickname'): response += f"Имя вомбата: <b>{user_data['nickname']}</b>\n"

    medals = user_data.get('medals', 0)
    if medals > 0:
        response += f"Медали престижа: {medals} 🏅\n"

    response += f"Текущий размер: {user_data.get('size', 0)} см\n"
    response += f"Место в топе: {rank} из {len(all_users)}\n"
    if user_data.get('status') == 'condemned': response += f"Статус: <b>ОСУЖДЕН</b> 😡"
    await message.answer(response)


@dp.message(Command("duel"))
async def command_duel_handler(message: types.Message):
    chat_id, attacker_id = message.chat.id, message.from_user.id
    attacker_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (attacker_id, chat_id),
                             fetchone=True)
    if not attacker_data: await message.answer("Сначала напиши /start"); return
    if await handle_humiliation(message, attacker_data): return
    chat_info = db_query("SELECT active_duel_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    if chat_info and chat_info.get("active_duel_json"): await message.answer(
        "В чате уже идет вызов на дуэль! Подождите."); return
    defender_id = await get_target_id_from_message(message, chat_id)
    if not defender_id: await message.answer("Цель не найдена. Ответь на сообщение или упомяни игрока через @."); return
    if str(attacker_id) == defender_id: await message.answer("Нельзя драться с самим собой."); return
    defender_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (defender_id, chat_id),
                             fetchone=True)
    if not defender_data: await message.answer("Этот игрок еще не в игре."); return
    attacker_name, defender_name = attacker_data.get('first_name'), defender_data.get('first_name')
    duel_data = {"attacker_id": attacker_id, "defender_id": int(defender_id),
                 "end_time": (datetime.now() + timedelta(seconds=DUEL_ACCEPT_TIMEOUT_SECONDS)).isoformat()}
    db_query(
        "INSERT INTO chats (chat_id, active_duel_json) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET active_duel_json = excluded.active_duel_json",
        (chat_id, json.dumps(duel_data)))
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"duel_accept"),
         InlineKeyboardButton(text="❌ Отказаться", callback_data=f"duel_decline")]])
    msg = await message.answer(
        f"⚔️ <b>Вызов на дуэль!</b> ⚔️\n{attacker_name} бросает перчатку игроку {defender_name}!\nИсход решает удача (50/50). У тебя есть {DUEL_ACCEPT_TIMEOUT_SECONDS} секунд, чтобы принять вызов.",
        reply_markup=keyboard)
    duel_data['message_id'] = msg.message_id
    db_query("UPDATE chats SET active_duel_json = ? WHERE chat_id = ?", (json.dumps(duel_data), chat_id))


@dp.message(Command("casino"))
async def command_casino_handler(message: types.Message, command: CommandObject):
    chat_id, user_id = message.chat.id, message.from_user.id
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data: await message.answer("Сначала напиши /start"); return
    if await handle_humiliation(message, user_data): return
    bet_str = command.args
    if not bet_str or not bet_str.isdigit(): await message.answer("Укажи ставку числом: /casino 10"); return
    bet = int(bet_str)
    current_size = user_data.get("size", 0)
    if bet <= 0: await message.answer("Ставка должна быть больше нуля."); return
    if bet > current_size: await message.answer(
        f"Нельзя поставить больше, чем есть! Твой размер: {current_size} см."); return
    msg = await message.answer(f"<b>{user_data['first_name']}</b> ставит {bet} см... 🎲")
    await asyncio.sleep(3)

    change = 0
    if random.choice([True, False]):
        change = bet
        db_query("UPDATE users SET size = size + ? WHERE user_id = ? AND chat_id = ?", (change, user_id, chat_id))
        final_size = current_size + change
        await msg.edit_text(f"🎉 <b>ВЫИГРЫШ!</b> 🎉\nТы выиграл {bet} см! Твой новый размер: {final_size} см.")
    else:
        change = -bet
        db_query("UPDATE users SET size = size + ? WHERE user_id = ? AND chat_id = ?", (change, user_id, chat_id))
        final_size = current_size + change
        await msg.edit_text(f"😥 <b>ПРОИГРЫШ...</b> 😥\nТы потерял {bet} см! Твой новый размер: {final_size} см.")
    # --------------------------------------------------------------------------


@dp.message(Command("tag"))
async def command_tag_handler(message: types.Message):
    chat_id = message.chat.id
    chat_info = db_query("SELECT last_tag_time FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True) or {}
    last_tag_str = chat_info.get("last_tag_time")
    if last_tag_str:
        last_tag_time = datetime.fromisoformat(last_tag_str)
        if datetime.now() < last_tag_time + timedelta(seconds=TAG_COOLDOWN_SECONDS):
            await message.answer(f"Общий сбор можно будет объявить через несколько секунд.")
            return
    all_users = db_query("SELECT user_id, first_name FROM users WHERE chat_id = ?", (chat_id,), fetchall=True)
    if not all_users: await message.answer("В этом чате еще нет игроков для сбора."); return
    mentions = [f"<a href='tg://user?id={user['user_id']}'>{user.get('first_name', 'Player')}</a>" for user in
                all_users]
    await message.answer(f"📢 <b>ОБЩИЙ СБОР!</b> 📢\n{', '.join(mentions)}")
    db_query(
        "INSERT INTO chats (chat_id, last_tag_time) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET last_tag_time = excluded.last_tag_time",
        (chat_id, datetime.now().isoformat()))


@dp.message(Command("trial"))
async def command_trial_handler(message: types.Message):
    chat_id, prosecutor_id = message.chat.id, message.from_user.id
    prosecutor_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (prosecutor_id, chat_id),
                               fetchone=True)
    if not prosecutor_data: await message.answer("Сначала напиши /start"); return
    if await handle_humiliation(message, prosecutor_data): return
    chat_info = db_query("SELECT active_trial_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True) or {}
    if chat_info.get("active_trial_json"): await message.answer("В чате уже идет суд!"); return
    defendant_id = await get_target_id_from_message(message, chat_id)
    if not defendant_id: await message.answer("Цель не найдена."); return
    if str(prosecutor_id) == defendant_id: await message.answer("Нельзя судить самого себя."); return
    defendant_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (defendant_id, chat_id),
                              fetchone=True)
    if not defendant_data: await message.answer("Этот игрок еще не в игре"); return
    prosecutor_name, defendant_name = prosecutor_data['first_name'], defendant_data['first_name']
    trial_data = {"prosecutor_id": prosecutor_id, "defendant_id": int(defendant_id),
                  "end_time": (datetime.now() + timedelta(minutes=5)).isoformat(),
                  "votes": {"guilty": [], "innocent": []}}
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Виновен", callback_data="vote_guilty"),
                                                      InlineKeyboardButton(text="Невиновен",
                                                                           callback_data="vote_innocent")]])
    msg = await message.answer(
        f"⚖️ <b>СУД!</b> ⚖️\n{prosecutor_name} обвиняет {defendant_name}!\nГолосование длится 5 минут.",
        reply_markup=keyboard)
    trial_data['message_id'] = msg.message_id
    db_query(
        "INSERT INTO chats (chat_id, active_trial_json) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET active_trial_json = excluded.active_trial_json",
        (chat_id, json.dumps(trial_data)))


@dp.message(Command("execute"))
async def command_execute_handler(message: types.Message):
    chat_id, executioner_id = message.chat.id, message.from_user.id
    target_id = await get_target_id_from_message(message, chat_id)
    if not target_id: await message.answer("Цель для казни не найдена."); return
    target_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (target_id, chat_id), fetchone=True)
    executioner_data = db_query("SELECT first_name FROM users WHERE user_id = ? AND chat_id = ?",
                                (executioner_id, chat_id), fetchone=True)
    if not target_data or not executioner_data: return
    if target_data.get("status") == "condemned" and str(target_data.get("condemned_by")) == str(executioner_id):
        db_query(
            "UPDATE users SET size_before_execution = size, size = 0, status = 'executed', executed_at = ?, condemned_by = NULL, punishment_end_time = NULL WHERE user_id = ? AND chat_id = ?",
            (datetime.now().isoformat(), target_id, chat_id))
        await message.answer(
            f"☠️ <b>ПРИГОВОР ИСПОЛНЕН!</b>\n{executioner_data['first_name']} казнил {target_data['first_name']}. Его вомбат обнулен.")
    else:
        await message.answer("У вас нет права казнить этого игрока.")


@dp.message(Command("pardon"))
async def command_pardon_handler(message: types.Message):
    chat_id = message.chat.id
    target_id = await get_target_id_from_message(message, chat_id)
    if not target_id: await message.answer("Цель для помилования не найдена."); return
    target_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (target_id, chat_id), fetchone=True)
    if not target_data: return
    if target_data.get("status") == "executed" and target_data.get("executed_at"):
        executed_at = datetime.fromisoformat(target_data["executed_at"])
        if datetime.now() < executed_at + timedelta(minutes=30):
            db_query("UPDATE users SET size = ?, status = 'normal' WHERE user_id = ? AND chat_id = ?",
                     (target_data['size_before_execution'], target_id, chat_id))
            await message.answer(
                f"❤️ <b>МИЛОСЕРДИЕ!</b>\n{target_data['first_name']} был помилован. Его вомбат восстановлен!")
        else:
            await message.answer("Слишком поздно для милосердия.")


@dp.callback_query(F.data.startswith("vote_"))
async def process_vote_callback(callback: types.CallbackQuery):
    chat_id, user_id = callback.message.chat.id, callback.from_user.id
    chat_info = db_query("SELECT active_trial_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    if not chat_info or not chat_info['active_trial_json']: await callback.answer("Голосование завершено.",
                                                                                  show_alert=True); return
    trial = json.loads(chat_info['active_trial_json'])
    if str(user_id) in [str(trial["prosecutor_id"]), str(trial["defendant_id"])]: await callback.answer(
        "Обвинитель и обвиняемый не голосуют.", show_alert=True); return
    if any(user_id in v for v in trial["votes"].values()): await callback.answer("Вы уже проголосовали.",
                                                                                 show_alert=True); return
    vote = callback.data.split("_")[1]
    trial["votes"][vote].append(user_id)
    db_query("UPDATE chats SET active_trial_json = ? WHERE chat_id = ?", (json.dumps(trial), chat_id))
    await callback.answer(f"Ваш голос '{vote}' принят!")
    guilty_count, innocent_count = len(trial["votes"]["guilty"]), len(trial["votes"]["innocent"])
    defendant = db_query("SELECT first_name FROM users WHERE user_id = ? AND chat_id = ?",
                         (trial['defendant_id'], chat_id), fetchone=True)
    prosecutor = db_query("SELECT first_name FROM users WHERE user_id = ? AND chat_id = ?",
                          (trial['prosecutor_id'], chat_id), fetchone=True)
    await bot.edit_message_text(
        f"⚖️ <b>СУД!</b> ⚖️\n{prosecutor['first_name']} обвиняет {defendant['first_name']}!\nГолосование длится 5 минут.\n\n<b>Голоса 'Виновен': {guilty_count} | 'Невиновен': {innocent_count}</b>",
        chat_id, trial["message_id"], reply_markup=callback.message.reply_markup)


@dp.callback_query(F.data.startswith("set_term:"))
async def set_term_callback(callback: types.CallbackQuery):
    chat_id, prosecutor_id = callback.message.chat.id, callback.from_user.id
    _, defendant_id, hours = callback.data.split(":")
    defendant_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (defendant_id, chat_id),
                              fetchone=True)
    if not defendant_data or str(prosecutor_id) != str(defendant_data.get("condemned_by")): await callback.answer(
        "Только обвинитель может выбрать срок.", show_alert=True); return
    end_time = (datetime.now() + timedelta(hours=int(hours))).isoformat()
    db_query("UPDATE users SET punishment_end_time = ? WHERE user_id = ? AND chat_id = ?",
             (end_time, defendant_id, chat_id))
    days, hours_rem = divmod(int(hours), 24)
    await callback.message.edit_text(
        f"Приговор вынесен! {defendant_data['first_name']} осужден на {days} д. {hours_rem} ч.\nОн может быть казнен в любой момент до истечения срока.")


@dp.callback_query(F.data.startswith("duel_"))
async def process_duel_callback(callback: types.CallbackQuery):
    chat_id, user_id = callback.message.chat.id, callback.from_user.id
    chat_info = db_query("SELECT active_duel_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    if not chat_info or not chat_info['active_duel_json']: await callback.message.edit_text(
        "Этот вызов на дуэль уже недействителен."); return
    duel_data = json.loads(chat_info['active_duel_json'])
    if user_id != duel_data["defender_id"]: await callback.answer("Это не твой вызов!", show_alert=True); return
    action = callback.data.split("_")[1]
    attacker_id, defender_id = duel_data["attacker_id"], duel_data["defender_id"]
    attacker = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (attacker_id, chat_id), fetchone=True)
    defender = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (defender_id, chat_id), fetchone=True)
    if action == "decline":
        await callback.message.edit_text(
            f"{defender['first_name']} трусливо отказался от дуэли с {attacker['first_name']}.")
    elif action == "accept":
        await callback.message.edit_text(f"{defender['first_name']} принимает вызов! Бой начинается...")
        await asyncio.sleep(2)
        if random.choice([True, False]):
            winner, loser, winner_name, loser_name = attacker, defender, attacker['first_name'], defender['first_name']
        else:
            winner, loser, winner_name, loser_name = defender, attacker, defender['first_name'], attacker['first_name']
        stolen_size = random.randint(1, 5)
        final_loser_size = max(0, loser['size'] - stolen_size)
        stolen_size = loser['size'] - final_loser_size
        db_query("UPDATE users SET size = size + ? WHERE user_id = ? AND chat_id = ?",
                 (stolen_size, winner['user_id'], chat_id))
        db_query("UPDATE users SET size = ? WHERE user_id = ? AND chat_id = ?",
                 (final_loser_size, loser['user_id'], chat_id))
        await callback.message.edit_text(
            f"🏆 <b>Победитель: {winner_name}!</b>\nВ случайной схватке удача была на его стороне. Он отбирает у {loser_name} целых {stolen_size} см!")
    db_query("UPDATE chats SET active_duel_json = NULL WHERE chat_id = ?", (chat_id,))


# A background task to check for finished events.
async def background_tasks():
    while True:
        await asyncio.sleep(20)
        now = datetime.now()
        all_chats = db_query("SELECT * FROM chats", fetchall=True)
        for chat in all_chats:
            chat_id = chat['chat_id']
            if chat['active_duel_json']:
                duel = json.loads(chat['active_duel_json'])
                if now > datetime.fromisoformat(duel["end_time"]):
                    try:
                        await bot.edit_message_text("Время вышло! Вызов на дуэль отменен.", chat_id, duel["message_id"])
                    except Exception:
                        pass
                    db_query("UPDATE chats SET active_duel_json = NULL WHERE chat_id = ?", (chat_id,))
            if chat['active_trial_json']:
                trial = json.loads(chat['active_trial_json'])
                if now > datetime.fromisoformat(trial["end_time"]):
                    prosecutor_id, defendant_id = trial["prosecutor_id"], trial["defendant_id"]
                    prosecutor = db_query("SELECT * FROM users WHERE user_id=? AND chat_id=?", (prosecutor_id, chat_id),
                                          fetchone=True)
                    defendant = db_query("SELECT * FROM users WHERE user_id=? AND chat_id=?", (defendant_id, chat_id),
                                         fetchone=True)
                    guilty_count, innocent_count = len(trial["votes"]["guilty"]), len(trial["votes"]["innocent"])
                    if guilty_count > innocent_count and (guilty_count + innocent_count) >= 1:
                        db_query("UPDATE users SET status='condemned', condemned_by=? WHERE user_id=? AND chat_id=?",
                                 (prosecutor_id, defendant_id, chat_id))
                        await bot.delete_message(chat_id, trial["message_id"])
                        term_kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="1 час", callback_data=f"set_term:{defendant_id}:1"),
                             InlineKeyboardButton(text="1 день", callback_data=f"set_term:{defendant_id}:24")],
                            [InlineKeyboardButton(text="3 дня", callback_data=f"set_term:{defendant_id}:72"),
                             InlineKeyboardButton(text="Неделя", callback_data=f"set_term:{defendant_id}:168")]])
                        await bot.send_message(chat_id,
                                               f"<b>ВЕРДИКТ: ВИНОВЕН!</b>\n{prosecutor['first_name']}, выбери срок наказания для {defendant['first_name']}.",
                                               reply_markup=term_kb)
                    else:
                        await bot.delete_message(chat_id, trial["message_id"])
                        await bot.send_message(chat_id,
                                               f"<b>ВЕРДИКТ: НЕВИНОВЕН!</b>\n{defendant['first_name']} оправдан. Штраф обвинителю ({prosecutor['first_name']}): -2 см.")
                        db_query("UPDATE users SET size=max(0, size - 2) WHERE user_id=? AND chat_id=?",
                                 (prosecutor_id, chat_id))
                    db_query("UPDATE chats SET active_trial_json = NULL WHERE chat_id = ?", (chat_id,))
        condemned_users = db_query("SELECT * FROM users WHERE status = 'condemned' AND punishment_end_time IS NOT NULL",
                                   fetchall=True)
        for user in condemned_users:
            if now > datetime.fromisoformat(user["punishment_end_time"]):
                db_query(
                    "UPDATE users SET status='normal', condemned_by=NULL, punishment_end_time=NULL WHERE user_id=? AND chat_id=?",
                    (user['user_id'], user['chat_id']))
                await bot.send_message(user['chat_id'],
                                       f"Время наказания для {user['first_name']} истекло. Он снова на свободе.")


async def main() -> None:
    init_db()
    asyncio.create_task(background_tasks())
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())