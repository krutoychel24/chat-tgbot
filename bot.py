import asyncio
import json
import logging
import os
import random
import sqlite3
import html
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, TelegramObject
from aiogram.exceptions import TelegramBadRequest

TOKEN = ""
DOCS_URL = "https://telegra.ph/WombatCombat---help-06-28"

DB_FILE = "wombat.db"
CHART_FILE = "chart.png"
BJ_RESULTS_FILE = "blackjack_results.png"
GROW_COOLDOWN_HOURS = 24
TAG_COOLDOWN_SECONDS = 10
EVENT_CHANCE = 20
SPAM_COOLDOWN_SECONDS = 2
DUEL_ACCEPT_TIMEOUT_SECONDS = 60
PRESTIGE_REQUIREMENT = 100

# --- Localization Strings ---
LANGUAGES = {
    'ru': {
        'start_new': "Привет, {first_name}! 👋\nЗдарова! Твой вомбат сразу вырос на {initial_growth} см!\n<b>Текущий размер: {initial_growth} см.</b>\nНапиши /help, чтобы увидеть список команд.",
        'start_existing': "{first_name}, ты уже в игре! 😉",
        'help_text': "Полная документация по боту доступна по ссылке ниже.\nFull documentation for the bot is available at the link below.",
        'help_button': "📜 Документация / Documentation",
        'start_first': "Сначала напиши /start",
        'grow_cooldown': "Следующий рост будет доступен через: {h} ч. {m} мин.",
        'grow_fail': "К сожалению, твой вомбат сегодня не вырос. Попробуй завтра!",
        'grow_success': "Твой вомбат вырос на +{growth} см! 📏\nТеперь его длина: {new_size} см.",
        'prestige_success': "🏅 <b>ПРЕСТИЖ!</b> 🏅\n\nПоздравляем, ты достиг {req} см и сбросил свой рост ради медали!\nТвой размер теперь: <b>{new_size} см</b>.\nВсего медалей: <b>{medals}</b>.",
        'prestige_fail': "Для сброса и получения медали нужно достичь {req} см.\nТебе не хватает еще {needed} см.",
        'top_no_players': "В этом чате еще никто не играет.",
        'top_caption': "Вот текущий рейтинг 🏆",
        'top_title': "Топ вомбатов этого чата",
        'top_xlabel': "Размер (см)",
        'nickname_too_long': "Слишком длинное имя!",
        'nickname_success': "Отлично! Теперь твоего вомбата зовут: <b>{nickname}</b>",
        'nickname_prompt': "Чтобы дать имя, напиши команду так: /nickname [новое имя]",
        'me_title': "👤 <b>Твой профиль:</b>",
        'me_name': "Имя вомбата: <b>{nickname}</b>",
        'me_medals': "Медали престижа: {medals} �",
        'me_size': "Текущий размер: {size} см",
        'me_rank': "Место в топе: {rank} из {total}",
        'me_status_condemned': "Статус: <b>ОСУЖДЕН</b> 😡",
        'lang_select': "Выберите язык:",
        'lang_selected': "Язык изменен на русский.",
        'bj_already_running': "В этом чате уже идет игра или сбор в блэкджек!",
        'bj_need_bet': "Неверный формат. Используйте: /blackjack [сумма ставки]",
        'bj_bet_positive': "Ставка должна быть больше нуля.",
        'bj_not_enough_size': "У тебя недостаточно см для ставки (нужно {bet}, у тебя {size}).",
        'bj_lobby_title': "<b>🃏 Стол для Блэкджека открыт! 🃏</b>",
        'bj_lobby_host': "<b>Организатор:</b> {host_name}",
        'bj_lobby_timer': "<b>Сбор игроков:</b> {seconds_left} сек.",
        'bj_lobby_players_title': "<b>Участники:</b>",
        'bj_lobby_player_line': "👤 {p_name} (ставка: {bet} см)",
        'bj_lobby_no_players': "...пока никого...",
        'bj_join_button': "Присоединиться",
        'bj_bet_prompt': "{name}, введите сумму вашей ставки в чат.",
        'bj_bet_accepted': "Ваша ставка в {bet} см принята! Вы в игре.",
        'bj_game_started': "🃏 <b>Сбор на Блэкджек завершен!</b> 🃏\n\nУчастники: {players}\nРаздаю карты...",
        'bj_no_players_cancel': "Никто не присоединился, игра в блэкджек отменена.",
        'bj_turn_of': "Ход игрока: <b>{name}</b>",
        'bj_hit_button': "✔️ Взять (Hit)",
        'bj_stand_button': "✋ Пас (Stand)",
        'bj_dealer_turn': "Все игроки сделали ход. Теперь ходит дилер...",
        'bj_results_title': "🏁 Итоги игры в Блэкджек 🏁",
        'bj_dealer_hand': "Рука дилера",
        'bj_player_hand': "Карты",
        'bj_player_balance': "Баланс",
        'bj_res_bust': "Перебор! (-{bet} см)",
        'bj_res_win': "Победа! (+{bet} см)",
        'bj_res_loss': "Проигрыш. (-{bet} см)",
        'bj_res_push': "Ничья.",
        'unknown_player': "Неизвестный игрок",
    },
    'en': {
        'start_new': "Hello, {first_name}! 👋\nYour wombat has grown by {initial_growth} cm right away!\n<b>Current size: {initial_growth} cm.</b>\nType /help to see the command list.",
        'start_existing': "{first_name}, you are already in the game! 😉",
        'help_text': "Full documentation for the bot is available at the link below.\nПолная документация по боту доступна по ссылке ниже.",
        'help_button': "📜 Documentation / Документация",
        'start_first': "First, type /start",
        'grow_cooldown': "Next growth will be available in: {h}h {m}m.",
        'grow_fail': "Unfortunately, your wombat didn't grow today. Try again tomorrow!",
        'grow_success': "Your wombat grew by +{growth} cm! 📏\nIts new length is: {new_size} cm.",
        'prestige_success': "🏅 <b>PRESTIGE!</b> 🏅\n\nCongratulations, you reached {req} cm and reset your growth for a medal!\nYour size is now: <b>{new_size} cm</b>.\nTotal medals: <b>{medals}</b>.",
        'prestige_fail': "To reset and get a medal, you need to reach {req} cm.\nYou need {needed} more cm.",
        'top_no_players': "No one is playing in this chat yet.",
        'top_caption': "Here is the current rating 🏆",
        'top_title': "Top Wombats of this Chat",
        'top_xlabel': "Size (cm)",
        'nickname_too_long': "Nickname is too long!",
        'nickname_success': "Great! Your wombat is now named: <b>{nickname}</b>",
        'nickname_prompt': "To set a name, use the command: /nickname [new name]",
        'me_title': "👤 <b>Your Profile:</b>",
        'me_name': "Wombat's name: <b>{nickname}</b>",
        'me_medals': "Prestige medals: {medals} 🏅",
        'me_size': "Current size: {size} cm",
        'me_rank': "Rank: {rank} of {total}",
        'me_status_condemned': "Status: <b>CONDEMNED</b> 😡",
        'lang_select': "Select a language:",
        'lang_selected': "Language changed to English.",
        'bj_already_running': "A blackjack game or lobby is already active in this chat!",
        'bj_need_bet': "Invalid format. Use: /blackjack [bet amount]",
        'bj_bet_positive': "The bet must be greater than zero.",
        'bj_not_enough_size': "You don't have enough cm to bet (need {bet}, you have {size}).",
        'bj_lobby_title': "<b>🃏 Blackjack Table is Open! 🃏</b>",
        'bj_lobby_host': "<b>Host:</b> {host_name}",
        'bj_lobby_timer': "<b>Joining time:</b> {seconds_left} sec.",
        'bj_lobby_players_title': "<b>Players:</b>",
        'bj_lobby_player_line': "👤 {p_name} (bet: {bet} cm)",
        'bj_lobby_no_players': "...no one yet...",
        'bj_join_button': "Join",
        'bj_bet_prompt': "{name}, please enter your bet amount in the chat.",
        'bj_bet_accepted': "Your bet of {bet} cm has been accepted! You are in the game.",
        'bj_game_started': "🃏 <b>Joining period is over!</b> 🃏\n\nPlayers: {players}\nDealing cards...",
        'bj_no_players_cancel': "No one joined, the blackjack game is cancelled.",
        'bj_turn_of': "It's <b>{name}</b>'s turn",
        'bj_hit_button': "✔️ Hit",
        'bj_stand_button': "✋ Stand",
        'bj_dealer_turn': "All players have made their move. Now it's the dealer's turn...",
        'bj_results_title': "🏁 Blackjack Game Results 🏁",
        'bj_dealer_hand': "Dealer's Hand",
        'bj_player_hand': "Hand",
        'bj_player_balance': "Balance",
        'bj_res_bust': "Bust! (-{bet} cm)",
        'bj_res_win': "Win! (+{bet} cm)",
        'bj_res_loss': "Loss. (-{bet} cm)",
        'bj_res_push': "Push.",
        'unknown_player': "Unknown Player",
    }
}

HUMILIATION_PHRASES = [
    "Молчать, приговоренный!", "Осужденный не имеет права голоса.", "Слово тебе не давали, червь.",
    "Твое место у параши, а не в командах.", "Петушок, не чирикай!",
    "Тише будь, а то вилкой в глаз кольну.", "Твой жалкий лепет здесь никого не интересует."
]

dp = Dispatcher()
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
user_last_message_time = {}

# --- Helper Functions ---

def get_lang(chat_id: int) -> str:
    lang_data = db_query("SELECT language FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    return lang_data['language'] if lang_data and lang_data.get('language') else 'ru'

def t(key: str, lang: str, **kwargs) -> str:
    return LANGUAGES.get(lang, LANGUAGES['ru']).get(key, key).format(**kwargs)

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
            cursor.execute("SELECT medals FROM users LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN medals INTEGER DEFAULT 0")
            logging.info("Column 'medals' added to 'users' table.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY, last_event TEXT, last_tag_time TEXT,
                active_duel_json TEXT, active_trial_json TEXT, active_blackjack_json TEXT,
                language TEXT DEFAULT 'ru'
            )
        ''')
        try:
            cursor.execute("SELECT language FROM chats LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE chats ADD COLUMN language TEXT DEFAULT 'ru'")
            logging.info("Column 'language' added to 'chats' table.")

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

async def get_player_name(user_id, chat_id):
    user_data = db_query("SELECT first_name, nickname FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    lang = get_lang(chat_id)
    name = t('unknown_player', lang)
    if user_data:
        name = user_data.get('nickname') or user_data.get('first_name')
    return html.escape(name)

# --- Blackjack Helper Functions ---
def get_blackjack_game(chat_id):
    chat_info = db_query("SELECT active_blackjack_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    if chat_info and chat_info['active_blackjack_json']:
        try:
            return json.loads(chat_info['active_blackjack_json'])
        except json.JSONDecodeError:
            return None
    return None

def save_blackjack_game(chat_id, game_data):
    if game_data:
        game_json = json.dumps(game_data)
        db_query(
            "INSERT INTO chats (chat_id, active_blackjack_json) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET active_blackjack_json = excluded.active_blackjack_json",
            (chat_id, game_json)
        )
    else:
        db_query("UPDATE chats SET active_blackjack_json = NULL WHERE chat_id = ?", (chat_id,))

def create_deck():
    suits = ['♥', '♦', '♣', '♠']
    ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    return [{'rank': rank, 'suit': suit} for suit in suits for rank in ranks]

def get_card_value(card):
    if card['rank'] in ['J', 'Q', 'K']:
        return 10
    if card['rank'] == 'A':
        return 11
    return int(card['rank'])

def get_hand_value(hand):
    value = sum(get_card_value(card) for card in hand)
    num_aces = sum(1 for card in hand if card['rank'] == 'A')
    while value > 21 and num_aces:
        value -= 10
        num_aces -= 1
    return value

def format_hand(hand):
    return " ".join([f"{card['rank']}{card['suit']}" for card in hand])

async def generate_lobby_text(game: Dict, chat_id: int) -> str:
    lang = get_lang(chat_id)
    host_name = await get_player_name(game['host_id'], chat_id)
    end_time = datetime.fromisoformat(game['end_time'])
    seconds_left = max(0, int((end_time - datetime.now()).total_seconds()))
    
    player_lines = []
    for uid, pdata in game['players'].items():
        p_name = await get_player_name(int(uid), chat_id)
        player_lines.append(t('bj_lobby_player_line', lang, p_name=p_name, bet=pdata['bet']))

    if not player_lines:
        player_lines.append(t('bj_lobby_no_players', lang))

    return (
        f"{t('bj_lobby_title', lang)}\n\n"
        f"{t('bj_lobby_host', lang, host_name=host_name)}\n"
        f"{t('bj_lobby_timer', lang, seconds_left=seconds_left)}\n\n"
        f"{t('bj_lobby_players_title', lang)}\n" +
        "\n".join(player_lines)
    )

# --- Middlewares ---
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

# --- Command Handlers ---
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
    lang = get_lang(chat_id)
    user = db_query("SELECT 1 FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user:
        initial_growth = random.randint(1, 10)
        db_query(
            "INSERT INTO users (chat_id, user_id, first_name, username, size, last_growth, status, medals) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (chat_id, user_id, first_name, username, initial_growth, datetime.now().isoformat(), 'normal', 0)
        )
        await message.answer(
            t('start_new', lang, first_name=html.escape(first_name), initial_growth=initial_growth)
        )
    else:
        db_query("UPDATE users SET first_name = ?, username = ? WHERE user_id = ? AND chat_id = ?",
                 (first_name, username, user_id, chat_id))
        await message.answer(t('start_existing', lang, first_name=html.escape(first_name)))

@dp.message(Command("help"))
async def command_help_handler(message: types.Message):
    lang = get_lang(message.chat.id)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t('help_button', lang), url=DOCS_URL)]])
    await message.answer(t('help_text', lang), reply_markup=keyboard, disable_web_page_preview=True)

async def handle_humiliation(message: types.Message, user_data: dict):
    if user_data and user_data.get("status") == "condemned":
        await message.reply(random.choice(HUMILIATION_PHRASES))
        return True
    return False

@dp.message(Command("grow"))
async def command_grow_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    lang = get_lang(chat_id)
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer(t('start_first', lang))
        return
    if await handle_humiliation(message, user_data): return
    last_growth_str = user_data.get("last_growth")
    if last_growth_str:
        last_growth_time = datetime.fromisoformat(last_growth_str)
        cooldown = timedelta(hours=GROW_COOLDOWN_HOURS)
        if datetime.now() < last_growth_time + cooldown:
            time_left = (last_growth_time + cooldown) - datetime.now()
            h, rem = divmod(int(time_left.total_seconds()), 3600)
            m, _ = divmod(rem, 60)
            await message.answer(t('grow_cooldown', lang, h=h, m=m))
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
        await message.answer(t('grow_fail', lang))
    else:
        new_size = user_data.get("size", 0) + growth
        db_query("UPDATE users SET size = ?, last_growth = ? WHERE user_id = ? AND chat_id = ?",
                 (new_size, datetime.now().isoformat(), user_id, chat_id))
        await message.answer(t('grow_success', lang, growth=growth, new_size=new_size))

@dp.message(Command("prestige"))
async def command_prestige_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    lang = get_lang(chat_id)
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer(t('start_first', lang))
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
        await message.answer(t('prestige_success', lang, req=PRESTIGE_REQUIREMENT, new_size=new_size, medals=new_medals))
    else:
        needed = PRESTIGE_REQUIREMENT - current_size
        await message.answer(t('prestige_fail', lang, req=PRESTIGE_REQUIREMENT, needed=needed))

@dp.message(Command("top"))
async def command_top_handler(message: types.Message):
    chat_id = message.chat.id
    lang = get_lang(chat_id)
    sorted_users = db_query("SELECT * FROM users WHERE chat_id = ? ORDER BY size DESC LIMIT 15", (chat_id,),
                            fetchall=True)
    if not sorted_users:
        await message.answer(t('top_no_players', lang))
        return
        
    players = [await get_player_name(user['user_id'], chat_id) for user in sorted_users]
    sizes = [user.get('size', 0) for user in sorted_users]
    
    plt.style.use('dark_background')
    fig, ax = plt.subplots()
    bars = ax.barh(players, sizes, color='#0088cc')
    ax.invert_yaxis()
    ax.set_xlabel(t('top_xlabel', lang))
    ax.set_title(t('top_title', lang))
    ax.bar_label(bars, fmt='%d см', label_type='edge', color='white', padding=5)
    fig.tight_layout()
    plt.savefig(CHART_FILE, dpi=200, bbox_inches='tight')
    plt.close(fig)
    chart = FSInputFile(CHART_FILE)
    await message.answer_photo(chart, caption=t('top_caption', lang))
    if os.path.exists(CHART_FILE): os.remove(CHART_FILE)

@dp.message(Command("nickname"))
async def command_nickname_handler(message: types.Message, command: CommandObject):
    chat_id, user_id = message.chat.id, message.from_user.id
    lang = get_lang(chat_id)
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer(t('start_first', lang))
        return
    if await handle_humiliation(message, user_data): return
    new_nickname = command.args
    if new_nickname:
        if len(new_nickname) > 20:
            await message.answer(t('nickname_too_long', lang))
            return
        db_query("UPDATE users SET nickname = ? WHERE user_id = ? AND chat_id = ?", (new_nickname, user_id, chat_id))
        await message.answer(t('nickname_success', lang, nickname=html.escape(new_nickname)))
    else:
        await message.answer(t('nickname_prompt', lang))

@dp.message(Command("me"))
async def command_me_handler(message: types.Message):
    chat_id, user_id = message.chat.id, message.from_user.id
    lang = get_lang(chat_id)
    user_data = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer(t('start_first', lang))
        return
        
    all_users = db_query("SELECT * FROM users WHERE chat_id = ? ORDER BY size DESC", (chat_id,), fetchall=True)
    rank = next((i + 1 for i, u in enumerate(all_users) if u['user_id'] == user_id), len(all_users))

    response = f"{t('me_title', lang)}\n"
    if user_data.get('nickname'):
        response += f"{t('me_name', lang, nickname=html.escape(user_data['nickname']))}\n"

    medals = user_data.get('medals', 0)
    if medals > 0:
        response += f"{t('me_medals', lang, medals=medals)}\n"

    response += f"{t('me_size', lang, size=user_data.get('size', 0))}\n"
    response += f"{t('me_rank', lang, rank=rank, total=len(all_users))}\n"
    if user_data.get('status') == 'condemned':
        response += t('me_status_condemned', lang)
    await message.answer(response)

@dp.message(Command("language"))
async def command_language_handler(message: types.Message):
    lang = get_lang(message.chat.id)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Русский 🇷🇺", callback_data="set_lang:ru")],
        [InlineKeyboardButton(text="English 🇬🇧", callback_data="set_lang:en")]
    ])
    await message.answer(t('lang_select', lang), reply_markup=keyboard)

@dp.callback_query(F.data.startswith("set_lang:"))
async def set_language_callback(callback: types.CallbackQuery):
    lang_code = callback.data.split(":")[1]
    chat_id = callback.message.chat.id
    db_query(
        "INSERT INTO chats (chat_id, language) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET language = excluded.language",
        (chat_id, lang_code)
    )
    await callback.message.edit_text(t('lang_selected', lang_code))
    await callback.answer()

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
    attacker_name = await get_player_name(attacker_id, chat_id)
    defender_name = await get_player_name(int(defender_id), chat_id)
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
    user_name = await get_player_name(user_id, chat_id)
    msg = await message.answer(f"<b>{user_name}</b> ставит {bet} см... 🎲")
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
    mentions = [f"<a href='tg://user?id={user['user_id']}'>{html.escape(user.get('first_name', 'Player'))}</a>" for user in
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
    prosecutor_name = await get_player_name(prosecutor_id, chat_id)
    defendant_name = await get_player_name(int(defendant_id), chat_id)
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
        executioner_name = await get_player_name(executioner_id, chat_id)
        target_name = await get_player_name(int(target_id), chat_id)
        db_query(
            "UPDATE users SET size_before_execution = size, size = 0, status = 'executed', executed_at = ?, condemned_by = NULL, punishment_end_time = NULL WHERE user_id = ? AND chat_id = ?",
            (datetime.now().isoformat(), target_id, chat_id))
        await message.answer(
            f"☠️ <b>ПРИГОВОР ИСПОЛНЕН!</b>\n{executioner_name} казнил {target_name}. Его вомбат обнулен.")
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
        target_name = await get_player_name(int(target_id), chat_id)
        executed_at = datetime.fromisoformat(target_data["executed_at"])
        if datetime.now() < executed_at + timedelta(minutes=30):
            db_query("UPDATE users SET size = ?, status = 'normal' WHERE user_id = ? AND chat_id = ?",
                     (target_data['size_before_execution'], target_id, chat_id))
            await message.answer(
                f"❤️ <b>МИЛОСЕРДИЕ!</b>\n{target_name} был помилован. Его вомбат восстановлен!")
        else:
            await message.answer("Слишком поздно для милосердия.")

@dp.message(Command("blackjack"))
async def command_blackjack_handler(message: types.Message, command: CommandObject):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_lang(chat_id)
    user_name = await get_player_name(user_id, chat_id)

    game = get_blackjack_game(chat_id)
    if game and game.get('state') not in [None, 'finished']:
        await message.answer(t('bj_already_running', lang))
        return

    if not command.args or not command.args.isdigit():
        await message.reply(t('bj_need_bet', lang))
        return
    
    bet = int(command.args)
    if bet <= 0:
        await message.reply(t('bj_bet_positive', lang))
        return

    user_data = db_query("SELECT size FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await message.answer(t('start_first', lang))
        return
    
    if user_data['size'] < bet:
        await message.reply(t('bj_not_enough_size', lang, bet=bet, size=user_data.get('size', 0)))
        return

    join_end_time = datetime.now() + timedelta(seconds=30)

    new_game = {
        "state": "waiting",
        "host_id": user_id,
        "players": {
            str(user_id): {"hand": [], "bet": bet, "status": "playing"}
        },
        "deck": [],
        "dealer_hand": [],
        "message_id": None,
        "current_player_index": 0,
        "end_time": join_end_time.isoformat(),
        "expecting_bet_from": None
    }
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t('bj_join_button', lang), callback_data="blackjack_join")]
    ])
    
    logging.info(f"[BJ_CREATE] Chat {chat_id}: User {user_id} ({user_name}) started a new blackjack game with bet {bet}.")
    
    lobby_text = await generate_lobby_text(new_game, chat_id)
    msg = await message.answer(lobby_text, reply_markup=keyboard)
    
    new_game['message_id'] = msg.message_id
    save_blackjack_game(chat_id, new_game)


@dp.message(F.text & ~F.text.startswith('/'))
async def handle_blackjack_bet(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    lang = get_lang(chat_id)

    game = get_blackjack_game(chat_id)
    if not game or game.get('state') != 'waiting' or game.get('expecting_bet_from') != user_id:
        return

    bet_str = message.text
    if not bet_str.isdigit():
        await message.reply(t('bj_need_bet', lang))
        return
    
    bet = int(bet_str)
    if bet <= 0:
        await message.reply(t('bj_bet_positive', lang))
        return

    user_data = db_query("SELECT size FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data or user_data['size'] < bet:
        await message.reply(t('bj_not_enough_size', lang, bet=bet, size=user_data.get('size', 0)))
        game['expecting_bet_from'] = None 
        save_blackjack_game(chat_id, game)
        return

    game['players'][str(user_id)] = {"hand": [], "bet": bet, "status": "playing"}
    game['expecting_bet_from'] = None
    save_blackjack_game(chat_id, game)
    logging.info(f"[BJ_BET] Chat {chat_id}: User {user_id} successfully placed a bet of {bet}.")

    try:
        lobby_text = await generate_lobby_text(game, chat_id)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t('bj_join_button', lang), callback_data="blackjack_join")]
        ])
        await bot.edit_message_text(
            text=lobby_text,
            chat_id=chat_id,
            message_id=game['message_id'],
            reply_markup=keyboard
        )
    except TelegramBadRequest:
        pass
    
    await message.reply(t('bj_bet_accepted', lang, bet=bet))

async def start_blackjack_game_logic(chat_id: int):
    game = get_blackjack_game(chat_id)
    lang = get_lang(chat_id)
    if not game or game.get('state') != 'waiting':
        logging.warning(f"[BJ_START_FAIL] Chat {chat_id}: Attempted to start game but state was not 'waiting'. State: {game.get('state') if game else 'None'}")
        return

    if not game['players']:
        logging.info(f"[BJ_CANCEL] Chat {chat_id}: No players joined. Cancelling game.")
        try:
            await bot.edit_message_text(
                text=t('bj_no_players_cancel', lang),
                chat_id=chat_id,
                message_id=game['message_id'],
                reply_markup=None
            )
        except TelegramBadRequest:
            pass
        save_blackjack_game(chat_id, None)
        return
    
    logging.info(f"[BJ_STARTING] Chat {chat_id}: Starting blackjack game with players: {list(game['players'].keys())}")
    game['state'] = 'in_progress'
    deck = create_deck()
    random.shuffle(deck)
    game['deck'] = deck

    for player_id_str in game['players']:
        player_data = game['players'][player_id_str]
        player_data['hand'].append(game['deck'].pop())
        player_data['hand'].append(game['deck'].pop())

    game['dealer_hand'].append(game['deck'].pop())
    game['dealer_hand'].append(game['deck'].pop())

    try:
        player_names = [await get_player_name(int(uid), chat_id) for uid in game['players']]
        await bot.edit_message_text(
            text=t('bj_game_started', lang, players=', '.join(player_names)),
            chat_id=chat_id,
            message_id=game['message_id'],
            reply_markup=None
        )
    except TelegramBadRequest:
        pass

    await asyncio.sleep(2)
    
    save_blackjack_game(chat_id, game)
    await update_blackjack_message(chat_id)

async def update_blackjack_message(chat_id: int, game_over: bool = False):
    game = get_blackjack_game(chat_id)
    if not game: return
    lang = get_lang(chat_id)

    dealer_status = ""
    dealer_hand_value = get_hand_value(game['dealer_hand'])
    if game_over or game['state'] == 'dealer_turn':
        if dealer_hand_value > 21:
            dealer_status = " (Перебор!)"
        dealer_hand_str = format_hand(game['dealer_hand'])
        dealer_value_str = f"({dealer_hand_value}){dealer_status}"
    else:
        dealer_hand_str = f"{format_hand([game['dealer_hand'][0]])} [?]"
        dealer_value_str = f"({get_card_value(game['dealer_hand'][0])})"

    text = f"🤵‍♂️ <b>Дилер:</b> {dealer_hand_str} {dealer_value_str}\n"
    text += "------------------------------------\n"
    
    player_ids = list(game['players'].keys())
    
    for i, player_id_str in enumerate(player_ids):
        player_id = int(player_id_str)
        player_data = game['players'][player_id_str]
        player_name = await get_player_name(player_id, chat_id)
        hand_str = format_hand(player_data['hand'])
        hand_value = get_hand_value(player_data['hand'])
        
        status_emoji = ""
        if player_data['status'] == 'busted' or hand_value > 21:
            status_emoji = "💥"
        elif player_data['status'] == 'stood':
            status_emoji = "✋"
        
        cursor = "▶️ " if i == game['current_player_index'] and not game_over and game['state'] == 'in_progress' else "👤 "
        text += f"{cursor}<b>{player_name}:</b> {hand_str} ({hand_value}) {status_emoji}\n"

    keyboard = None
    if not game_over and game['state'] == 'in_progress':
        text += "\n"
        if game['current_player_index'] < len(player_ids):
            current_player_id = int(player_ids[game['current_player_index']])
            current_player_name = await get_player_name(current_player_id, chat_id)
            text += t('bj_turn_of', lang, name=current_player_name)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t('bj_hit_button', lang), callback_data="blackjack_hit"),
                 InlineKeyboardButton(text=t('bj_stand_button', lang), callback_data="blackjack_stand")]
            ])

    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=game['message_id'],
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        logging.warning(f"Failed to edit blackjack message in chat {chat_id}, content might be unchanged. Error: {e}")

async def process_next_player_turn(chat_id):
    game = get_blackjack_game(chat_id)
    if not game: return

    player_ids = list(game['players'].keys())
    
    while game['current_player_index'] < len(player_ids):
        current_player_id_str = player_ids[game['current_player_index']]
        if get_hand_value(game['players'][current_player_id_str]['hand']) > 21:
            game['players'][current_player_id_str]['status'] = 'busted'
            game['current_player_index'] += 1
        else:
            break 

    if game['current_player_index'] >= len(player_ids):
        save_blackjack_game(chat_id, game)
        await dealer_turn(chat_id)
    else:
        save_blackjack_game(chat_id, game)
        await update_blackjack_message(chat_id)

async def dealer_turn(chat_id):
    game = get_blackjack_game(chat_id)
    if not game: return
    lang = get_lang(chat_id)
    
    game['state'] = 'dealer_turn'
    save_blackjack_game(chat_id, game)
    await update_blackjack_message(chat_id)
    await bot.send_message(chat_id, t('bj_dealer_turn', lang))
    await asyncio.sleep(2)

    while get_hand_value(game['dealer_hand']) < 17:
        game['dealer_hand'].append(game['deck'].pop())
        save_blackjack_game(chat_id, game)
        await update_blackjack_message(chat_id)
        await asyncio.sleep(1.5)
    
    await end_blackjack_game(chat_id)

async def end_blackjack_game(chat_id):
    game = get_blackjack_game(chat_id)
    if not game: return
    lang = get_lang(chat_id)
    
    logging.info(f"[BJ_END] Chat {chat_id}: Blackjack game ended. Calculating results.")
    
    initial_sizes = {}
    results_data = []

    dealer_value = get_hand_value(game['dealer_hand'])
    dealer_busts = dealer_value > 21

    for player_id_str, player_data in game['players'].items():
        player_id = int(player_id_str)
        user_info = db_query("SELECT size FROM users WHERE user_id=? AND chat_id=?", (player_id, chat_id), fetchone=True)
        initial_sizes[player_id_str] = user_info['size'] if user_info else 0
        
        player_name = await get_player_name(player_id, chat_id)
        player_value = get_hand_value(player_data['hand'])
        bet = player_data['bet']
        change = 0
        
        result_text = ""
        color = 'white'
        if player_value > 21:
            change = -bet
            result_text = t('bj_res_bust', lang, bet=bet)
            color = '#FF5555' # Red
        elif dealer_busts or player_value > dealer_value:
            change = bet
            result_text = t('bj_res_win', lang, bet=bet)
            color = '#55FF55' # Green
        elif player_value < dealer_value:
            change = -bet
            result_text = t('bj_res_loss', lang, bet=bet)
            color = '#FF5555' # Red
        else:
            change = 0
            result_text = t('bj_res_push', lang)
            color = 'yellow'
        
        if change != 0:
            db_query("UPDATE users SET size = size + ? WHERE user_id = ? AND chat_id = ?", (change, player_id, chat_id))
            db_query("UPDATE users SET size = 0 WHERE user_id = ? AND chat_id = ? AND size < 0", (player_id, chat_id))

        new_size = initial_sizes.get(player_id_str, 0) + change
        results_data.append({
            "name": player_name,
            "hand": format_hand(player_data['hand']),
            "value": player_value,
            "result_text": result_text,
            "color": color,
            "balance_text": f"{initial_sizes.get(player_id_str, 0)} → {new_size} см"
        })

    # --- Image Generation ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(8, 4 + len(results_data) * 1.5))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#1c1c1c')

    ax.set_title(t('bj_results_title', lang), fontsize=20, color='white', pad=20)
    ax.axis('off')

    dealer_hand_str = format_hand(game['dealer_hand'])
    dealer_status = " (Перебор!)" if dealer_busts else ""
    ax.text(0.5, 0.9, f"{t('bj_dealer_hand', lang)}: {dealer_hand_str} ({dealer_value}){dealer_status}", ha='center', va='center', fontsize=14, color='cyan')

    y_pos = 0.8
    for data in results_data:
        ax.text(0.05, y_pos, data['name'], ha='left', va='center', fontsize=14, color='white', weight='bold')
        ax.text(0.05, y_pos - 0.05, f"{t('bj_player_hand', lang)}: {data['hand']} ({data['value']})", ha='left', va='center', fontsize=12, color='lightgrey')
        ax.text(0.95, y_pos, data['result_text'], ha='right', va='center', fontsize=14, color=data['color'], weight='bold')
        ax.text(0.95, y_pos - 0.05, f"{t('bj_player_balance', lang)}: {data['balance_text']}", ha='right', va='center', fontsize=12, color='lightgrey')
        y_pos -= 0.15

    fig.tight_layout(pad=2.0)
    plt.savefig(BJ_RESULTS_FILE, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)

    photo = FSInputFile(BJ_RESULTS_FILE)
    await bot.send_photo(chat_id, photo, reply_to_message_id=game['message_id'])
    if os.path.exists(BJ_RESULTS_FILE):
        os.remove(BJ_RESULTS_FILE)
        
    save_blackjack_game(chat_id, None)


@dp.callback_query(F.data == "blackjack_join")
async def process_blackjack_join_callback(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    lang = get_lang(chat_id)
    
    game = get_blackjack_game(chat_id)
    if not game or game['state'] != 'waiting':
        await callback.answer(t('bj_already_running', lang), show_alert=True)
        return

    if str(user_id) in game['players']:
        await callback.answer("Ты уже в игре!", show_alert=True)
        return

    if game.get('expecting_bet_from') is not None:
        await callback.answer("Подождите, пока другой игрок сделает свою ставку.", show_alert=True)
        return
    
    user_data = db_query("SELECT size FROM users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id), fetchone=True)
    if not user_data:
        await callback.answer(t('start_first', lang), show_alert=True)
        return

    game['expecting_bet_from'] = user_id
    save_blackjack_game(chat_id, game)
    
    logging.info(f"[BJ_JOIN] Chat {chat_id}: User {user_id} clicked join. Prompting for bet.")
    await callback.answer()
    user_name = await get_player_name(user_id, chat_id)
    await callback.message.reply(t('bj_bet_prompt', lang, name=user_name))


@dp.callback_query(F.data.startswith("blackjack_"))
async def process_blackjack_callback(callback: types.CallbackQuery):
    if callback.data == "blackjack_join":
        return 

    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    
    game = get_blackjack_game(chat_id)
    if not game or game['state'] != 'in_progress':
        await callback.answer("Игра неактивна.", show_alert=True)
        return

    player_ids = list(game['players'].keys())
    if game['current_player_index'] >= len(player_ids):
        await callback.answer("Сейчас не ваш ход!", show_alert=True)
        return
        
    current_player_id = int(player_ids[game['current_player_index']])

    if user_id != current_player_id:
        await callback.answer("Сейчас не ваш ход!", show_alert=True)
        return

    action = callback.data.split("_")[1]
    logging.info(f"[BJ_ACTION] Chat {chat_id}: Player {user_id} chose to {action}.")

    if action == "hit":
        game['players'][str(user_id)]['hand'].append(game['deck'].pop())
        
        if get_hand_value(game['players'][str(user_id)]['hand']) >= 21:
            game['current_player_index'] += 1
            save_blackjack_game(chat_id, game)
            await update_blackjack_message(chat_id)
            await asyncio.sleep(1)
            await process_next_player_turn(chat_id)
        else:
            save_blackjack_game(chat_id, game)
            await update_blackjack_message(chat_id)

    elif action == "stand":
        game['players'][str(user_id)]['status'] = 'stood'
        game['current_player_index'] += 1
        save_blackjack_game(chat_id, game)
        await process_next_player_turn(chat_id)
    
    await callback.answer()

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
    defendant_name = await get_player_name(trial['defendant_id'], chat_id)
    prosecutor_name = await get_player_name(trial['prosecutor_id'], chat_id)
    await bot.edit_message_text(
        text=(
            f"⚖️ <b>СУД!</b> ⚖️\n{prosecutor_name} обвиняет {defendant_name}!\n"
            f"Голосование длится 5 минут.\n\n"
            f"<b>Голоса 'Виновен': {guilty_count} | 'Невиновен': {innocent_count}</b>"
        ),
        chat_id=chat_id,
        message_id=trial["message_id"],
        reply_markup=callback.message.reply_markup
    )

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
    defendant_name = await get_player_name(int(defendant_id), chat_id)
    await callback.message.edit_text(
        text=f"Приговор вынесен! {defendant_name} осужден на {days} д. {hours_rem} ч.\nОн может быть казнен в любой момент до истечения срока."
    )

@dp.callback_query(F.data.startswith("duel_"))
async def process_duel_callback(callback: types.CallbackQuery):
    chat_id, user_id = callback.message.chat.id, callback.from_user.id
    chat_info = db_query("SELECT active_duel_json FROM chats WHERE chat_id = ?", (chat_id,), fetchone=True)
    if not chat_info or not chat_info['active_duel_json']:
        await callback.message.edit_text(text="Этот вызов на дуэль уже недействителен.")
        return
    duel_data = json.loads(chat_info['active_duel_json'])
    if user_id != duel_data["defender_id"]: await callback.answer("Это не твой вызов!", show_alert=True); return
    action = callback.data.split("_")[1]
    attacker_id, defender_id = duel_data["attacker_id"], duel_data["defender_id"]
    attacker = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (attacker_id, chat_id), fetchone=True)
    defender = db_query("SELECT * FROM users WHERE user_id = ? AND chat_id = ?", (defender_id, chat_id), fetchone=True)
    
    attacker_name = await get_player_name(attacker_id, chat_id)
    defender_name = await get_player_name(defender_id, chat_id)

    if action == "decline":
        await callback.message.edit_text(
            text=f"{defender_name} трусливо отказался от дуэли с {attacker_name}.")
    elif action == "accept":
        await callback.message.edit_text(text=f"{defender_name} принимает вызов! Бой начинается...")
        await asyncio.sleep(2)
        winner_id, loser_id = random.sample([attacker_id, defender_id], 2)
        winner_name = await get_player_name(winner_id, chat_id)
        loser_name = await get_player_name(loser_id, chat_id)
        loser_size = db_query("SELECT size FROM users WHERE user_id = ? AND chat_id = ?", (loser_id, chat_id), fetchone=True)['size']
        
        stolen_size = random.randint(1, 5)
        final_loser_size = max(0, loser_size - stolen_size)
        stolen_size = loser_size - final_loser_size
        
        db_query("UPDATE users SET size = size + ? WHERE user_id = ? AND chat_id = ?",
                 (stolen_size, winner_id, chat_id))
        db_query("UPDATE users SET size = ? WHERE user_id = ? AND chat_id = ?",
                 (final_loser_size, loser_id, chat_id))
        await callback.message.edit_text(
            text=f"🏆 <b>Победитель: {winner_name}!</b>\nВ случайной схватке удача была на его стороне. Он отбирает у {loser_name} целых {stolen_size} см!")
    db_query("UPDATE chats SET active_duel_json = NULL WHERE chat_id = ?", (chat_id,))

async def background_tasks():
    while True:
        await asyncio.sleep(5)
        now = datetime.now()
        try:
            all_chats = db_query("SELECT * FROM chats", fetchall=True)
            for chat in all_chats:
                chat_id = chat['chat_id']
                try:
                    if chat['active_duel_json']:
                        duel = json.loads(chat['active_duel_json'])
                        if now > datetime.fromisoformat(duel["end_time"]):
                            await bot.edit_message_text(
                                text="Время вышло! Вызов на дуэль отменен.",
                                chat_id=chat_id,
                                message_id=duel["message_id"]
                            )
                            db_query("UPDATE chats SET active_duel_json = NULL WHERE chat_id = ?", (chat_id,))

                    if chat['active_blackjack_json']:
                        game = get_blackjack_game(chat_id)
                        if game and game.get('state') == 'waiting':
                            end_time = datetime.fromisoformat(game["end_time"])
                            logging.info(f"[BJ_TIMER] Chat {chat_id}: Checking timer. Now={now}, End={end_time}. Time left: {(end_time - now).total_seconds():.1f}s")
                            if now > end_time:
                                logging.info(f"[BJ_TIMER_EXPIRED] Chat {chat_id}: Timer expired. Forcing game start.")
                                if game.get('expecting_bet_from') is not None:
                                    logging.warning(f"[BJ_BET_TIMEOUT] Chat {chat_id}: User {game['expecting_bet_from']} did not bet in time.")
                                    game['expecting_bet_from'] = None
                                    save_blackjack_game(chat_id, game)
                                await start_blackjack_game_logic(chat_id)

                    if chat['active_trial_json']:
                        trial = json.loads(chat['active_trial_json'])
                        if now > datetime.fromisoformat(trial["end_time"]):
                            prosecutor_id, defendant_id = trial["prosecutor_id"], trial["defendant_id"]
                            prosecutor_name = await get_player_name(prosecutor_id, chat_id)
                            defendant_name = await get_player_name(defendant_id, chat_id)
                            guilty_count, innocent_count = len(trial["votes"]["guilty"]), len(trial["votes"]["innocent"])
                            if guilty_count > innocent_count and (guilty_count + innocent_count) >= 1:
                                db_query("UPDATE users SET status='condemned', condemned_by=? WHERE user_id=? AND chat_id=?", (prosecutor_id, defendant_id, chat_id))
                                await bot.delete_message(chat_id=chat_id, message_id=trial["message_id"])
                                term_kb = InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="1 час", callback_data=f"set_term:{defendant_id}:1"),
                                     InlineKeyboardButton(text="1 день", callback_data=f"set_term:{defendant_id}:24")],
                                    [InlineKeyboardButton(text="3 дня", callback_data=f"set_term:{defendant_id}:72"),
                                     InlineKeyboardButton(text="Неделя", callback_data=f"set_term:{defendant_id}:168")]])
                                await bot.send_message(chat_id, f"<b>ВЕРДИКТ: ВИНОВЕН!</b>\n{prosecutor_name}, выбери срок наказания для {defendant_name}.", reply_markup=term_kb)
                            else:
                                await bot.delete_message(chat_id=chat_id, message_id=trial["message_id"])
                                await bot.send_message(chat_id, f"<b>ВЕРДИКТ: НЕВИНОВЕН!</b>\n{defendant_name} оправдан. Штраф обвинителю ({prosecutor_name}): -2 см.")
                                db_query("UPDATE users SET size=max(0, size - 2) WHERE user_id=? AND chat_id=?", (prosecutor_id, chat_id))
                            db_query("UPDATE chats SET active_trial_json = NULL WHERE chat_id = ?", (chat_id,))
                except (json.JSONDecodeError, KeyError) as e:
                    logging.error(f"Error processing event for chat {chat_id} due to bad data: {e}. Resetting relevant state might be needed.")
                except TelegramBadRequest as e:
                    logging.error(f"Telegram API error for chat {chat_id}: {e}")
                except Exception as e:
                    logging.error(f"An unexpected error occurred while processing chat {chat_id}: {e}", exc_info=True)
            
            condemned_users = db_query("SELECT * FROM users WHERE status = 'condemned' AND punishment_end_time IS NOT NULL", fetchall=True)
            for user in condemned_users:
                if now > datetime.fromisoformat(user["punishment_end_time"]):
                    user_name = await get_player_name(user['user_id'], user['chat_id'])
                    db_query("UPDATE users SET status='normal', condemned_by=NULL, punishment_end_time=NULL WHERE user_id=? AND chat_id=?", (user['user_id'], user['chat_id']))
                    await bot.send_message(user['chat_id'], f"Время наказания для {user_name} истекло. Он снова на свободе.")
        except Exception as e:
            logging.error(f"FATAL: Unhandled exception in background_tasks main loop: {e}", exc_info=True)

async def main() -> None:
    init_db()
    asyncio.create_task(background_tasks())
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())
