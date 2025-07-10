import json
import sqlite3

DB_FILE = "wombat.db"
JSON_FILE = "data.json"


def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER, chat_id INTEGER, first_name TEXT, username TEXT,
                size INTEGER DEFAULT 0, nickname TEXT, last_growth TEXT, status TEXT DEFAULT 'normal',
                condemned_by INTEGER, punishment_end_time TEXT, executed_at TEXT,
                size_before_execution INTEGER DEFAULT 0, PRIMARY KEY (user_id, chat_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY, last_event TEXT, last_tag_time TEXT,
                active_duel_json TEXT, active_trial_json TEXT
            )
        ''')
        conn.commit()


def migrate_data():
    init_db()

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Файл {JSON_FILE} не найден или пуст. Миграция не требуется.")
        return

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        user_count = 0
        for chat_id, chat_data in data.items():
            for user_id, user_info in chat_data.items():
                if isinstance(user_info, dict):
                    cursor.execute('''
                        INSERT OR REPLACE INTO users (chat_id, user_id, first_name, username, size, nickname, last_growth, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        int(chat_id), int(user_id), user_info.get('first_name'), user_info.get('username'),
                        user_info.get('size', 0), user_info.get('nickname'), user_info.get('last_growth'),
                        user_info.get('status', 'normal')
                    ))
                    user_count += 1
        conn.commit()

    print(f"Миграция завершена. Перенесено {user_count} пользователей.")


if __name__ == "__main__":
    migrate_data()