---

# 🦡 Wombat Combat Bot

Welcome to **Wombat Combat**!
This is a fun, competitive Telegram bot where you grow your own virtual wombat, compete with friends, and engage in various activities like duels, casino games, and even a player-run judicial system.

---

## 🚀 Installation

To run this bot yourself, follow these steps:

### 1️⃣ Clone the repository

```bash
git clone <repository_url>
cd <repository_directory>
```

### 2️⃣ Install dependencies

The bot uses `aiogram` for interacting with the Telegram API and `matplotlib` for generating charts.

```bash
pip install aiogram matplotlib
```

### 3️⃣ Get a Telegram Bot Token

* Talk to [@BotFather](https://t.me/BotFather) on Telegram.
* Use the `/newbot` command and follow the instructions.
* BotFather will give you a unique `TOKEN`.

### 4️⃣ Configure the bot

Open the Python script (`.py` file).
Find the line:

```python
TOKEN = ""
```

Paste your bot token between the quotes.

### 5️⃣ Run the bot

```bash
python your_bot_script_name.py
```

Now your bot should be running and ready to be added to a group chat!

---

## 🎮 How to Play

* **Start Your Journey:**
  Type `/start` in your group chat to get your first wombat. It will start with a random size.

* **Grow Your Wombat:**
  Use `/grow` once every 24 hours to increase your wombat’s size.

* **Climb the Ranks:**
  Use `/top` to see a chart of the biggest wombats. Check your personal progress with `/me`.

* **Get Stronger:**
  Once your wombat reaches 100 cm, use `/prestige` to reset its size back to 5 cm in exchange for a prestige medal 🏅.
  Medals grant you a permanent bonus to your daily growth!

---

## 📖 Command Reference

### Basic Commands

| Command            | Description                                       |
| ------------------ | ------------------------------------------------- |
| `/start`           | Register in the game or get a welcome message.    |
| `/help`            | Shows a link to the bot’s full documentation.     |
| `/grow`            | Increase your wombat’s size. Cooldown: 24 hours.  |
| `/me`              | Shows your wombat’s name, size, rank, and medals. |
| `/nickname [name]` | Set a custom name for your wombat.                |
| `/top`             | Displays a bar chart of the top 15 players.       |

---

### Game & Social Commands

| Command         | Description                                                |
| --------------- | ---------------------------------------------------------- |
| `/duel`         | Challenge another player to a duel by replying or mention. |
| `/casino [bet]` | Gamble a portion of your wombat’s size (50/50 chance).     |
| `/tag`          | Mention all registered players in the chat.                |

---

### ⚖️ The Judicial System

| Command    | Description                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------- |
| `/trial`   | Accuse another player of a crime. Starts a 5-minute vote.                                   |
| `/execute` | Reset a player’s wombat size to 0 after a guilty verdict and punishment term.               |
| `/pardon`  | Anyone can pardon an executed player within 30 minutes, restoring their wombat to its size. |

---

