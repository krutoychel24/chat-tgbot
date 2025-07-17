🦡 **Wombat Combat Bot**
Welcome to Wombat Combat!
This is a fun, competitive Telegram bot where you grow your own virtual wombat, compete with friends, and engage in various activities like duels, casino games, and even a player-run judicial system.

---

🚀 **Installation**
To run this bot yourself, follow these steps:

---

1️⃣ **Clone the repository**

```bash
git clone <repository_url>
cd <repository_directory>
```

---

2️⃣ **Install dependencies**
The bot uses `aiogram` for interacting with the Telegram API and `matplotlib` for generating charts.

```bash
pip install aiogram matplotlib
```

---

3️⃣ **Get a Telegram Bot Token**
Talk to [@BotFather](https://t.me/BotFather) on Telegram.

Use the `/newbot` command and follow the instructions.
BotFather will give you a unique **TOKEN**.

---

4️⃣ **Configure the bot**
It is highly recommended to use environment variables for your token.

✅ *Using Environment Variables (Recommended):*
Set an environment variable named `BOT_TOKEN` with your token value.

In the script, use the following code to retrieve it:

```python
import os
TOKEN = os.getenv("BOT_TOKEN")
```

⚠️ *Directly in the code (for testing only):*
Open the Python script (`.py` file).
Find the line:

```python
TOKEN = "YOUR_TOKEN_HERE"
```

Paste your bot token between the quotes.

---

5️⃣ **Run the bot**

```bash
python bot.py
```

Now your bot should be running and ready to be added to a group chat!

---

🎮 **How to Play**

**Start Your Journey:**
Type `/start` in your group chat to get your first wombat. It will start with a random size.

**Grow Your Wombat:**
Use `/grow` once every 24 hours to increase your wombat’s size.

**Climb the Ranks:**
Use `/top` to see a chart of the biggest wombats.
Check your personal progress with `/me`.

**Get Stronger:**
Once your wombat reaches **100 cm**, use `/prestige` to reset its size back to 5 cm in exchange for a prestige medal 🏅.
Medals grant you a permanent bonus to your daily growth!

---

📖 **Command Reference**

### 📝 Basic Commands

| Command            | Description                                              |
| ------------------ | -------------------------------------------------------- |
| `/start`           | Register in the game or get a welcome message.           |
| `/help`            | Shows a link to the bot’s full documentation.            |
| `/grow`            | Increase your wombat’s size. Cooldown: 24 hours.         |
| `/me`              | Shows your wombat’s name, size, rank, and medals.        |
| `/nickname [name]` | Set a custom name for your wombat.                       |
| `/top`             | Displays a bar chart of the top 15 players.              |
| `/language`        | Change the bot's language in the chat (Русский/English). |

---

### 🎮 Game & Social Commands

| Command            | Description                                                   |
| ------------------ | ------------------------------------------------------------- |
| `/duel`            | Challenge another player to a duel by replying or mentioning. |
| `/casino [bet]`    | Gamble a portion of your wombat’s size (50/50 chance).        |
| `/blackjack [bet]` | Start a new Blackjack game with a specified bet.              |
| `/tag`             | Mention all registered players in the chat.                   |

---

🃏 **Blackjack**

**Start a Game:**
Anyone can start a game using:

```text
/blackjack [bet]
```

Example: `/blackjack 10`
The person who starts is automatically added to the game.

**Join:**
A message will appear with a "Join" button. Other players have 30 seconds to join.

**Place Your Bet:**
After joining, the bot will ask you to enter your bet amount in the chat.

**Auto-Start:**
The game begins automatically after 30 seconds with all players who have successfully placed their bets.

**Results:**
At the end of the game, a custom image with the results for all participants is sent to the chat.

---

⚖️ **The Judicial System**

| Command    | Description                                               |
| ---------- | --------------------------------------------------------- |
| `/trial`   | Accuse another player of a crime. Starts a 5-minute vote. |
| `/execute` | Reset a player’s wombat size to 0 after a guilty verdict. |
| `/pardon`  | Anyone can pardon an executed player within 30 minutes.   |


