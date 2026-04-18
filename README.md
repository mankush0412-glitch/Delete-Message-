# Telegram Auto-Delete Bot

Group mein kisi bhi user, bot, ya channel ke messages automatically delete karne wala bot.

---

## Features

- `/setdelete @username 10s` — user ke messages X seconds baad delete karo
- `/setdelete 123456789 5m` — user ID se bhi set kar sakte ho
- `/listdeletes` — active rules dekhna
- `/removedelete @username` — specific rule hatana
- `/cleardeletes` — is group ke saare rules clear karna
- `/status` — bot ka status dekhna
- Sirf **owner** yeh commands use kar sakta hai

---

## Time Format

| Format | Matlab |
|--------|--------|
| `10s`  | 10 seconds |
| `5m`   | 5 minutes  |
| `2h`   | 2 hours    |
| `30`   | 30 seconds (plain number = seconds) |

---

## Setup

### Step 1 — Bot banao

1. Telegram pe @BotFather ko message karo
2. `/newbot` command do
3. Naam aur username set karo
4. **Bot Token** copy karo

### Step 2 — Apna User ID pata karo

1. @userinfobot ko message karo Telegram pe
2. Woh tumhara **numeric User ID** batayega
3. Yahi `OWNER_ID` mein dalna hai

### Step 3 — Render pe Deploy karo (FREE)

1. [render.com](https://render.com) pe account banao
2. "New +" → "Background Worker" select karo
3. Apna GitHub repo connect karo (yeh folder push karo)
4. **Build Command:** `pip install -r requirements.txt`
5. **Start Command:** `python bot.py`
6. Environment Variables mein daalo:
   - `BOT_TOKEN` = tumhara bot token
   - `OWNER_ID` = tumhara numeric user ID
7. "Create Background Worker" click karo

### Step 4 — UptimeRobot se 24/7 Alive rakhna

> Render free tier me workers sleep nahi karte (sirf web services sote hain),
> isliye worker type ke liye UptimeRobot zaroori nahi hai.
> Lekin agar web service use karte ho toh:

1. [uptimerobot.com](https://uptimerobot.com) pe jaao
2. "Add New Monitor" → HTTP(s) type
3. URL daalo tumhara Render URL
4. Interval: 5 minutes

---

## Bot ko Group mein Add karo

1. Apne group mein bot ka username search karo
2. Add karo
3. **Bot ko Admin banana zaroori hai** (Delete Messages permission chahiye)
4. `/setdelete @targetusername 10s` — ab rule set karo

---

## Local Test karna (Optional)

```bash
# Python 3.11+ chahiye
pip install -r requirements.txt

# .env.example copy karo
cp .env.example .env
# .env mein apna token aur owner id daalo

# Bot chalao
python bot.py
```

---

## Important Notes

- Bot ko group mein **Admin** banana padega, tabhi woh messages delete kar sakta hai
- Owner sirf woh hoga jiska OWNER_ID tumne set kiya
- Rules memory mein hain — bot restart hone pe rules reset ho jayenge
  (Persistent storage ke liye SQLite add kar sakte hain)
