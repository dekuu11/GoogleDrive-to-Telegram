import os, time, asyncio
from telethon import TelegramClient
from telethon.network.connection import ConnectionTcpFull
from dotenv import load_dotenv
load_dotenv()


API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION = os.getenv("TG_SESSION", "telethon.session")
TARGET = int(os.getenv("TG_TARGET", "-1003558732793"))
FILE_PATH = "/Users/lmt/Downloads/Zootopia.2.2025.DCPRip.720p HMTV.mp4"

last_t = 0
start = time.time()

def progress(sent, total):
    global last_t
    now = time.time()
    if now - last_t < 1 and sent != total:
        return
    last_t = now
    elapsed = max(now - start, 1e-6)
    avg = (sent / elapsed) / (1024*1024)
    pct = sent / total * 100 if total else 0
    print(f"\r{pct:6.2f}% | avg {avg:6.2f} MB/s", end="")

async def main():
    client = TelegramClient(
        SESSION, API_ID, API_HASH,
        connection=ConnectionTcpFull,
        connection_retries=10,
        timeout=60,
    )
    async with client:
        await client.send_file(
            TARGET,
            FILE_PATH,
            part_size_kb=512,
            progress_callback=progress,
        )
    print("\n✅ done")

asyncio.run(main())
