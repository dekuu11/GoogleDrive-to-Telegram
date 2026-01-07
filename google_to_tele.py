#!/usr/bin/env python3
import asyncio
import io
import json
import os
import pickle
import time
import random
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from telethon.tl.types import DocumentAttributeVideo
from dotenv import load_dotenv


import requests
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

from googleapiclient.discovery import build
from google.auth.transport.requests import AuthorizedSession, Request as GoogleRequest
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()  # loads .env into os.environ


# ---------------- CONFIG ----------------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
GOOGLE_CREDS_FILE = "credentials.json"     # OAuth desktop JSON OR service_account JSON
GOOGLE_OAUTH_TOKEN = "token.json"          # created automatically for OAuth

# SAFETY: only allow sending to these targets (VERY IMPORTANT)
# Put your channel username(s) and/or numeric IDs here.
ALLOWED_TARGETS = {
    -1003558732793,
}

# SAFETY pacing (tune)
MIN_SECONDS_BETWEEN_SENDS = 25
JITTER_SECONDS = (2, 7)
MAX_SEND_RETRIES = 8

# Optional: daily cap (0 disables)
DAILY_SEND_LIMIT = 0
DAILY_STATE_FILE = "daily_send_state.json"
# ----------------------------------------


# ---------- Simple daily cap ----------
def _load_daily_state() -> Dict[str, Any]:
    if not os.path.exists(DAILY_STATE_FILE):
        return {}
    try:
        with open(DAILY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_daily_state(state: Dict[str, Any]) -> None:
    with open(DAILY_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

def _today_key() -> str:
    return time.strftime("%Y-%m-%d")

def check_daily_limit_or_raise():
    if DAILY_SEND_LIMIT <= 0:
        return
    state = _load_daily_state()
    key = _today_key()
    sent = int(state.get(key, 0))
    if sent >= DAILY_SEND_LIMIT:
        raise RuntimeError(f"Daily send limit reached ({sent}/{DAILY_SEND_LIMIT}).")
    state[key] = sent + 1
    _save_daily_state(state)


# ---------- Telegram safety helpers ----------
_last_send_ts = 0.0

def assert_allowed_target(target_chat):
    if not ALLOWED_TARGETS:
        raise ValueError(
            "ALLOWED_TARGETS is empty. For safety, fill it with your channel username or ID."
        )
    if target_chat not in ALLOWED_TARGETS:
        raise ValueError(
            f"Target {target_chat!r} is not in ALLOWED_TARGETS. Refusing to send."
        )
        
        
async def resolve_target_entity(client, target_chat):
    try:
        return await client.get_input_entity(target_chat)
    except Exception:
        await client.get_dialogs(limit=200)
        return await client.get_input_entity(target_chat)

async def safe_sleep_between_sends():
    global _last_send_ts
    now = asyncio.get_event_loop().time()
    elapsed = now - _last_send_ts
    wait = max(0.0, MIN_SECONDS_BETWEEN_SENDS - elapsed)
    wait += random.uniform(*JITTER_SECONDS)
    if wait > 0:
        print(f"[Safety] Waiting {wait:.1f}s before sending...")
        await asyncio.sleep(wait)
    _last_send_ts = asyncio.get_event_loop().time()

async def send_file_safe(client, target_chat, uploaded_file, caption: str = ""):
    check_daily_limit_or_raise()
    await safe_sleep_between_sends()

    # Resolve entity once (avoids PeerChannel errors)
    entity = await resolve_target_entity(client, target_chat)

    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            return await client.send_file(
                entity,
                uploaded_file,
                caption=caption,
                supports_streaming=True,
                force_document=False,  # try to make it a "video" message
            )

        except FloodWaitError as e:
            wait_s = int(getattr(e, "seconds", 60))
            extra = random.uniform(1, 5)
            print(f"[FloodWait] Sleeping {wait_s + extra:.1f}s...")
            await asyncio.sleep(wait_s + extra)

        except RPCError as e:
            backoff = min(60, 2 ** attempt)
            extra = random.uniform(1, 5)
            print(f"[RPCError] {e.__class__.__name__}: {e}. Backing off {backoff + extra:.1f}s...")
            await asyncio.sleep(backoff + extra)

    raise RuntimeError("Failed to send after retries.")

# ---------- Google Drive Auth ----------
def build_drive_service_and_creds(creds_path: str = GOOGLE_CREDS_FILE):
    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"Missing {creds_path} in current directory.")

    with open(creds_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Service Account
    if data.get("type") == "service_account":
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return service, creds

    # OAuth client secrets
    if "installed" not in data and "web" not in data:
        raise ValueError(
            "credentials.json must be either:\n"
            "- OAuth client secrets (Desktop app) JSON with 'installed' or 'web', OR\n"
            "- service account JSON with 'type=service_account'."
        )

    creds = None
    if os.path.exists(GOOGLE_OAUTH_TOKEN):
        creds = Credentials.from_authorized_user_file(GOOGLE_OAUTH_TOKEN, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_OAUTH_TOKEN, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service, creds


# ---------- Drive listing helpers (Shared Drive) ----------
def list_shared_drives(service, max_results: int = 100) -> List[Dict[str, Any]]:
    resp = service.drives().list(pageSize=max_results, fields="drives(id,name)").execute()
    return resp.get("drives", [])

def list_folders_in_shared_drive(service, shared_drive_id: str, name_contains: Optional[str] = None, limit: int = 200):
    q_parts = ["mimeType='application/vnd.google-apps.folder'", "trashed=false"]
    if name_contains:
        safe = name_contains.replace("'", "\\'")
        q_parts.append(f"name contains '{safe}'")
    q = " and ".join(q_parts)

    out: List[Dict[str, Any]] = []
    page_token = None
    while True:
        res = service.files().list(
            corpora="drive",
            driveId=shared_drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            q=q,
            fields="nextPageToken, files(id,name)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        out.extend(res.get("files", []))
        if len(out) >= limit:
            out = out[:limit]
            break
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    out.sort(key=lambda x: (x.get("name") or "").lower())
    return out

def list_files_in_folder(service, shared_drive_id: str, folder_id: str,
                         name_contains: Optional[str] = None,
                         extensions: Optional[tuple] = None,
                         limit: int = 200):
    q_parts = [f"'{folder_id}' in parents", "trashed=false", "mimeType!='application/vnd.google-apps.folder'"]
    if name_contains:
        safe = name_contains.replace("'", "\\'")
        q_parts.append(f"name contains '{safe}'")
    q = " and ".join(q_parts)

    out: List[Dict[str, Any]] = []
    page_token = None
    while True:
        res = service.files().list(
            corpora="drive",
            driveId=shared_drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            q=q,
            fields="nextPageToken, files(id,name,size,modifiedTime,mimeType)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()

        batch = res.get("files", [])
        if extensions:
            exts = tuple(e.lower() for e in extensions)
            batch = [f for f in batch if (f.get("name") or "").lower().endswith(exts)]

        out.extend(batch)
        if len(out) >= limit:
            out = out[:limit]
            break

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    out.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return out

def pick_by_number_or_id(prompt: str, items: List[Dict[str, Any]]) -> str:
    choice = input(prompt).strip()
    if not choice:
        raise ValueError("Selection cannot be empty.")
    if choice.isdigit():
        idx = int(choice)
        if idx < 1 or idx > len(items):
            raise ValueError("Invalid number.")
        return items[idx - 1]["id"]
    return choice


# ---------- Streaming reader ----------
class DriveStreamingReader(io.RawIOBase):
    def __init__(self, response: requests.Response, name: str, drive_chunk_mb: int = 8):
        self._resp = response
        self._iter = response.iter_content(chunk_size=drive_chunk_mb * 1024 * 1024)
        self._buf = bytearray()
        self.name = name

    def readable(self):
        return True

    def read(self, n=-1):
        if n == 0:
            return b""
        while n < 0 or len(self._buf) < n:
            try:
                chunk = next(self._iter)
            except StopIteration:
                break
            if chunk:
                self._buf.extend(chunk)

        if n < 0:
            data = bytes(self._buf)
            self._buf.clear()
            return data

        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def close(self):
        try:
            self._resp.close()
        finally:
            super().close()


@dataclass
class Progress:
    start_time: float
    last_time: float
    last_bytes: int

def make_progress_printer(label: str):
    st = time.time()
    prog = Progress(start_time=st, last_time=st, last_bytes=0)

    def cb(sent_bytes: int, total_bytes: int):
        now = time.time()
        if now - prog.last_time < 1.0 and sent_bytes != total_bytes:
            return

        dt = max(now - prog.last_time, 1e-6)
        delta = sent_bytes - prog.last_bytes
        speed_mb_s = (delta / dt) / (1024 * 1024)

        pct = (sent_bytes / total_bytes) * 100 if total_bytes else 0.0

        elapsed = max(now - prog.start_time, 1e-6)
        avg_bps = sent_bytes / elapsed
        eta = ""
        if total_bytes and avg_bps > 0:
            remaining = total_bytes - sent_bytes
            eta_sec = int(remaining / avg_bps)
            eta = f" | ETA {eta_sec//60:02d}:{eta_sec%60:02d}"

        print(f"{label}: {pct:6.2f}% | {speed_mb_s:7.2f} MB/s{eta}")
        prog.last_time = now
        prog.last_bytes = sent_bytes

    return cb


# ---------- Main ----------
async def main():
    api_id_str = (os.getenv("TG_API_ID") or "").strip()
    api_hash = (os.getenv("TG_API_HASH") or "").strip()

    if not api_id_str:
        api_id_str = input("Telegram api_id: ").strip()
    if not api_hash:
        api_hash = input("Telegram api_hash: ").strip()

    api_id = int(api_id_str)

    session_name = (os.getenv("TG_SESSION") or "telethon.session").strip()

    target = (os.getenv("TG_TARGET") or "").strip() or input("Send to (must be allowlisted): ").strip()
    try:
        target_chat = int(target)
    except ValueError:
        target_chat = target


    # SAFETY CHECK: only allow sending to approved targets
    assert_allowed_target(target_chat)

    drive_service, drive_creds = build_drive_service_and_creds(GOOGLE_CREDS_FILE)

    drives = list_shared_drives(drive_service)
    if not drives:
        raise RuntimeError("No Shared Drives visible to these credentials.")

    print("\nShared Drives:")
    for i, d in enumerate(drives, 1):
        print(f"{i:>2}. {d['name']}  -->  {d['id']}")

    shared_drive_id = pick_by_number_or_id("\nPick Shared Drive by number or paste ID: ", drives)

    folder_filter = input("\nFolder name filter (Enter to list first 200): ").strip() or None
    folders = list_folders_in_shared_drive(drive_service, shared_drive_id, name_contains=folder_filter, limit=200)
    if not folders:
        raise RuntimeError("No folders found (or insufficient permission).")

    print("\nFolders (up to 200):")
    for i, f in enumerate(folders, 1):
        print(f"{i:>3}. {f['name']}  -->  {f['id']}")

    folder_id = pick_by_number_or_id("\nPick folder by number or paste folder ID: ", folders)

    only_video = input("\nList only video extensions? (y/N): ").strip().lower() == "y"
    exts = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv") if only_video else None

    file_filter = input("File name contains filter (Enter for none): ").strip() or None
    files = list_files_in_folder(
        drive_service,
        shared_drive_id=shared_drive_id,
        folder_id=folder_id,
        name_contains=file_filter,
        extensions=exts,
        limit=200,
    )
    if not files:
        raise RuntimeError("No files found in that folder (with your filters).")

    print("\nFiles (newest first, up to 200):")
    for i, f in enumerate(files, 1):
        size_mb = (int(f.get("size", 0)) / (1024 * 1024)) if f.get("size") else 0
        print(f"{i:>3}. {f['name']}  |  {size_mb:.2f} MB  |  id={f['id']}")

    file_id = pick_by_number_or_id("\nPick file by number or paste file ID: ", files)

    meta = drive_service.files().get(fileId=file_id, fields="name,size", supportsAllDrives=True).execute()
    filename = meta["name"]
    file_size = int(meta["size"])
    print(f"\nSelected: {filename} ({file_size/1024/1024:.2f} MB)")

    sess = AuthorizedSession(drive_creds)
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    resp = sess.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    stream = DriveStreamingReader(resp, name=filename, drive_chunk_mb=32)

    async with TelegramClient(session_name, api_id, api_hash) as client:
        progress_cb = make_progress_printer("Uploading")
        entity = await resolve_target_entity(client, target_chat)

        # (Optional safety pacing before the send)
        # await safe_sleep_between_sends()

        await client.send_file(
            entity,
            stream,
            file_size=file_size,
            caption=filename,
            supports_streaming=True,
            force_document=False,
            part_size_kb=512,
            progress_callback=progress_cb,
        )


        # SAFER SEND (pacing + floodwait)
        await send_file_safe(client, target_chat, uploaded, caption=filename)

    print("✅ Done.")


if __name__ == "__main__":
    asyncio.run(main())
