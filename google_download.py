#!/usr/bin/env python3
import io
import json
import os
import pickle
from pathlib import Path
from typing import Dict, Any, List, Optional
import httplib2
from googleapiclient.discovery import build
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil


from google.auth.transport.requests import AuthorizedSession


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
CREDS_FILE = "credentials.json"
TOKEN_FILE = "token.pickle"

def download_file_parallel(service, creds, file_id: str, dest_path: str, workers: int = 8, part_mb: int = 32):
    """
    Parallel range-download to maximize throughput.
    workers: number of concurrent parts
    part_mb: size of each range part in MB (32-128MB is typical)
    """
    # 1) Get file metadata (size + name)
    meta = service.files().get(
        fileId=file_id,
        fields="size,name",
        supportsAllDrives=True
    ).execute()

    total = int(meta["size"])
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

    part_size = part_mb * 1024 * 1024
    num_parts = math.ceil(total / part_size)

    sess = AuthorizedSession(creds)
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    headers_base = {"Accept-Encoding": "identity"}  # avoid gzip issues for binary

    tmp_dir = Path(dest_path).with_suffix(".parts")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    print(f"Total size: {total/1024/1024:.2f} MB | parts: {num_parts} | workers: {workers}")

    downloaded_lock = threading.Lock()
    downloaded_bytes = 0
    start_time = time.time()

    def fetch_part(i: int):
        nonlocal downloaded_bytes
        start = i * part_size
        end = min(total - 1, start + part_size - 1)

        part_path = tmp_dir / f"part_{i:05d}"
        if part_path.exists() and part_path.stat().st_size == (end - start + 1):
            return i  # already done

        headers = dict(headers_base)
        headers["Range"] = f"bytes={start}-{end}"

        r = sess.get(url, headers=headers, timeout=300)
        r.raise_for_status()

        with open(part_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                with downloaded_lock:
                    downloaded_bytes += len(chunk)

        return i

    # Progress printer
    def progress_loop():
        last = 0
        last_t = start_time
        while True:
            time.sleep(1)
            with downloaded_lock:
                cur = downloaded_bytes
            now = time.time()
            dt = max(now - last_t, 1e-6)
            speed_mb = ((cur - last) / dt) / (1024 * 1024)
            pct = (cur / total) * 100
            eta = ""
            avg_speed = (cur / max(now - start_time, 1e-6))
            if avg_speed > 0:
                eta_sec = int((total - cur) / avg_speed)
                eta = f" | ETA {eta_sec//60:02d}:{eta_sec%60:02d}"
            print(f"Progress: {pct:6.2f}% | {speed_mb:7.2f} MB/s{eta}")
            last, last_t = cur, now
            if cur >= total:
                break

    t = threading.Thread(target=progress_loop, daemon=True)
    t.start()

    # 2) Download parts
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(fetch_part, i) for i in range(num_parts)]
        for f in as_completed(futures):
            f.result()  # raise errors

    # 3) Merge
    with open(dest_path, "wb") as out:
        for i in range(num_parts):
            part_path = tmp_dir / f"part_{i:05d}"
            with open(part_path, "rb") as p:
                out.write(p.read())
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"✅ Saved to: {dest_path}")




def build_drive_service(credentials_path: str = CREDS_FILE, token_pickle: str = TOKEN_FILE):
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Missing {credentials_path} in current directory.")

    with open(credentials_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ---- Create creds ----
    if data.get("type") == "service_account":
        creds = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
    else:
        if "installed" not in data and "web" not in data:
            raise ValueError("credentials.json must be service_account or OAuth client secrets (installed/web).")

        creds = None
        if os.path.exists(token_pickle):
            with open(token_pickle, "rb") as f:
                creds = pickle.load(f)

        if not creds or not creds.valid:
            if creds and creds.expired and getattr(creds, "refresh_token", None):
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)

            with open(token_pickle, "wb") as f:
                pickle.dump(creds, f)

    # ---- Build authorized http with timeout ----
    http = httplib2.Http(timeout=300)
    authed_http = AuthorizedHttp(creds, http=http)

    # NOTE: pass ONLY http= (not credentials=)
    service = build("drive", "v3", http=authed_http, cache_discovery=False)
    return service, creds


def list_shared_drives(service, max_results: int = 100) -> List[Dict[str, Any]]:
    resp = service.drives().list(pageSize=max_results, fields="drives(id,name)").execute()
    return resp.get("drives", [])


def list_folders_in_shared_drive(service, shared_drive_id: str, name_contains: Optional[str] = None, limit: int = 200):
    q_parts = [
        "mimeType='application/vnd.google-apps.folder'",
        "trashed=false",
    ]
    if name_contains:
        safe = name_contains.replace("'", "\\'")
        q_parts.append(f"name contains '{safe}'")

    q = " and ".join(q_parts)

    folders: List[Dict[str, Any]] = []
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

        folders.extend(res.get("files", []))
        if len(folders) >= limit:
            folders = folders[:limit]
            break

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    folders.sort(key=lambda x: (x.get("name") or "").lower())
    return folders


def find_file_in_folder(service, shared_drive_id: str, folder_id: str, filename: str) -> Optional[Dict[str, Any]]:
    safe = filename.replace("'", "\\'")
    q = f"'{folder_id}' in parents and name = '{safe}' and trashed=false"

    res = service.files().list(
        corpora="drive",
        driveId=shared_drive_id,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        q=q,
        fields="files(id,name,size,modifiedTime,mimeType)",
        pageSize=10,
    ).execute()

    files = res.get("files", [])
    if not files:
        return None
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files[0]


def list_candidates(service, shared_drive_id: str, folder_id: str, text: str, limit: int = 20) -> List[Dict[str, Any]]:
    safe = text.replace("'", "\\'")
    q = f"'{folder_id}' in parents and name contains '{safe}' and trashed=false"

    res = service.files().list(
        corpora="drive",
        driveId=shared_drive_id,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        q=q,
        fields="files(id,name,size,modifiedTime)",
        pageSize=limit,
    ).execute()

    files = res.get("files", [])
    files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    return files


def download_file(service, file_id: str, dest_path: str, chunk_size_mb: int = 32):
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

    with io.FileIO(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_size_mb * 1024 * 1024)
        done = False
        last_pct = -1
        while not done:
            status, done = downloader.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if pct != last_pct:
                    print(f"Downloading... {pct}%")
                    last_pct = pct

    print(f"✅ Saved to: {dest_path}")
    
    
def list_files_in_folder(
    service,
    shared_drive_id: str,
    folder_id: str,
    extensions: Optional[tuple] = None,   # e.g. (".mp4", ".mkv")
    limit: int = 500
) -> List[Dict[str, Any]]:
    """
    Lists files inside a folder on a Shared Drive.
    - extensions: if provided, filters by filename extension (case-insensitive)
    - limit: max number of files to print/return
    """
    q = f"'{folder_id}' in parents and trashed=false"

    files: List[Dict[str, Any]] = []
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

        files.extend(batch)
        if len(files) >= limit:
            files = files[:limit]
            break

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    # Sort by name
    files.sort(key=lambda x: (x.get("name") or "").lower())
    return files



def main():
    service, creds = build_drive_service(CREDS_FILE, TOKEN_FILE)

    # Show authenticated user (OAuth only; service accounts may not have this)
    try:
        about = service.about().get(fields="user").execute()
        user = about.get("user", {})
        print(f"\nAuthenticated as: {user.get('displayName')} ({user.get('emailAddress')})")
    except Exception:
        pass

    drives = list_shared_drives(service)
    if not drives:
        print("\nNo Shared Drives visible to these credentials.")
        print("If you're using a Service Account: you must add the service account email as a member of the Shared Drive.")
        print("If you're using OAuth: you might have authorized the wrong Google account; delete token.pickle and re-auth.")
        return

    print("\nAvailable Shared Drives:")
    for i, d in enumerate(drives, 1):
        print(f"{i:>2}. {d['name']}  -->  {d['id']}")

    choice = input("\nPick Shared Drive by number or paste ID: ").strip()
    if choice.isdigit():
        idx = int(choice)
        if idx < 1 or idx > len(drives):
            raise ValueError("Invalid Shared Drive number.")
        shared_drive_id = drives[idx - 1]["id"]
    else:
        shared_drive_id = choice

    # Pick folder
    term = input("\nFolder name filter (press Enter to list first 200): ").strip() or None
    folders = list_folders_in_shared_drive(service, shared_drive_id, name_contains=term, limit=200)
    if not folders:
        raise RuntimeError("No folders found in this Shared Drive (or insufficient permissions).")

    print("\nFolders (up to 200):")
    for i, f in enumerate(folders, 1):
        print(f"{i:>3}. {f['name']}  -->  {f['id']}")

    fchoice = input("\nPick folder by number or paste folder ID: ").strip()
    if fchoice.isdigit():
        idx = int(fchoice)
        if idx < 1 or idx > len(folders):
            raise ValueError("Invalid folder number.")
        folder_id = folders[idx - 1]["id"]
    else:
        folder_id = fchoice
        
        # ---- PRINT FILES INSIDE CHOSEN FOLDER ----
    print("\nListing files in chosen folder (first 300):")
    files = list_files_in_folder(
        service,
        shared_drive_id=shared_drive_id,
        folder_id=folder_id,
        extensions=None,   # or (".mp4", ".mkv") if you want only videos
        limit=300
    )

    if not files:
        print("  (No files found in this folder.)")
    else:
        for i, f in enumerate(files, 1):
            print(f"{i:>4}. {f['name']}")
    # -----------------------------------------


    filename = input("\nEnter EXACT filename to download: ").strip()
    if not filename:
        raise ValueError("Filename cannot be empty.")

    file_meta = find_file_in_folder(service, shared_drive_id, folder_id, filename)
    if not file_meta:
        print("\n❌ Exact match not found. Partial matches:")
        cands = list_candidates(service, shared_drive_id, folder_id, filename, limit=20)
        if not cands:
            raise FileNotFoundError(f"No file found in that folder matching '{filename}'")
        for c in cands:
            print(f"- {c['name']} (id={c['id']})")
        raise FileNotFoundError("Copy the exact filename from above and try again.")

    dest = str(Path("./downloads") / file_meta["name"])
    print("\nMatched file:")
    print(f"  name: {file_meta.get('name')}")
    print(f"  id:   {file_meta.get('id')}")
    print(f"  size: {file_meta.get('size')}")
    print(f"  mod:  {file_meta.get('modifiedTime')}")
    print(f"\nDownloading to: {dest}\n")
    
    dest = str(Path("./downloads") / file_meta["name"])
    # creds are inside your service builder; easiest is to return both (service, creds)
    download_file_parallel(service, creds, file_meta["id"], dest, workers=8, part_mb=64)



if __name__ == "__main__":
    main()
