"""
Telegram -> Filen uploader bot (Telethon, MTProto, files up to 2 GB).

Workflow:
  /newfolder Trip 2026   -> creates the folder in Filen and makes it active
  (send or forward photos / videos / albums)  -> uploaded into the active folder
  /done                  -> shows a summary and closes the folder
"""
import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events

load_dotenv()

# ----------------------------------------------------------------- config ---
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

ALLOWED = {
    int(x) for x in os.getenv("ALLOWED_USER_IDS", "").replace(" ", "").split(",") if x
}
if not ALLOWED:
    raise SystemExit("Set ALLOWED_USER_IDS in .env (comma separated Telegram user IDs).")

RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "filen").strip().rstrip(":")
ROOT = os.getenv("STORAGE_ROOT", "Telegram").strip("/")  # top-level folder in Filen

TMP_DIR = Path(os.getenv("TMP_DIR", tempfile.gettempdir())) / "tg_filen_bot"
STATE_FILE = Path(os.getenv("STATE_FILE", "state.json"))

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
log = logging.getLogger("filen-bot")

# ---------------------------------------------------------- storage (rclone) ---
class RcloneStorage:
    """Talks to Filen (or any rclone remote) by calling the rclone binary.
    rclone handles the end-to-end encryption, chunking and retries."""

    def _remote(self, path):
        return f"{RCLONE_REMOTE}:{path.strip('/')}"

    def _run(self, *args, timeout=None):
        return subprocess.run(
            [RCLONE_BIN, *args], capture_output=True, text=True, timeout=timeout
        )

    def check(self):
        """Startup check: rclone installed and the remote reachable."""
        try:
            r = self._run("lsd", f"{RCLONE_REMOTE}:", timeout=120)
        except FileNotFoundError:
            raise RuntimeError(f"rclone not found (RCLONE_BIN={RCLONE_BIN}). Install rclone first.")
        if r.returncode != 0:
            raise RuntimeError(
                f"Cannot reach rclone remote '{RCLONE_REMOTE}': {r.stderr.strip() or r.stdout.strip()}"
            )
        log.info("rclone remote '%s' OK", RCLONE_REMOTE)

    def ensure_folder(self, path):
        r = self._run("mkdir", self._remote(path), timeout=300)
        if r.returncode != 0:
            raise RuntimeError(f"Cannot create folder {path}: {r.stderr.strip()[-300:]}")

    def folder_exists(self, path):
        return self._run("lsf", self._remote(path), timeout=300).returncode == 0

    def upload(self, folder_path, local_path, filename):
        dest = self._remote(f"{folder_path}/{filename}")
        r = self._run("copyto", local_path, dest, "--retries", "3", "--low-level-retries", "5")
        if r.returncode != 0:
            raise RuntimeError(f"Upload failed: {r.stderr.strip()[-300:]}")


store = RcloneStorage()

# ------------------------------------------------------------------ state ---
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            log.warning("Could not read state file, starting fresh")
    return {"active": {}, "stats": {}}


def save_state():
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


state = load_state()

# ---------------------------------------------------------------- helpers ---
def clean_name(name):
    name = re.sub(r'[\\/:*?"<>|\r\n\t]+', "-", name).strip(" .-")
    return name[:100]


def media_kind(msg):
    """Return 'photo', 'video' or None."""
    if msg.photo:
        return "photo"
    if msg.video or msg.video_note or msg.gif:
        return "video"
    if msg.document and msg.file and msg.file.mime_type:
        if msg.file.mime_type.startswith("image/"):
            return "photo"
        if msg.file.mime_type.startswith("video/"):
            return "video"
    return None


def make_filename(msg, kind):
    ext = msg.file.ext if msg.file and msg.file.ext else (".jpg" if kind == "photo" else ".mp4")
    stamp = msg.date.strftime("%Y%m%d_%H%M%S")
    orig = msg.file.name if msg.file and msg.file.name else None
    if orig:
        return f"{stamp}_{msg.id}_{clean_name(Path(orig).stem)[:60]}{ext}"
    return f"{stamp}_{msg.id}{ext}"


HELP = (
    "Send /newfolder <name> to create a folder in Filen, then send or forward "
    "photos, videos and albums. Everything goes into that folder.\n\n"
    "/newfolder <name> - create a folder and make it active\n"
    "/setfolder <name> - switch to an existing folder\n"
    "/folder - show the active folder\n"
    "/done - close the active folder and show a summary"
)

# ----------------------------------------------------------------- client ---
client = TelegramClient("filen_bot_session", API_ID, API_HASH)

queue: asyncio.Queue = asyncio.Queue()
pending = defaultdict(int)  # user_id -> number of queued/running batches
results = defaultdict(lambda: {"ok": 0, "fail": 0, "folders": []})


async def deny(event):
    await event.reply(f"Not authorized. Your Telegram ID is {event.sender_id}.")


def guard(handler):
    async def wrapper(event):
        if event.sender_id not in ALLOWED:
            return await deny(event)
        return await handler(event)

    return wrapper


# --------------------------------------------------------------- commands ---
@client.on(events.NewMessage(incoming=True, pattern=r"(?is)^/(start|help)(@\w+)?\s*$"))
@guard
async def cmd_help(event):
    await event.reply(HELP)


@client.on(events.NewMessage(incoming=True, pattern=r"(?is)^/newfolder(?:@\w+)?(?:\s+(.+))?$"))
@guard
async def cmd_newfolder(event):
    raw = event.pattern_match.group(1)
    name = clean_name(raw or "")
    if not name:
        return await event.reply("Usage: /newfolder <name>")
    try:
        await asyncio.to_thread(store.ensure_folder, ROOT)
        await asyncio.to_thread(store.ensure_folder, f"{ROOT}/{name}")
    except Exception as e:
        log.exception("newfolder failed")
        return await event.reply(f"Storage error: {e}")
    state["active"][str(event.sender_id)] = name
    state["stats"].setdefault(str(event.sender_id), {}).setdefault(name, {"photo": 0, "video": 0})
    save_state()
    await event.reply(f"Folder ready: {name}\nNow send your photos, videos or albums.")


@client.on(events.NewMessage(incoming=True, pattern=r"(?is)^/setfolder(?:@\w+)?(?:\s+(.+))?$"))
@guard
async def cmd_setfolder(event):
    name = clean_name(event.pattern_match.group(1) or "")
    if not name:
        return await event.reply("Usage: /setfolder <name>")
    exists = await asyncio.to_thread(store.folder_exists, f"{ROOT}/{name}")
    if not exists:
        return await event.reply(f"Folder not found: {name}\nCreate it with /newfolder {name}")
    state["active"][str(event.sender_id)] = name
    state["stats"].setdefault(str(event.sender_id), {}).setdefault(name, {"photo": 0, "video": 0})
    save_state()
    await event.reply(f"Active folder: {name}")


@client.on(events.NewMessage(incoming=True, pattern=r"(?is)^/folder(@\w+)?\s*$"))
@guard
async def cmd_folder(event):
    name = state["active"].get(str(event.sender_id))
    await event.reply(f"Active folder: {name}" if name else "No active folder. Use /newfolder <name>")


@client.on(events.NewMessage(incoming=True, pattern=r"(?is)^/done(@\w+)?\s*$"))
@guard
async def cmd_done(event):
    uid = event.sender_id
    name = state["active"].get(str(uid))
    if not name:
        return await event.reply("No active folder.")
    if pending[uid]:
        return await event.reply("Still uploading. Send /done again in a moment.")
    s = state["stats"].get(str(uid), {}).get(name, {"photo": 0, "video": 0})
    del state["active"][str(uid)]
    save_state()
    await event.reply(
        f"Closed folder: {name}\n{s['photo']} photo(s), {s['video']} video(s) uploaded.\n"
        "Create the next one with /newfolder <name>."
    )


# ------------------------------------------------------------ media intake ---
async def enqueue(event, msgs):
    uid = event.sender_id
    if uid not in ALLOWED:
        return await deny(event)
    msgs = [m for m in msgs if media_kind(m)]
    if not msgs:
        return
    name = state["active"].get(str(uid))
    if not name:
        return await event.reply("Create a folder first: /newfolder <name>")
    first = pending[uid] == 0
    pending[uid] += 1
    await queue.put((uid, event.chat_id, name, msgs))
    if first:
        await event.reply(f"Uploading to {name} ...")


@client.on(events.Album(func=lambda e: e.is_private))
async def on_album(event):
    await enqueue(event, event.messages)


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private and not e.grouped_id))
async def on_single(event):
    if media_kind(event.message):
        await enqueue(event, [event.message])


# ----------------------------------------------------------------- worker ---
async def worker():
    while True:
        uid, chat_id, folder, msgs = await queue.get()
        res = results[uid]
        if folder not in res["folders"]:
            res["folders"].append(folder)
        tmp = Path(tempfile.mkdtemp(dir=TMP_DIR))
        try:
            await asyncio.to_thread(store.ensure_folder, f"{ROOT}/{folder}")
            for m in msgs:
                kind = media_kind(m)
                name = make_filename(m, kind)
                local = tmp / name
                try:
                    await client.download_media(m, file=str(local))
                    await asyncio.to_thread(store.upload, f"{ROOT}/{folder}", str(local), name)
                    res["ok"] += 1
                    s = state["stats"].setdefault(str(uid), {}).setdefault(folder, {"photo": 0, "video": 0})
                    s[kind] += 1
                    save_state()
                except Exception:
                    log.exception("Failed on message %s", m.id)
                    res["fail"] += 1
                finally:
                    local.unlink(missing_ok=True)
        except Exception:
            log.exception("Batch failed")
            res["fail"] += len(msgs)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            pending[uid] -= 1
            if pending[uid] <= 0:
                pending[uid] = 0
                text = f"Uploaded {res['ok']} file(s) to {', '.join(res['folders'])}."
                if res["fail"]:
                    text += f"\n{res['fail']} file(s) failed, check the logs."
                results.pop(uid, None)
                try:
                    await client.send_message(chat_id, text)
                except Exception:
                    log.exception("Could not send summary")
            queue.task_done()


# ------------------------------------------------------------------- main ---
async def main():
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(store.check)
    await client.start(bot_token=BOT_TOKEN)
    asyncio.create_task(worker())
    me = await client.get_me()
    log.info("Bot @%s is running", me.username)
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
