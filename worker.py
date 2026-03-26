import os
import time
import asyncio
import uvloop
from pyrogram import Client

# Install C-based speed loop
uvloop.install()

# Get inputs from Workflow 'env'
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = int(os.environ.get("CHAT_ID"))
MSG_ID = int(os.environ.get("MESSAGE_ID"))
STATUS_ID = int(os.environ.get("STATUS_MSG_ID"))
FORMAT = os.environ.get("TARGET_FORMAT").lower().strip()

# Speed Trick: Use RAM Disk
WORK_DIR = "/dev/shm"

app = Client("fast_worker", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

class Progress:
    last_upd = 0

async def progress_func(current, total, action, msg, start_time):
    now = time.time()
    if now - Progress.last_upd > 4 or current == total:
        percent = round((current / total) * 100, 1)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        speed_mb = speed / (1024 * 1024)
        try:
            await msg.edit_text(f"🚀 **{action}**\n📊 Progress: `{percent}%` @ `{speed_mb:.1f} MB/s`")
            Progress.last_upd = now
        except: pass

async def main():
    async with app:
        status_msg = await app.get_messages(CHAT_ID, STATUS_ID)
        file_path = None
        out_file = None
        
        try:
            # 1. Download directly to RAM
            start_time = time.time()
            target_msg = await app.get_messages(CHAT_ID, MSG_ID)
            
            await status_msg.edit_text("⚡ **Step 1:** Downloading to RAM Disk...")
            file_path = await app.download_media(
                target_msg, 
                file_name=f"{WORK_DIR}/input",
                progress=progress_func, 
                progress_args=("Downloading", status_msg, start_time)
            )

            # 2. Convert using Ultrafast Presets
            await status_msg.edit_text(f"⚙️ **Step 2:** Converting to `{FORMAT.upper()}`...")
            out_file = f"{WORK_DIR}/output.{FORMAT}"
            
            if FORMAT in ['mp4', 'mkv', 'avi', 'mp3', 'wav', 'ogg', 'gif']:
                cmd = ["ffmpeg", "-y", "-i", file_path, "-preset", "ultrafast", "-threads", "0", out_file]
            elif FORMAT in ['png', 'jpg', 'webp', 'pdf']:
                cmd = ["convert", file_path, out_file]
            elif FORMAT in ['docx', 'pdf', 'txt']:
                cmd = ["libreoffice", "--headless", "--convert-to", FORMAT, file_path, "--outdir", WORK_DIR]
                out_file = f"{WORK_DIR}/input.{FORMAT}"
            else:
                await status_msg.edit_text("❌ Format not implemented in worker yet.")
                return

            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.communicate()

            # 3. Upload Result
            await status_msg.edit_text("⚡ **Step 3:** Uploading back to Telegram...")
            start_time = time.time()
            
            await app.send_document(
                CHAT_ID, 
                document=out_file, 
                reply_to_message_id=MSG_ID,
                progress=progress_func, 
                progress_args=("Uploading", status_msg, start_time)
            )
            
            await status_msg.delete()

        except Exception as e:
            await app.send_message(CHAT_ID, f"❌ **Worker Error:** `{str(e)}`", reply_to_message_id=MSG_ID)
        finally:
            # Clean RAM Disk
            for f in [file_path, out_file]:
                if f and os.path.exists(f): os.remove(f)

app.run(main())
