import os
import time
import asyncio
import subprocess
from pyrogram import Client

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = int(os.environ.get("CHAT_ID"))
MESSAGE_ID = int(os.environ.get("MESSAGE_ID"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID"))
TARGET_FORMAT = os.environ.get("TARGET_FORMAT").lower()

app = Client("worker_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

last_update_time = 0

async def progress_callback(current, total, action, msg):
    global last_update_time
    if time.time() - last_update_time > 5:  # Update progress every 5 seconds to avoid FloodWait
        percent = round((current / total) * 100, 1)
        try:
            await msg.edit_text(f"⏳ **{action}...** {percent}%")
            last_update_time = time.time()
        except Exception:
            pass

async def main():
    async with app:
        status_msg = await app.get_messages(CHAT_ID, STATUS_MSG_ID)
        file_path = None
        out_file = None
        
        try:
            await status_msg.edit_text("⬇️ **Downloading file...** (Bypassing 20MB limit)")
            
            target_msg = await app.get_messages(CHAT_ID, MESSAGE_ID)
            file_path = await app.download_media(
                target_msg, 
                progress=progress_callback, 
                progress_args=("Downloading", status_msg)
            )
            
            if not file_path:
                await status_msg.edit_text("❌ **Failed to download the file.**")
                return

            await status_msg.edit_text(f"⚙️ **Converting to `{TARGET_FORMAT.upper()}`...**\nThis might take a while for large files.")
            
            out_file = f"converted_output.{TARGET_FORMAT}"
            
            # Select tool based on format
            if TARGET_FORMAT in ['mp4', 'mkv', 'avi', 'mov', 'mp3', 'wav', 'ogg', 'gif', 'webm', 'flac']:
                cmd = ["ffmpeg", "-y", "-i", file_path, out_file]
            elif TARGET_FORMAT in ['png', 'jpg', 'jpeg', 'webp', 'ico', 'bmp', 'tiff']:
                cmd = ["convert", file_path, out_file]
            elif TARGET_FORMAT in ['pdf', 'docx', 'odt', 'txt']:
                cmd = ["libreoffice", "--headless", "--convert-to", TARGET_FORMAT, file_path]
                base_name = os.path.basename(file_path).rsplit('.', 1)[0]
                out_file = f"{base_name}.{TARGET_FORMAT}"
            else:
                await status_msg.edit_text(f"❌ Unsupported format: `{TARGET_FORMAT}`")
                return

            # Execute the conversion
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                await status_msg.edit_text(f"❌ **Conversion Failed!**\n`{stderr.decode('utf-8')[:500]}`")
                return

            if not os.path.exists(out_file) or os.path.getsize(out_file) == 0:
                await status_msg.edit_text("❌ **Conversion Failed!** Output file is empty.")
                return

            await status_msg.edit_text("⬆️ **Uploading converted file...**")
            
            await app.send_document(
                CHAT_ID, 
                out_file, 
                reply_to_message_id=MESSAGE_ID,
                progress=progress_callback,
                progress_args=("Uploading", status_msg)
            )
            
            await status_msg.edit_text("✅ **Conversion Complete!**")

        except Exception as e:
            await status_msg.edit_text(f"❌ **Critical Error:** `{str(e)[:500]}`")
        finally:
            # Prevent server storage leaks
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            if out_file and os.path.exists(out_file):
                os.remove(out_file)

app.run(main())
