import os
import time
import asyncio
import shutil
from pyrogram import Client

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = int(os.environ.get("CHAT_ID"))
MESSAGE_ID = int(os.environ.get("MESSAGE_ID"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID"))
TARGET_FORMAT = os.environ.get("TARGET_FORMAT").lower().strip()

# TRICK 3: Use Linux RAM Disk (/dev/shm) instead of SSD for 10x-50x faster Read/Write speeds
WORK_DIR = "/dev/shm"

VIDEO_EXTS = {'mp4', 'mkv', 'avi', 'mov', 'webm', 'flv', 'wmv', 'gif'}
AUDIO_EXTS = {'mp3', 'wav', 'ogg', 'm4a', 'flac', 'aac', 'opus', 'wma'}
IMAGE_EXTS = {'jpg', 'jpeg', 'png', 'webp', 'bmp', 'tiff', 'ico'}
DOC_EXTS   = {'pdf', 'docx', 'doc', 'odt', 'rtf', 'txt', 'html', 'xlsx', 'xls', 'ods', 'csv', 'pptx', 'ppt', 'odp'}
EBOOK_EXTS = {'epub', 'mobi', 'azw3', 'fb2', 'lit'}

app = Client("worker_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

class ProgressState:
    last_update_time = 0

async def progress_callback(current, total, action, msg, start_time):
    now = time.time()
    # Update every 3.5 seconds to balance responsiveness and avoid Telegram limits
    if now - ProgressState.last_update_time > 3.5 or current == total:
        percent = round((current / total) * 100, 1)
        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        speed_mb = speed / (1024 * 1024)
        
        text = (
            f"**{action}...**\n"
            f"📊 **Progress:** `{percent}%`\n"
            f"🚀 **Speed:** `{speed_mb:.2f} MB/s`\n"
            f"⏱ **ETA:** `{eta:.0f}s`"
        )
        try:
            await msg.edit_text(text)
            ProgressState.last_update_time = now
        except Exception:
            pass

async def generate_thumbnail(video_path):
    thumb_path = f"{video_path}_thumb.jpg"
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", 
        "-ss", "00:00:01", "-i", video_path, 
        "-vframes", "1", "-q:v", "2", thumb_path
    ]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.communicate()
    return thumb_path if os.path.exists(thumb_path) else None

async def main():
    async with app:
        status_msg = await app.get_messages(CHAT_ID, STATUS_MSG_ID)
        file_path = None
        out_file = None
        thumb_path = None
        
        try:
            await status_msg.edit_text("⚡️ **Fast-Track Activated:** Downloading to RAM Disk...")
            
            target_msg = await app.get_messages(CHAT_ID, MESSAGE_ID)
            
            # Use RAM Disk for blazing fast I/O
            temp_download_path = os.path.join(WORK_DIR, "original_file")
            start_time = time.time()
            
            file_path = await app.download_media(
                target_msg,
                file_name=temp_download_path,
                progress=progress_callback, 
                progress_args=("⬇️ Downloading to RAM", status_msg, start_time)
            )
            
            if not file_path:
                await status_msg.edit_text("❌ **Failed to download the file.**")
                return

            await status_msg.edit_text(f"⚙️ **Converting to `{TARGET_FORMAT.upper()}`...**\n_Utilizing all CPU cores & Ultrafast Presets._")
            
            out_file = os.path.join(WORK_DIR, f"output.{TARGET_FORMAT}")
            
            # TRICK 4: Optimized Command Flags
            if TARGET_FORMAT in VIDEO_EXTS or TARGET_FORMAT in AUDIO_EXTS:
                cmd = [
                    "ffmpeg", "-y", "-i", file_path, 
                    "-preset", "ultrafast", # Forces max encoding speed
                    "-threads", "0"         # Unlocks all CPU cores
                ]
                if TARGET_FORMAT == 'gif':
                    cmd.extend(["-vf", "fps=12,scale=480:-1:flags=fast_bilinear"]) # Fast GIF scaling
                cmd.append(out_file)

            elif TARGET_FORMAT in IMAGE_EXTS:
                cmd = ["convert", file_path, "-quality", "85", out_file]

            elif TARGET_FORMAT in DOC_EXTS:
                cmd = ["libreoffice", "--headless", "--convert-to", TARGET_FORMAT, file_path, "--outdir", WORK_DIR]
                out_file = os.path.join(WORK_DIR, f"original_file.{TARGET_FORMAT}")

            elif TARGET_FORMAT in EBOOK_EXTS:
                cmd = ["ebook-convert", file_path, out_file]
            else:
                await status_msg.edit_text(f"❌ **Unsupported format:** `{TARGET_FORMAT}`")
                return

            # Execute Conversion
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                await status_msg.edit_text(f"❌ **Conversion Failed!**\n`{stderr.decode('utf-8')[:500]}`")
                return

            if not os.path.exists(out_file) or os.path.getsize(out_file) == 0:
                await status_msg.edit_text("❌ **Output file is empty.**")
                return

            # Rename file to original name for the user
            original_name = getattr(target_msg.document or target_msg.video or target_msg.audio, 'file_name', 'Converted_File')
            if original_name:
                final_name = os.path.join(WORK_DIR, f"{os.path.splitext(original_name)[0]}.{TARGET_FORMAT}")
            else:
                final_name = os.path.join(WORK_DIR, f"Converted.{TARGET_FORMAT}")
            
            os.rename(out_file, final_name)
            out_file = final_name

            await status_msg.edit_text("⬆️ **Initializing High-Speed Upload...**")
            start_time = time.time()
            
            # Smart Upload
            if TARGET_FORMAT in VIDEO_EXTS and TARGET_FORMAT != 'gif':
                thumb_path = await generate_thumbnail(out_file)
                await app.send_video(
                    CHAT_ID, video=out_file, thumb=thumb_path,
                    caption=f"⚡️ Converted to **{TARGET_FORMAT.upper()}**",
                    reply_to_message_id=MESSAGE_ID,
                    progress=progress_callback, progress_args=("⬆️ Uploading Video", status_msg, start_time)
                )
            elif TARGET_FORMAT in AUDIO_EXTS:
                await app.send_audio(
                    CHAT_ID, audio=out_file, 
                    caption=f"⚡️ Converted to **{TARGET_FORMAT.upper()}**",
                    reply_to_message_id=MESSAGE_ID,
                    progress=progress_callback, progress_args=("⬆️ Uploading Audio", status_msg, start_time)
                )
            elif TARGET_FORMAT in IMAGE_EXTS or TARGET_FORMAT == 'gif':
                await app.send_photo(
                    CHAT_ID, photo=out_file, 
                    caption=f"⚡️ Converted to **{TARGET_FORMAT.upper()}**",
                    reply_to_message_id=MESSAGE_ID,
                    progress=progress_callback, progress_args=("⬆️ Uploading Image", status_msg, start_time)
                )
            else:
                await app.send_document(
                    CHAT_ID, document=out_file, 
                    caption=f"⚡️ Converted to **{TARGET_FORMAT.upper()}**",
                    reply_to_message_id=MESSAGE_ID,
                    progress=progress_callback, progress_args=("⬆️ Uploading Document", status_msg, start_time)
                )
            
            await status_msg.edit_text("✅ **Task Completed!**\n_Processed via High-Speed GitHub Action._")

        except Exception as e:
            await status_msg.edit_text(f"❌ **System Error:**\n`{str(e)[:800]}`")
            
        finally:
            # Cleanup RAM Disk
            for f in [file_path, out_file, thumb_path]:
                if f and os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception:
                        pass

if __name__ == "__main__":
    app.run(main())
