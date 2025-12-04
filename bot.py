import os
import asyncio
import re
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from motor.motor_asyncio import AsyncIOMotorClient
from rapidfuzz import fuzz, process
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
API_ID = int(os.environ.get("API_ID", "YOUR_API_ID"))
API_HASH = os.environ.get("API_HASH", "YOUR_API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x]
GROUP_ID = int(os.environ.get("GROUP_ID", "-1001234567890"))
PORT = int(os.environ.get("PORT", 8080))

# ==================== DATABASE ====================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client.file_bot
files_collection = db.files
users_collection = db.users
channels_collection = db.channels
delete_queue = db.delete_queue

# ==================== PYROGRAM CLIENT ====================
app = Client(
    "file_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    sleep_threshold=30
)

scheduler = AsyncIOScheduler()

# ==================== HELPER FUNCTIONS ====================
async def is_banned(user_id):
    user = await users_collection.find_one({"user_id": user_id})
    return user and user.get("banned", False)

async def ban_user(user_id):
    await users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"banned": True}},
        upsert=True
    )

async def unban_user(user_id):
    await users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"banned": False}},
        upsert=True
    )

def parse_file_info(filename):
    """Extract quality, year, language, season from filename"""
    info = {
        "quality": None,
        "year": None,
        "language": [],
        "season": None
    }
    
    # Quality
    quality_match = re.search(r'(480p|720p|1080p|2160p|4k)', filename, re.IGNORECASE)
    if quality_match:
        info["quality"] = quality_match.group(1).upper()
    
    # Year
    year_match = re.search(r'(19\d{2}|20\d{2})', filename)
    if year_match:
        info["year"] = int(year_match.group(1))
    
    # Language
    languages = ["Hindi", "English", "Tamil", "Telugu", "Kannada", "Malayalam"]
    for lang in languages:
        if lang.lower() in filename.lower():
            info["language"].append(lang)
    
    # Season
    season_match = re.search(r'S(\d{1,2})', filename, re.IGNORECASE)
    if season_match:
        info["season"] = int(season_match.group(1))
    
    return info

def format_size(size):
    """Convert bytes to readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"

async def fuzzy_search(query, limit=5):
    """Find similar file names if exact match not found"""
    all_files = await files_collection.find().to_list(length=1000)
    file_names = [f["file_name"] for f in all_files]
    
    matches = process.extract(query, file_names, scorer=fuzz.token_sort_ratio, limit=limit)
    suggestions = [match[0] for match in matches if match[1] > 60]
    
    return suggestions

async def search_files(query, filters=None):
    """Search files with optional filters"""
    search_query = {"file_name": {"$regex": query, "$options": "i"}}
    
    if filters:
        if filters.get("quality"):
            search_query["quality"] = filters["quality"]
        if filters.get("year"):
            search_query["year"] = filters["year"]
        if filters.get("language"):
            search_query["language"] = {"$in": [filters["language"]]}
        if filters.get("season"):
            search_query["season"] = filters["season"]
    
    files = await files_collection.find(search_query).to_list(length=None)
    return files

def create_result_keyboard(page, total_pages, current_filters=None, query=""):
    """Create pagination and filter keyboard"""
    keyboard = []
    
    # Filter buttons
    filter_row1 = [
        InlineKeyboardButton("🎨 Quality 🍿", callback_data=f"filter_quality_{query}_{page}"),
        InlineKeyboardButton("📅 Year 🎬", callback_data=f"filter_year_{query}_{page}")
    ]
    filter_row2 = [
        InlineKeyboardButton("🔤 Language", callback_data=f"filter_language_{query}_{page}"),
        InlineKeyboardButton("📺 Season", callback_data=f"filter_season_{query}_{page}")
    ]
    
    keyboard.append(filter_row1)
    keyboard.append(filter_row2)
    
    # Pagination buttons
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"page_{query}_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="current_page"))
    
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{query}_{page+1}"))
    
    keyboard.append(nav_buttons)
    
    return InlineKeyboardMarkup(keyboard)

def create_filter_keyboard(filter_type, query, page):
    """Create filter selection keyboard"""
    keyboard = []
    
    if filter_type == "quality":
        options = [
            ("480p", "480P"),
            ("720p", "720P"),
            ("1080p", "1080P"),
            ("4K", "2160P")
        ]
        for i in range(0, len(options), 2):
            row = []
            for j in range(2):
                if i + j < len(options):
                    label, value = options[i + j]
                    row.append(InlineKeyboardButton(label, callback_data=f"setfilter_quality_{value}_{query}_{page}"))
            keyboard.append(row)
    
    elif filter_type == "language":
        options = ["English", "Hindi", "Tamil", "Telugu", "Kannada", "Malayalam"]
        for i in range(0, len(options), 2):
            row = []
            for j in range(2):
                if i + j < len(options):
                    lang = options[i + j]
                    row.append(InlineKeyboardButton(lang, callback_data=f"setfilter_language_{lang}_{query}_{page}"))
            keyboard.append(row)
    
    elif filter_type == "year":
        current_year = 2025
        years = list(range(current_year, 1899, -1))
        # Show 8 years at a time
        for i in range(0, 8, 4):
            row = []
            for j in range(4):
                if i + j < 8:
                    year = years[i + j]
                    row.append(InlineKeyboardButton(str(year), callback_data=f"setfilter_year_{year}_{query}_{page}"))
            keyboard.append(row)
    
    elif filter_type == "season":
        seasons = list(range(1, 16))
        for i in range(0, len(seasons), 5):
            row = []
            for j in range(5):
                if i + j < len(seasons):
                    season = seasons[i + j]
                    row.append(InlineKeyboardButton(f"S{season}", callback_data=f"setfilter_season_{season}_{query}_{page}"))
            keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"back_to_results_{query}_{page}")])
    
    return InlineKeyboardMarkup(keyboard)

async def schedule_delete(chat_id, message_id, delay_minutes=15):
    """Schedule message deletion after delay"""
    delete_time = datetime.utcnow() + timedelta(minutes=delay_minutes)
    await delete_queue.insert_one({
        "chat_id": chat_id,
        "message_id": message_id,
        "delete_time": delete_time
    })

async def auto_delete_job():
    """Background job to delete scheduled messages"""
    try:
        current_time = datetime.utcnow()
        messages_to_delete = await delete_queue.find({"delete_time": {"$lte": current_time}}).to_list(length=None)
        
        for msg in messages_to_delete:
            try:
                await app.delete_messages(msg["chat_id"], msg["message_id"])
                await delete_queue.delete_one({"_id": msg["_id"]})
            except Exception as e:
                logger.error(f"Delete error: {e}")
                await delete_queue.delete_one({"_id": msg["_id"]})
    except Exception as e:
        logger.error(f"Auto delete job error: {e}")

# ==================== BOT HANDLERS ====================

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if await is_banned(message.from_user.id):
        return await message.reply("❌ You are banned from using this bot.")
    
    await message.reply(
        "👋 **Welcome to File Sharing Bot!**\n\n"
        "🔍 Search for files in the group\n"
        "📥 Get files directly in DM\n"
        "⏰ Files auto-delete after 15 minutes\n\n"
        "Go to the group and search for movies/series!"
    )

@app.on_message(filters.text & filters.group)
async def handle_search(client, message):
    if message.chat.id != GROUP_ID:
        return
    
    if await is_banned(message.from_user.id):
        return
    
    query = message.text.strip()
    
    # Show loading
    loading_msg = await message.reply("🔍 **Searching...**")
    
    try:
        # Search files
        files = await search_files(query)
        
        # If no results, try fuzzy search
        if not files:
            suggestions = await fuzzy_search(query)
            
            if suggestions:
                keyboard = []
                for suggestion in suggestions[:5]:
                    keyboard.append([InlineKeyboardButton(
                        f"🔍 {suggestion[:50]}...",
                        callback_data=f"fuzzy_search_{suggestion}"
                    )])
                
                await loading_msg.edit(
                    f"❌ No exact results for **{query}**\n\n"
                    "Did you mean:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                await schedule_delete(message.chat.id, loading_msg.id)
            else:
                await loading_msg.edit(f"❌ No results found for **{query}**")
                await schedule_delete(message.chat.id, loading_msg.id)
            
            await schedule_delete(message.chat.id, message.id)
            return
        
        # Send results to DM
        total_results = len(files)
        total_pages = (total_results + 9) // 10
        
        result_text = f"📁 **Results for \"{query}\"** - {total_results} files found\n\n"
        
        # First page (10 results)
        for file in files[:10]:
            size = format_size(file.get("file_size", 0))
            result_text += f"[{size}] {file['file_name']}\n\n"
        
        keyboard = create_result_keyboard(1, total_pages, query=query)
        
        try:
            sent = await client.send_message(
                message.from_user.id,
                result_text,
                reply_markup=keyboard
            )
            await schedule_delete(message.from_user.id, sent.id)
            await loading_msg.edit("✅ **Results sent to DM!**")
        except Exception as e:
            await loading_msg.edit("❌ Please start the bot in DM first: @YourBotUsername")
        
        await schedule_delete(message.chat.id, loading_msg.id)
        await schedule_delete(message.chat.id, message.id)
    
    except Exception as e:
        logger.error(f"Search error: {e}")
        await loading_msg.edit("❌ Search failed. Try again.")
        await schedule_delete(message.chat.id, loading_msg.id)

@app.on_callback_query()
async def handle_callbacks(client, callback: CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id
    
    if await is_banned(user_id):
        return await callback.answer("You are banned!", show_alert=True)
    
    try:
        # Fuzzy search selection
        if data.startswith("fuzzy_search_"):
            query = data.replace("fuzzy_search_", "")
            await callback.message.edit("🔄 **Searching...**")
            
            files = await search_files(query)
            total_results = len(files)
            total_pages = (total_results + 9) // 10
            
            result_text = f"📁 **Results for \"{query}\"** - {total_results} files found\n\n"
            
            for file in files[:10]:
                size = format_size(file.get("file_size", 0))
                result_text += f"[{size}] {file['file_name']}\n\n"
            
            keyboard = create_result_keyboard(1, total_pages, query=query)
            await callback.message.edit(result_text, reply_markup=keyboard)
        
        # Pagination
        elif data.startswith("page_"):
            parts = data.split("_")
            query = parts[1]
            page = int(parts[2])
            
            files = await search_files(query)
            total_results = len(files)
            total_pages = (total_results + 9) // 10
            
            start = (page - 1) * 10
            end = start + 10
            
            result_text = f"📁 **Results for \"{query}\"** - {total_results} files found\n\n"
            
            for file in files[start:end]:
                size = format_size(file.get("file_size", 0))
                result_text += f"[{size}] {file['file_name']}\n\n"
            
            keyboard = create_result_keyboard(page, total_pages, query=query)
            await callback.message.edit(result_text, reply_markup=keyboard)
        
        # Filter buttons
        elif data.startswith("filter_"):
            parts = data.split("_")
            filter_type = parts[1]
            query = parts[2]
            page = int(parts[3])
            
            keyboard = create_filter_keyboard(filter_type, query, page)
            await callback.message.edit(
                f"🎯 **Select {filter_type.title()}:**",
                reply_markup=keyboard
            )
        
        # Set filter and search
        elif data.startswith("setfilter_"):
            parts = data.split("_")
            filter_type = parts[1]
            filter_value = parts[2]
            query = parts[3]
            page = int(parts[4])
            
            filters = {filter_type: filter_value}
            files = await search_files(query, filters)
            
            total_results = len(files)
            total_pages = (total_results + 9) // 10
            
            result_text = f"📁 **Filtered Results** - {total_results} files\n"
            result_text += f"🎯 {filter_type.title()}: {filter_value}\n\n"
            
            for file in files[:10]:
                size = format_size(file.get("file_size", 0))
                result_text += f"[{size}] {file['file_name']}\n\n"
            
            keyboard = create_result_keyboard(1, total_pages, query=query)
            await callback.message.edit(result_text, reply_markup=keyboard)
        
        await callback.answer()
    
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await callback.answer("Error occurred!", show_alert=True)

# ==================== ADMIN COMMANDS ====================

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast_message(client, message):
    if len(message.command) < 2:
        return await message.reply("Usage: /broadcast <message>")
    
    broadcast_text = message.text.split(None, 1)[1]
    users = await users_collection.distinct("user_id")
    
    success = 0
    failed = 0
    
    status_msg = await message.reply("📢 Broadcasting...")
    
    for user_id in users:
        try:
            await client.send_message(user_id, broadcast_text)
            success += 1
        except:
            failed += 1
    
    await status_msg.edit(
        f"✅ Broadcast Complete!\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}"
    )

@app.on_message(filters.command("ban") & filters.user(ADMIN_IDS))
async def ban_user_command(client, message):
    if len(message.command) < 2:
        return await message.reply("Usage: /ban <user_id>")
    
    try:
        user_id = int(message.command[1])
        await ban_user(user_id)
        await message.reply(f"✅ User {user_id} has been banned.")
    except ValueError:
        await message.reply("❌ Invalid user ID")

@app.on_message(filters.command("unban") & filters.user(ADMIN_IDS))
async def unban_user_command(client, message):
    if len(message.command) < 2:
        return await message.reply("Usage: /unban <user_id>")
    
    try:
        user_id = int(message.command[1])
        await unban_user(user_id)
        await message.reply(f"✅ User {user_id} has been unbanned.")
    except ValueError:
        await message.reply("❌ Invalid user ID")

@app.on_message(filters.command("addchannel") & filters.user(ADMIN_IDS))
async def add_channel(client, message):
    if len(message.command) < 2:
        return await message.reply("Usage: /addchannel <channel_id>")
    
    try:
        channel_id = int(message.command[1])
        await channels_collection.insert_one({"channel_id": channel_id})
        await message.reply(f"✅ Channel {channel_id} added. Indexing files...")
        
        # Index files from channel
        await index_channel(channel_id)
        await message.reply("✅ Indexing complete!")
    except ValueError:
        await message.reply("❌ Invalid channel ID")

async def index_channel(channel_id):
    """Index all files from a channel"""
    try:
        async for message in app.get_chat_history(channel_id):
            if message.document or message.video:
                file_name = message.document.file_name if message.document else message.video.file_name
                file_size = message.document.file_size if message.document else message.video.file_size
                
                info = parse_file_info(file_name)
                
                await files_collection.update_one(
                    {"file_id": message.id, "channel_id": channel_id},
                    {"$set": {
                        "file_name": file_name,
                        "file_size": file_size,
                        "file_id": message.id,
                        "channel_id": channel_id,
                        "quality": info["quality"],
                        "year": info["year"],
                        "language": info["language"],
                        "season": info["season"]
                    }},
                    upsert=True
                )
    except Exception as e:
        logger.error(f"Indexing error: {e}")

@app.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def show_stats(client, message):
    total_files = await files_collection.count_documents({})
    total_users = await users_collection.count_documents({})
    total_channels = await channels_collection.count_documents({})
    
    await message.reply(
        f"📊 **Bot Statistics**\n\n"
        f"📁 Total Files: {total_files}\n"
        f"👥 Total Users: {total_users}\n"
        f"📢 Total Channels: {total_channels}"
    )

# ==================== HEALTH CHECK FOR RENDER ====================

async def health_check(request):
    return web.Response(text="Bot is running! ✅", status=200)

async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get("/", health_check)
    app_web.router.add_get("/health", health_check)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Web server started on port {PORT}")

# ==================== MAIN ====================

async def main():
    # Start web server
    await start_web_server()
    
    # Start scheduler
    scheduler.add_job(auto_delete_job, "interval", minutes=1)
    scheduler.start()
    
    # Start bot
    await app.start()
    logger.info("🤖 Bot started successfully!")
    
    # Keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
