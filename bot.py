import asyncio
import logging
import json
from telethon import TelegramClient, events, errors
from telethon.tl.types import MessageMediaWebPage
from collections import deque
from datetime import datetime

API_ID = 28451755  # Replace with your API ID
API_HASH = "c888900d408dcd71e8bf31f5aa15ae0e"  # Replace with your API hash
SESSION_FILE = "userbot_session"
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Configuration
MAPPINGS_FILE = "channel_mappings.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_QUEUE_SIZE = 100
MAX_MAPPING_HISTORY = 1000
MONITOR_CHAT_ID = None

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("forward_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ForwardBot")

# Data structures
channel_mappings = {}
message_queue = deque(maxlen=MAX_QUEUE_SIZE)
is_connected = False
pair_stats = {}

def save_mappings():
    try:
        with open(MAPPINGS_FILE, "w") as f:
            json.dump(channel_mappings, f)
        logger.info("Channel mappings saved to file.")
    except Exception as e:
        logger.error(f"Error saving mappings: {e}")

def load_mappings():
    global channel_mappings
    try:
        with open(MAPPINGS_FILE, "r") as f:
            channel_mappings = json.load(f)
        logger.info(f"Loaded {sum(len(v) for v in channel_mappings.values())} mappings from file.")
        for user_id, pairs in channel_mappings.items():
            if user_id not in pair_stats:
                pair_stats[user_id] = {}
            for pair_name in pairs:
                pair_stats[user_id][pair_name] = {
                    'forwarded': 0, 'edited': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                }
    except FileNotFoundError:
        logger.info("No existing mappings file found. Starting fresh.")
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")

async def process_message_queue():
    while message_queue and is_connected:
        message_data = message_queue.popleft()
        await forward_message_with_retry(*message_data)

def filter_blacklisted_words(text, blacklist):
    if not text or not blacklist:
        return text
    for word in blacklist:
        text = text.replace(word, "***")
    return text

def check_blocked_sentences(text, blocked_sentences):
    if not text or not blocked_sentences:
        return False, None
    for sentence in blocked_sentences:
        if sentence.lower() in text.lower():
            return True, sentence
    return False, None

def filter_urls(text, block_urls):
    if not text or not block_urls:
        return text
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s]*)?'
    import re
    return re.sub(url_pattern, '[URL REMOVED]', text)

def remove_header_footer(text, header_pattern, footer_pattern):
    if not text:
        return text
    if header_pattern and text.startswith(header_pattern):
        text = text[len(header_pattern):].strip()
    if footer_pattern and text.endswith(footer_pattern):
        text = text[:-len(footer_pattern)].strip()
    return text

def apply_custom_header_footer(text, custom_header, custom_footer):
    if not text:
        return text
    result = text
    if custom_header:
        result = f"{custom_header} {result}"
    if custom_footer:
        result = f"{result.rstrip()} {custom_footer}"
    return result.strip()

async def forward_message_with_retry(event, mapping, user_id, pair_name):
    for attempt in range(MAX_RETRIES):
        try:
            message_text = event.message.text or event.message.raw_text or ""
            if mapping.get('blocked_sentences'):
                should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
                if should_block:
                    logger.info(f"Message blocked due to blocked sentence: '{matching_sentence}'")
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True

            if mapping.get('blacklist') and message_text:
                message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
                if message_text.strip() == "***":
                    logger.info("Message entirely blocked due to blacklist filter")
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True

            if mapping.get('block_urls', False) and message_text:
                message_text = filter_urls(message_text, mapping['block_urls'])

            if (mapping.get('header_pattern') or mapping.get('footer_pattern')) and message_text:
                message_text = remove_header_footer(
                    message_text,
                    mapping.get('header_pattern', ''),
                    mapping.get('footer_pattern', '')
                )

            if mapping.get('remove_mentions', False) and message_text:
                import re
                message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
                message_text = re.sub(r'\s+', ' ', message_text).strip()

            if not message_text.strip() and not event.message.media:
                logger.info("Message skipped: empty after filtering")
                pair_stats[user_id][pair_name]['blocked'] += 1
                return True

            # Apply custom header and footer
            message_text = apply_custom_header_footer(
                message_text,
                mapping.get('custom_header', ''),
                mapping.get('custom_footer', '')
            )

            reply_to = await handle_reply_mapping(event, mapping)
            media = event.message.media
            # Check if the media is a webpage preview
            is_webpage = isinstance(media, MessageMediaWebPage)
            # Only enable link preview if it's a webpage and URLs aren't blocked
            has_url_preview = is_webpage and not mapping.get('block_urls', False)

            # Prepare parameters for sending the message
            send_params = {
                'entity': int(mapping['destination']),
                'message': message_text,
                'link_preview': has_url_preview,
                'reply_to': reply_to,
                'silent': event.message.silent,
                'formatting_entities': event.message.entities
            }

            # Only include the file parameter if media exists and it's not a webpage
            if media and not is_webpage:
                send_params['file'] = media

            sent_message = await client.send_message(**send_params)

            await store_message_mapping(event, mapping, sent_message)
            pair_stats[user_id][pair_name]['forwarded'] += 1
            pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
            logger.info(f"Message forwarded from {mapping['source']} to {mapping['destination']} (ID: {sent_message.id})")
            return True

        except errors.FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait error, sleeping for {wait_time} seconds...")
            await asyncio.sleep(wait_time)
        except (errors.RPCError, ConnectionError) as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to forward message after {MAX_RETRIES} attempts")
                return False
        except Exception as e:
            logger.error(f"Unexpected error forwarding message: {e}")
            return False

async def edit_forwarded_message(event, mapping, user_id, pair_name):
    try:
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if not hasattr(client, 'forwarded_messages'):
            logger.warning("No forwarded_messages attribute found on client")
            return
        if mapping_key not in client.forwarded_messages:
            logger.warning(f"No mapping found for message: {mapping_key}")
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        forwarded_msg = await client.get_messages(int(mapping['destination']), ids=forwarded_msg_id)
        if not forwarded_msg:
            logger.warning(f"Forwarded message {forwarded_msg_id} not found in destination {mapping['destination']}")
            del client.forwarded_messages[mapping_key]
            return

        message_text = event.message.text or event.message.raw_text or ""
        if mapping.get('blocked_sentences'):
            should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
            if should_block:
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blocked sentence: '{matching_sentence}'")
                pair_stats[user_id][pair_name]['blocked'] += 1
                return

        if mapping.get('blacklist') and message_text:
            message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
            if message_text.strip() == "***":
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blacklist filter")
                pair_stats[user_id][pair_name]['blocked'] += 1
                return

        if mapping.get('block_urls', False) and message_text:
            message_text = filter_urls(message_text, mapping['block_urls'])

        if (mapping.get('header_pattern') or mapping.get('footer_pattern')) and message_text:
            message_text = remove_header_footer(
                message_text,
                mapping.get('header_pattern', ''),
                mapping.get('footer_pattern', '')
            )

        if mapping.get('remove_mentions', False) and message_text:
            import re
            message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
            message_text = re.sub(r'\s+', ' ', message_text).strip()

        if not message_text.strip() and not event.message.media:
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            logger.info(f"Forwarded message {forwarded_msg_id} deleted: empty after filtering")
            pair_stats[user_id][pair_name]['blocked'] += 1
            return

        # Apply custom header and footer on edit
        message_text = apply_custom_header_footer(
            message_text,
            mapping.get('custom_header', ''),
            mapping.get('custom_footer', '')
        )

        media = event.message.media
        is_webpage = isinstance(media, MessageMediaWebPage)
        has_url_preview = is_webpage and not mapping.get('block_urls', False)

        # Prepare parameters for editing the message
        edit_params = {
            'entity': int(mapping['destination']),
            'message': forwarded_msg_id,
            'text': message_text,
            'link_preview': has_url_preview,
            'formatting_entities': event.message.entities
        }

        # Only include file if media exists and it's not a webpage
        if media and not is_webpage:
            edit_params['file'] = media

        await client.edit_message(**edit_params)
        pair_stats[user_id][pair_name]['edited'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        logger.info(f"Forwarded message {forwarded_msg_id} edited in {mapping['destination']}")

    except errors.MessageAuthorRequiredError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Bot must be the original author")
    except errors.MessageIdInvalidError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Message ID is invalid or deleted")
        if mapping_key in client.forwarded_messages:
            del client.forwarded_messages[mapping_key]
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait error while editing, sleeping for {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error editing forwarded message {forwarded_msg_id}: {e}")

async def handle_reply_mapping(event, mapping):
    if not hasattr(event.message, 'reply_to') or not event.message.reply_to:
        return None
    try:
        source_reply_id = event.message.reply_to.reply_to_msg_id
        if not source_reply_id:
            return None
        mapping_key = f"{mapping['source']}:{source_reply_id}"
        if hasattr(client, 'forwarded_messages') and mapping_key in client.forwarded_messages:
            return client.forwarded_messages[mapping_key]
        replied_msg = await client.get_messages(int(mapping['source']), ids=source_reply_id)
        if replied_msg and replied_msg.text:
            dest_msgs = await client.get_messages(int(mapping['destination']), search=replied_msg.text[:20], limit=5)
            if dest_msgs:
                return dest_msgs[0].id
    except Exception as e:
        logger.error(f"Error handling reply mapping: {e}")
    return None

async def store_message_mapping(event, mapping, sent_message):
    try:
        if not hasattr(event.message, 'id'):
            return
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        if len(client.forwarded_messages) >= MAX_MAPPING_HISTORY:
            oldest_key = next(iter(client.forwarded_messages))
            client.forwarded_messages.pop(oldest_key)
        source_msg_id = event.message.id
        mapping_key = f"{mapping['source']}:{source_msg_id}"
        client.forwarded_messages[mapping_key] = sent_message.id
    except Exception as e:
        logger.error(f"Error storing message mapping: {e}")

@client.on(events.NewMessage(pattern='(?i)^/start$'))
async def start(event):
    await event.reply("✅ Bot is running! Use /commands to see available commands.")

@client.on(events.NewMessage(pattern='(?i)^/commands$'))
async def list_commands(event):
    commands = """
    📌 Available Commands:
    /setpair <name> <source> <destination> [remove_mentions]
    /listpairs - List all forwarding pairs
    /pausepair <name> - Pause a forwarding pair
    /startpair <name> - Resume a forwarding pair
    /clearpairs - Clear all forwarding pairs
    /togglementions <name> - Toggle mention removal
    /monitor - Show detailed status of all pairs

    📋 Filtering Commands:
    /addblacklist <name> <word1,word2,...> - Add words to blacklist
    /clearblacklist <name> - Clear blacklist for a pair
    /showblacklist <name> - Show blacklisted words
    /toggleurlblock <name> - Toggle blocking of URLs
    /setheader <name> <pattern> - Set header pattern to remove
    /setfooter <name> <pattern> - Set footer pattern to remove
    /clearheaderfooter <name> - Clear header/footer patterns

    📝 Custom Text Commands:
    /setcustomheader <name> <text> - Set custom header to add
    /setcustomfooter <name> <text> - Set custom footer to add (with space before)
    /clearcustomheaderfooter <name> - Clear custom header/footer

    🚫 Message Blocking Commands:
    /blocksentence <name> <sentence> - Block messages with this sentence
    /clearblocksentences <name> - Clear blocked sentences
    /showblocksentences <name> - Show blocked sentences
    """
    await event.reply(commands)

@client.on(events.NewMessage(pattern='(?i)^/monitor$'))
async def monitor_pairs(event):
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("⚠️ No forwarding pairs found.")
        return
    report = ["📊 Forwarding Pairs Monitor"]
    total_queued = len(message_queue)
    for pair_name, data in channel_mappings[user_id].items():
        stats = pair_stats.get(user_id, {}).get(pair_name, {'forwarded': 0, 'edited': 0, 'blocked': 0, 'queued': 0, 'last_activity': None})
        report.append(
            f"\n🔹 {pair_name}: {data['source']} → {data['destination']}\n"
            f"   Status: {'Active' if data['active'] else 'Paused'}\n"
            f"   Forwarded: {stats['forwarded']}\n"
            f"   Edited: {stats['edited']}\n"
            f"   Blocked: {stats['blocked']}\n"
            f"   Queued: {stats['queued']}\n"
            f"   Last Activity: {stats['last_activity'] or 'N/A'}"
        )
    report.append(f"\n📥 Total Queued Messages: {total_queued}")
    await event.reply("\n".join(report))

@client.on(events.NewMessage(pattern=r'/setpair (\S+) (\S+) (\S+)(?: (yes|no))?'))
async def set_pair(event):
    pair_name, source, destination, remove_mentions = event.pattern_match.groups()
    user_id = str(event.sender_id)
    remove_mentions = remove_mentions == "yes"
    if user_id not in channel_mappings:
        channel_mappings[user_id] = {}
    if user_id not in pair_stats:
        pair_stats[user_id] = {}
    channel_mappings[user_id][pair_name] = {
        'source': source,
        'destination': destination,
        'active': True,
        'remove_mentions': remove_mentions,
        'blacklist': [],
        'block_urls': False,
        'header_pattern': '',
        'footer_pattern': '',
        'custom_header': '',
        'custom_footer': '',
        'blocked_sentences': []
    }
    pair_stats[user_id][pair_name] = {'forwarded': 0, 'edited': 0, 'blocked': 0, 'queued': 0, 'last_activity': None}
    save_mappings()
    await event.reply(f"✅ Forwarding pair '{pair_name}' added: {source} → {destination} (Remove mentions: {remove_mentions})")

@client.on(events.NewMessage(pattern=r'/blocksentence (\S+) (.+)'))
async def block_sentence(event):
    pair_name = event.pattern_match.group(1)
    sentence = event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        if 'blocked_sentences' not in channel_mappings[user_id][pair_name]:
            channel_mappings[user_id][pair_name]['blocked_sentences'] = []
        channel_mappings[user_id][pair_name]['blocked_sentences'].append(sentence)
        save_mappings()
        await event.reply(f"🚫 Added sentence to block list for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearblocksentences (\S+)$'))
async def clear_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blocked_sentences'] = []
        save_mappings()
        await event.reply(f"🗑️ Block sentences list cleared for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/showblocksentences (\S+)$'))
async def show_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blocked_sentences = channel_mappings[user_id][pair_name].get('blocked_sentences', [])
        if blocked_sentences:
            sentences_list = "\n".join([f"- {sentence}" for sentence in blocked_sentences])
            await event.reply(f"📋 Blocked sentences for '{pair_name}':\n{sentences_list}")
        else:
            await event.reply(f"📋 No blocked sentences for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern=r'/addblacklist (\S+) (.+)'))
async def add_blacklist(event):
    pair_name = event.pattern_match.group(1)
    words = event.pattern_match.group(2).split(',')
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        if 'blacklist' not in channel_mappings[user_id][pair_name]:
            channel_mappings[user_id][pair_name]['blacklist'] = []
        channel_mappings[user_id][pair_name]['blacklist'].extend([word.strip() for word in words])
        channel_mappings[user_id][pair_name]['blacklist'] = list(set(channel_mappings[user_id][pair_name]['blacklist']))
        save_mappings()
        await event.reply(f"🚫 Added {len(words)} word(s) to blacklist for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearblacklist (\S+)$'))
async def clear_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blacklist'] = []
        save_mappings()
        await event.reply(f"🗑️ Blacklist cleared for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/showblacklist (\S+)$'))
async def show_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blacklist = channel_mappings[user_id][pair_name].get('blacklist', [])
        if blacklist:
            words_list = ", ".join(blacklist)
            await event.reply(f"📋 Blacklisted words for '{pair_name}':\n{words_list}")
        else:
            await event.reply(f"📋 No blacklisted words for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/toggleurlblock (\S+)$'))
async def toggle_url_block(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name].get('block_urls', False)
        channel_mappings[user_id][pair_name]['block_urls'] = not current_status
        save_mappings()
        status_text = "ENABLED" if not current_status else "DISABLED"
        await event.reply(f"🔗 URL blocking {status_text} for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setheader (\S+) (.+)'))
async def set_header(event):
    pair_name = event.pattern_match.group(1)
    pattern = event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = pattern
        save_mappings()
        await event.reply(f"✂️ Header pattern set for '{pair_name}': '{pattern}'")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setfooter (\S+) (.+)'))
async def set_footer(event):
    pair_name = event.pattern_match.group(1)
    pattern = event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['footer_pattern'] = pattern
        save_mappings()
        await event.reply(f"✂️ Footer pattern set for '{pair_name}': '{pattern}'")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearheaderfooter (\S+)$'))
async def clear_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = ''
        channel_mappings[user_id][pair_name]['footer_pattern'] = ''
        save_mappings()
        await event.reply(f"🗑️ Header and footer patterns cleared for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setcustomheader (\S+) (.+)'))
async def set_custom_header(event):
    pair_name = event.pattern_match.group(1)
    text = event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = text
        save_mappings()
        await event.reply(f"📝 Custom header set for '{pair_name}': '{text}'")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern=r'/setcustomfooter (\S+) (.+)'))
async def set_custom_footer(event):
    pair_name = event.pattern_match.group(1)
    text = event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_footer'] = text
        save_mappings()
        await event.reply(f"📝 Custom footer set for '{pair_name}': '{text}' (added with a space before)")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearcustomheaderfooter (\S+)$'))
async def clear_custom_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = ''
        channel_mappings[user_id][pair_name]['custom_footer'] = ''
        save_mappings()
        await event.reply(f"🗑️ Custom header and footer cleared for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/togglementions (\S+)$'))
async def toggle_mentions(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name]['remove_mentions']
        channel_mappings[user_id][pair_name]['remove_mentions'] = not current_status
        save_mappings()
        status_text = "ENABLED" if not current_status else "DISABLED"
        await event.reply(f"🔄 Mention removal {status_text} for '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/listpairs$'))
async def list_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings and channel_mappings[user_id]:
        pairs_list = "\n".join([
            f"{name}: {data['source']} → {data['destination']} "
            f"(Active: {data['active']}, Remove Mentions: {data['remove_mentions']}, "
            f"Block URLs: {data.get('block_urls', False)}, "
            f"Header: '{data.get('header_pattern', '')}', "
            f"Footer: '{data.get('footer_pattern', '')}', "
            f"Custom Header: '{data.get('custom_header', '')}', "
            f"Custom Footer: '{data.get('custom_footer', '')}', "
            f"Blacklist: {len(data.get('blacklist', []))} words, "
            f"Blocked Sentences: {len(data.get('blocked_sentences', []))})"
            for name, data in channel_mappings[user_id].items()
        ])
        await event.reply(f"📋 Active Forwarding Pairs:\n{pairs_list}")
    else:
        await event.reply("⚠️ No forwarding pairs found.")

@client.on(events.NewMessage(pattern='(?i)^/pausepair (\S+)$'))
async def pause_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = False
        save_mappings()
        await event.reply(f"⏸️ Forwarding pair '{pair_name}' has been paused.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/startpair (\S+)$'))
async def start_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = True
        save_mappings()
        await event.reply(f"▶️ Forwarding pair '{pair_name}' has been activated.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearpairs$'))
async def clear_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings:
        channel_mappings[user_id] = {}
        pair_stats[user_id] = {}
        save_mappings()
        await event.reply("🗑️ All forwarding pairs have been cleared.")
    else:
        await event.reply("⚠️ No forwarding pairs found.")

@client.on(events.NewMessage)
async def forward_messages(event):
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    success = await forward_message_with_retry(event, mapping, user_id, pair_name)
                    if not success:
                        message_queue.append((event, mapping, user_id, pair_name))
                        pair_stats[user_id][pair_name]['queued'] += 1
                        logger.warning(f"Message queued due to forwarding failure for pair '{pair_name}'")
                except Exception as e:
                    logger.error(f"Error in forward_messages for pair '{pair_name}': {e}")
                    message_queue.append((event, mapping, user_id, pair_name))
                    pair_stats[user_id][pair_name]['queued'] += 1
                return

@client.on(events.MessageEdited)
async def handle_message_edit(event):
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    await edit_forwarded_message(event, mapping, user_id, pair_name)
                except Exception as e:
                    logger.error(f"Error handling message edit for pair '{pair_name}': {e}")
                return

async def check_connection_status():
    global is_connected
    while True:
        current_status = client.is_connected()
        if current_status and not is_connected:
            is_connected = True
            logger.info("Connection established, processing queued messages...")
            await process_message_queue()
        elif not current_status and is_connected:
            is_connected = False
            logger.warning("Connection lost, messages will be queued...")
        await asyncio.sleep(5)

async def send_periodic_report():
    while True:
        await asyncio.sleep(3600)  # Every hour
        if not is_connected or not MONITOR_CHAT_ID:
            continue
        for user_id in channel_mappings:
            report = ["📈 Hourly Forwarding Report"]
            total_queued = len(message_queue)
            for pair_name, data in channel_mappings[user_id].items():
                stats = pair_stats.get(user_id, {}).get(pair_name, {'forwarded': 0, 'edited': 0, 'blocked': 0, 'queued': 0, 'last_activity': None})
                report.append(
                    f"\n🔹 {pair_name}: {data['source']} → {data['destination']}\n"
                    f"   Status: {'Active' if data['active'] else 'Paused'}\n"
                    f"   Forwarded: {stats['forwarded']}\n"
                    f"   Edited: {stats['edited']}\n"
                    f"   Blocked: {stats['blocked']}\n"
                    f"   Queued: {stats['queued']}"
                )
            report.append(f"\n📥 Total Queued Messages: {total_queued}")
            try:
                await client.send_message(MONITOR_CHAT_ID, "\n".join(report))
                logger.info("Sent periodic report")
            except Exception as e:
                logger.error(f"Error sending periodic report: {e}")

async def main():
    load_mappings()
    asyncio.create_task(check_connection_status())
    asyncio.create_task(send_periodic_report())
    logger.info("🚀 Bot is starting...")

    try:
        await client.start()
        if not await client.is_user_authorized():
            phone = input("Please enter your phone (or bot token): ")
            await client.start(phone=phone)
            code = input("Please enter the verification code you received: ")
            await client.sign_in(phone=phone, code=code)

        global is_connected, MONITOR_CHAT_ID
        is_connected = client.is_connected()
        MONITOR_CHAT_ID = (await client.get_me()).id

        if is_connected:
            logger.info("Initial connection established")
        else:
            logger.warning("Initial connection not established")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Bot is shutting down...")
        save_mappings()

if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
