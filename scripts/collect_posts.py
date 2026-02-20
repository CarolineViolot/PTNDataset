import asyncio
import argparse
import os
import json
import logging
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.tl import functions
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
)


class TelegramCrawlerConfig:
    """Configuration class for Telegram Crawler"""

    def __init__(self, config_path, api_name, session_name, data_folder, channels_json, log_file):
        # API Configuration
        with open(config_path) as f:
            api_configs = json.load(f)

        if api_name not in api_configs:
            raise KeyError(f"API name '{api_name}' not found in {config_path}.")

        curr_id = api_configs[api_name]
        self.api_id = curr_id['api_id']
        self.api_hash = curr_id['api_hash']
        self.session_name = session_name

        # Date range configuration
        self.start_date = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.end_date = datetime(2025, 7, 15, 23, 59, 59, tzinfo=timezone.utc)

        # Limits
        self.comment_limit = 20000

        # Folders
        self.data_folder = data_folder
        os.makedirs(self.data_folder, exist_ok=True)

        # Rate limiting configuration
        self.delay_between_messages = 0  # seconds between each message fetch
        self.delay_between_comments = 0  # seconds between fetching comments

        # Files
        self.channels_json = channels_json
        self.log_file = log_file

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )


class MediaExtractor:
    """Class to handle media metadata extraction"""

    def __init__(self, logger):
        self.logger = logger

    def extract_metadata(self, message):
        """Extract photo_id, access_hash, file_reference, and size from message.media for all media types"""
        if not message.media:
            return None

        media_info = {
            'media_type': str(type(message.media).__name__),
            'has_media': True
        }

        try:
            if isinstance(message.media, MessageMediaPhoto):
                photo = message.media.photo
                if photo:
                    media_info['photo_id'] = photo.id
                    media_info['access_hash'] = photo.access_hash
                    media_info['file_reference'] = photo.file_reference.hex() if photo.file_reference else None
                    media_info['media_type'] = 'photo'

                    # Extract photo size (get the largest available size)
                    if hasattr(photo, 'sizes') and photo.sizes:
                        largest_size = 0
                        for size in photo.sizes:
                            # PhotoSize, PhotoSizeProgressive have 'size' attribute
                            if hasattr(size, 'size'):
                                if size.size > largest_size:
                                    largest_size = size.size
                            # PhotoSizeProgressive also has 'sizes' list with multiple progressive sizes
                            elif hasattr(size, 'sizes') and isinstance(size.sizes, list):
                                max_progressive = max(size.sizes) if size.sizes else 0
                                if max_progressive > largest_size:
                                    largest_size = max_progressive

                        if largest_size > 0:
                            media_info['size'] = largest_size          # Size in bytes
                            media_info['size_mb'] = round(largest_size / (1024 * 1024), 2)  # Size in MB

            elif isinstance(message.media, MessageMediaDocument):
                document = message.media.document
                if document:
                    media_info['document_id'] = document.id
                    media_info['access_hash'] = document.access_hash
                    media_info['file_reference'] = document.file_reference.hex() if document.file_reference else None
                    media_info['media_type'] = 'document'

                    # Extract document size
                    if hasattr(document, 'size') and document.size:
                        media_info['size'] = document.size          # Size in bytes
                        media_info['size_mb'] = round(document.size / (1024 * 1024), 2)  # Size in MB

                    # Check if it's a video
                    if hasattr(document, 'mime_type') and document.mime_type:
                        if document.mime_type.startswith('video/'):
                            media_info['media_type'] = 'video'
                        media_info['mime_type'] = document.mime_type

        except Exception as e:
            self.logger.warning(f"Error extracting media metadata for message {message.id}: {e}")
            return {
                'has_media': True,
                'media_type': str(type(message.media).__name__),
                'extraction_error': str(e)
            }

        return media_info


class DataManager:
    """Class to handle JSON data persistence"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def save_to_json(self, data, output_file):
        """Save data to JSON file using an atomic write (temp file + rename)"""
        try:
            temp_file = output_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            if os.path.exists(output_file):
                os.replace(temp_file, output_file)
            else:
                os.rename(temp_file, output_file)
            return True
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
            return False

    def load_existing_data(self, output_file):
        """Load existing data if file exists"""
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load existing data: {e}")
                return []
        return []

    def read_channels_from_json(self):
        """
        Read channel handles from a JSON file like:
          {"channel_handles": ["bayjuvan", "esfahanemrouz", ...]}

        Returns a list of normalized handles/usernames suitable for Telethon:
          - strips whitespace
          - removes "https://t.me/" prefix
          - removes leading "@"
        """
        channels = []
        path = self.config.channels_json

        if not os.path.exists(path):
            self.logger.error(f"JSON file not found: {path}")
            return channels

        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            if not isinstance(payload, dict) or "channel_handles" not in payload:
                self.logger.error(
                    f"Invalid JSON structure in {path}. Expected {{'channel_handles': [...]}}"
                )
                return channels

            raw = payload.get("channel_handles", [])
            if not isinstance(raw, list):
                self.logger.error(
                    f"Invalid 'channel_handles' type in {path}. Expected a list."
                )
                return channels

            for x in raw:
                if not isinstance(x, str):
                    continue
                s = x.strip()
                if not s:
                    continue
                if s.startswith("https://t.me/"):
                    s = s.replace("https://t.me/", "", 1).strip()
                if s.startswith("@"):
                    s = s[1:].strip()
                if s:
                    channels.append(s)

            self.logger.info(f"Read {len(channels)} channels from JSON {path}")

        except Exception as e:
            self.logger.error(f"Error reading JSON file {path}: {e}")

        return channels


class MessageProcessor:
    """Class to handle message processing and information extraction"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    async def get_reactions(self, message):
        """Extract reactions from a message"""
        if not message.reactions:
            return [], 0

        reactions_list = []
        total_reactions = 0

        for reaction in message.reactions.results:
            emoji = reaction.reaction.emoticon if hasattr(reaction.reaction, 'emoticon') else str(reaction.reaction)
            count = reaction.count
            reactions_list.append({'emoji': emoji, 'count': count})
            total_reactions += count

        return reactions_list, total_reactions

    async def get_forwarded_info(self, message):
        """Extract forwarded message origin information"""
        if not message.fwd_from:
            return None

        forwarded_info = {}

        if hasattr(message.fwd_from, 'from_id') and message.fwd_from.from_id:
            forwarded_info['origin_from_id'] = str(message.fwd_from.from_id)

        if hasattr(message.fwd_from, 'from_name') and message.fwd_from.from_name:
            forwarded_info['origin_from_name'] = message.fwd_from.from_name

        if hasattr(message.fwd_from, 'date') and message.fwd_from.date:
            forwarded_info['origin_date'] = message.fwd_from.date.isoformat()

        if hasattr(message.fwd_from, 'channel_post') and message.fwd_from.channel_post:
            forwarded_info['origin_channel_post'] = message.fwd_from.channel_post

        if hasattr(message.fwd_from, 'post_author') and message.fwd_from.post_author:
            forwarded_info['origin_post_author'] = message.fwd_from.post_author

        if hasattr(message.fwd_from, 'saved_from_peer') and message.fwd_from.saved_from_peer:
            forwarded_info['origin_saved_from_peer'] = str(message.fwd_from.saved_from_peer)

        if hasattr(message.fwd_from, 'saved_from_msg_id') and message.fwd_from.saved_from_msg_id:
            forwarded_info['origin_saved_from_msg_id'] = message.fwd_from.saved_from_msg_id

        return forwarded_info

    async def get_comments(self, client, message, target):
        """Fetch comments/replies to a message"""
        comments_list = []

        if message.replies and message.replies.replies > 0:
            try:
                await asyncio.sleep(self.config.delay_between_comments)
                async for reply in client.iter_messages(
                    target,
                    reply_to=message.id,
                    limit=1000
                ):
                    comment_dict = {
                        'id': reply.id,
                        'date': reply.date.isoformat(),
                        'sender_id': reply.sender_id,
                        'text': reply.text or '[no text]',
                        'has_media': bool(reply.media)
                    }
                    comments_list.append(comment_dict)
            except Exception as e:
                self.logger.warning(f"Could not fetch comments for message {message.id}: {e}")

        return comments_list


class TelegramCrawler:

    def __init__(self, config=None):
        self.config = config if config else TelegramCrawlerConfig()
        self.logger = logging.getLogger(__name__)
        self.data_manager = DataManager(self.config, self.logger)
        self.media_extractor = MediaExtractor(self.logger)
        self.message_processor = MessageProcessor(self.config, self.logger)
        self.client = None

    def sanitize_filename(self, name):
        """Sanitize channel name for use as filename"""
        invalid_chars = '<>:"/\\|?*'
        sanitized = name
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        sanitized = sanitized.strip('. ')
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized if sanitized else 'unknown_channel'

    async def initialize_client(self):
        """Initialize and connect the Telegram client"""
        self.client = TelegramClient(
            self.config.session_name,
            self.config.api_id,
            self.config.api_hash,
            connection_retries=5,
            retry_delay=1
        )

        self.logger.info("Connecting to Telegram...")
        await asyncio.wait_for(self.client.connect(), timeout=30)

        if not await self.client.is_user_authorized():
            self.logger.info("Please login...")
            await self.client.start()
        else:
            self.logger.info("Already authorized")

    async def disconnect_client(self):
        """Disconnect the Telegram client"""
        if self.client:
            self.logger.info("Disconnecting...")
            try:
                await self.client.disconnect()
                await asyncio.sleep(1)
            except Exception:
                pass
            self.logger.info("Disconnected")

    async def process_channel(self, channel_url):
        """Process a single channel and return its messages"""
        self.logger.info(f"Processing channel: {channel_url}")

        try:
            target_entity = await self.client.get_entity(channel_url)
            channel_id = target_entity.id if hasattr(target_entity, 'id') else None

            if not channel_id:
                self.logger.error(f"Could not get channel ID for {channel_url}")
                return None, None

            channel_name = channel_url.replace('https://t.me/', '')
            output_file = os.path.join(
                self.config.data_folder, f'telegram_messages-{channel_name}.json'
            )

            # Load existing data for this channel
            messages_data = self.data_manager.load_existing_data(output_file)
            existing_ids = {msg['id'] for msg in messages_data}

            message_count = len(messages_data)
            skipped_count = 0
            new_messages = 0

            self.logger.debug(
                f"Fetching messages from {self.config.start_date.date()} to "
                f"{self.config.end_date.date()} for channel: {channel_name} (ID: {channel_id})"
            )
            if messages_data:
                self.logger.debug(f"Found {len(messages_data)} existing messages in {output_file}")

            async for message in self.client.iter_messages(
                channel_url,
                offset_date=self.config.end_date,
                reverse=False
            ):
                # Stop once we've passed the start date
                if message.date < self.config.start_date:
                    self.logger.debug(
                        f"Reached messages before {self.config.start_date.date()}, stopping..."
                    )
                    break

                # Skip already-processed messages
                if message.id in existing_ids:
                    skipped_count += 1
                    continue

                if self.config.start_date <= message.date <= self.config.end_date:
                    message_count += 1
                    new_messages += 1

                    reactions, total_reactions = await self.message_processor.get_reactions(message)
                    comments = await self.message_processor.get_comments(self.client, message, channel_url)
                    forwarded_info = await self.message_processor.get_forwarded_info(message)
                    media_info = self.media_extractor.extract_metadata(message)

                    message_dict = {
                        'id': message.id,
                        'date': message.date.isoformat(),
                        'sender_id': message.sender_id,
                        'text': message.text or '[no text]',
                        'views': message.views,
                        'forwards': message.forwards,
                        'reply_to': message.reply_to_msg_id,
                        'reactions': reactions,
                        'reactions_types': len(reactions),
                        'reactions_total': total_reactions,
                        'comments': comments,
                        'comments_count': len(comments),
                        'is_forwarded': forwarded_info is not None,
                    }

                    if media_info:
                        message_dict.update(media_info)
                    else:
                        message_dict['has_media'] = False
                        message_dict['media_type'] = None

                    if forwarded_info:
                        message_dict.update(forwarded_info)

                    messages_data.append(message_dict)

                    if self.data_manager.save_to_json(messages_data, output_file):
                        save_status = "saved"
                    else:
                        save_status = "warning"

                    forwarded_status = "Forwarded" if forwarded_info else ""
                    self.logger.debug(
                        f"{save_status.upper()} Message #{message_count} {forwarded_status} - "
                        f"ID: {message.id}, Date: {message.date}, "
                        f"Reactions: {total_reactions} ({len(reactions)} types), "
                        f"Comments: {len(comments)}"
                    )

                    await asyncio.sleep(self.config.delay_between_messages)
                else:
                    skipped_count += 1

            # Final save
            self.data_manager.save_to_json(messages_data, output_file)

            self.logger.info(
                f"Successfully processed {len(messages_data)} total message(s) "
                f"for channel: {channel_name} (ID: {channel_id})"
            )
            self.logger.info(f"  New messages: {new_messages}")
            self.logger.info(f"  Skipped: {skipped_count}")
            self.logger.info(f"  Saved to: {output_file}")

            return messages_data, channel_name

        except Exception as e:
            self.logger.error(f"Error processing channel {channel_url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None, None

    async def run(self):
        """Main method to process all channels from JSON"""
        try:
            await self.initialize_client()

            channel_urls = self.data_manager.read_channels_from_json()

            if not channel_urls:
                self.logger.error("No channels found in JSON file. Exiting.")
                return

            self.logger.info(f"Processing {len(channel_urls)} channel(s)")
            self.logger.debug(
                f"Date range: {self.config.start_date.date()} to {self.config.end_date.date()}"
            )

            for idx, channel_url in enumerate(channel_urls, 1):
                self.logger.debug(f"\n{'=' * 60}")
                self.logger.debug(f"Processing channel {idx}/{len(channel_urls)}: {channel_url}")
                self.logger.debug(f"{'=' * 60}")

                try:
                    messages_data, channel_name = await self.process_channel(channel_url)

                    if messages_data is not None:
                        self.logger.debug(
                            f"✅ Completed channel {idx}/{len(channel_urls)}: "
                            f"{channel_name} ({channel_url})"
                        )
                    else:
                        self.logger.warning(
                            f"⚠️ Failed to process channel {idx}/{len(channel_urls)}: {channel_url}"
                        )

                except KeyboardInterrupt:
                    self.logger.warning("Script interrupted by user")
                    raise
                except Exception as e:
                    self.logger.error(f"Error processing channel {channel_url}: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    continue

            self.logger.info(f"{'=' * 60}")
            self.logger.info("✅ All channels processed successfully")

        except KeyboardInterrupt:
            self.logger.warning("\nScript interrupted by user")
        except Exception as e:
            self.logger.error(f"\nAn error occurred: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            await self.disconnect_client()


def parse_args():
    parser = argparse.ArgumentParser(description="Collect posts from Telegram channels")
    parser.add_argument('--config_path', type=str, required=True,
                        help="Path to JSON file containing API credentials")
    parser.add_argument('--api_name', type=str, required=True,
                        help="Key name of the API credentials to use in the config file")
    parser.add_argument('--session_name', type=str, required=True,
                        help="Filename of the session to use for this collect")
    parser.add_argument('--data_folder', type=str, required=True,
                        help="Path to folder where the posts files should be saved")
    parser.add_argument('--channels_list_json', type=str, required=True,
                        help="Path to the json file containing the list of channels from which to collect posts")
    parser.add_argument('--log_file', type=str, required=True)
    return parser.parse_args()


async def main():
    args = parse_args()
    config = TelegramCrawlerConfig(
        config_path=args.config_path,
        api_name=args.api_name,
        session_name=args.session_name,
        data_folder=args.data_folder,
        channels_json=args.channels_list_json,
        log_file=args.log_file
    )
    crawler = TelegramCrawler(config=config)
    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
