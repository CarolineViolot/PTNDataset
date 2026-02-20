import json
import asyncio
import argparse
from telethon import TelegramClient, functions
import pandas as pd


async def get_channel_info(client, channel_url):
    """Get full metadata for a single channel"""
    try:
        entity = await client.get_entity(channel_url)

        channel_info = {
            'id': entity.id if hasattr(entity, 'id') else None,
            'title': entity.title if hasattr(entity, 'title') else None,
            'username': entity.username if hasattr(entity, 'username') else None,
            'broadcast': entity.broadcast if hasattr(entity, 'broadcast') else None,
            'verified': entity.verified if hasattr(entity, 'verified') else None,
            'restricted': entity.restricted if hasattr(entity, 'restricted') else None,
            'scam': entity.scam if hasattr(entity, 'scam') else None,
            'fake': entity.fake if hasattr(entity, 'fake') else None,
            'access_hash': str(entity.access_hash) if hasattr(entity, 'access_hash') and entity.access_hash else None,
            'date': entity.date.isoformat() if hasattr(entity, 'date') and entity.date else None,
            'about': None,
            'subscribers': None,
            'channel_url': channel_url
        }

        # Handle restriction reasons
        if hasattr(entity, 'restriction_reason') and entity.restriction_reason:
            channel_info['restriction_reason'] = [
                {
                    'platform': reason.platform,
                    'reason': reason.reason,
                    'text': reason.text
                }
                for reason in entity.restriction_reason
            ]
        else:
            channel_info['restriction_reason'] = None

        # Try to get subscriber count and about text
        try:
            info = await client(functions.channels.GetFullChannelRequest(channel=entity))
            if hasattr(info, 'full_chat'):
                if hasattr(info.full_chat, 'participants_count'):
                    channel_info['subscribers'] = info.full_chat.participants_count
                if hasattr(info.full_chat, 'about'):
                    channel_info['about'] = info.full_chat.about
        except Exception as e:
            print(f"  Warning: Could not get full info for {channel_url}: {e}")

        return channel_info

    except Exception as e:
        print(f"  Error getting info for {channel_url}: {e}")
        return {'channel_url': channel_url, 'error': str(e)}


async def process_channels(client, channels):
    """Process all channels and collect their information"""
    channels_data = []
    print(f"\nProcessing {len(channels)} channels...")
    print("=" * 60)

    for idx, channel_url in enumerate(channels, 1):
        print(f"[{idx}/{len(channels)}] Processing: {channel_url}")

        channel_info = await get_channel_info(client, channel_url)
        channels_data.append(channel_info)

        if 'error' in channel_info:
            print(f"  ✗ Error: {channel_info['error']}")
            if "A wait of" in channel_info['error']:
                return channels_data

        # Small delay to avoid rate limiting
        await asyncio.sleep(1)

    return channels_data


def load_completed_channels(filepath):
    """Load already collected channel information"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            telegram_channels = json.load(f)
    except FileNotFoundError:
        print(f"File {filepath} not found. Starting fresh.")
        return [], []

    done_channels_info = []
    for c in telegram_channels:
        if 'user_name' in c and c['user_name']:
            done_channels_info.append(c['user_name'])
            done_channels_info.append(c['channel_url'])
        elif 'channel_url' in c:
            done_channels_info.append(c['channel_url'])
        else:
            print(f"Warning: Channel entry missing channel_url: {c}")

    return telegram_channels, done_channels_info


def filter_results(results):
    """Separate clean results from errors; keep non-resolution errors"""
    result_clean = [r for r in results if 'error' not in r]
    result_error = [r for r in results if 'error' in r]

    # Keep errors that are not username-resolution failures (they may be recoverable)
    recoverable_errors = [
        r for r in result_error
        if 'caused by ResolveUsernameRequest' not in r['error']
    ]
    result_clean.extend(recoverable_errors)

    return result_clean, result_error


def save_results(results, output_path):
    """Save results to JSON file"""
    print(f"\n{'=' * 60}")
    print(f"Saving to {output_path}...")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"✓ Successfully saved {len(results)} channels")
    except Exception as e:
        print(f"✗ Error saving JSON: {e}")
        raise


def print_summary(results):
    """Print summary statistics"""
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)

    successful = len([c for c in results if 'error' not in c])
    failed = len([c for c in results if 'error' in c])
    with_username = len([c for c in results if c.get('username')])
    without_username = successful - with_username

    print(f"Total channels:                  {len(results)}")
    print(f"Successful:                      {successful}")
    print(f"Failed:                          {failed}")
    print(f"With username:                   {with_username}")
    print(f"Without username (title only):   {without_username}")


async def main():
    args = parse_args()

    # Load channel list from Excel
    channels_df = pd.read_excel(args.channels_file)

    # Load API credentials
    with open(args.config_path) as f:
        api_configs = json.load(f)

    if args.api_name not in api_configs:
        raise KeyError(f"API name '{args.api_name}' not found in {args.config_path}.")

    curr_id = api_configs[args.api_name]
    print(f"Using API ID: {curr_id['api_id']}")

    # Load previously completed channels
    telegram_channels, done_channels_info = load_completed_channels(args.input_json)

    # Initialize Telegram client
    client = TelegramClient(
        session=curr_id['session_name'],
        api_id=curr_id['api_id'],
        api_hash=curr_id['api_hash']
    )

    print("Connecting to Telegram...")
    try:
        await asyncio.wait_for(client.connect(), timeout=30)
    except asyncio.TimeoutError:
        print("✗ Connection timeout!")
        return

    if not await client.is_user_authorized():
        print("Please login...")
        await client.start()
    else:
        print("Already authorized")

    # Determine which channels still need to be processed
    channels = list(set(channels_df[args.handle_col]) - set(done_channels_info))
    print(f"{len(channels)} remaining channels to collect")

    # Process channels
    try:
        results = await process_channels(client, channels)
    finally:
        print("\nDisconnecting...")
        await client.disconnect()
        print("Done!")

    # Filter and merge results
    result_clean, result_error = filter_results(results)

    print(f"\nProcessed: {len(results)} channels")
    print(f"Clean results: {len(result_clean)}")
    print(f"Errors: {len(result_error)}")

    all_results = result_clean + telegram_channels

    # Save and summarize
    save_results(all_results, args.output_json)
    print_summary(results)


def parse_args():
    parser = argparse.ArgumentParser(description="Collect metadata for Telegram channels")
    parser.add_argument('--config_path', type=str, required=True,
                        help="Path to JSON file containing API credentials")
    parser.add_argument('--api_name', type=str, required=True,
                        help="Key name of the API credentials to use in the config file")
    parser.add_argument('--channels_file', type=str, required=True,
                        help="Path to Excel file containing channel handles")
    parser.add_argument('--handle_col', type=str, default='handle',
                        help="Column name for channel handles in the Excel file (default: 'handle')")
    parser.add_argument('--input_json', type=str, required=True,
                        help="Path to existing channel info JSON (for incremental collection)")
    parser.add_argument('--output_json', type=str, required=True,
                        help="Path to save the updated channel info JSON")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
