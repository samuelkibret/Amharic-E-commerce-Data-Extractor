from telethon import TelegramClient
import asyncio
import yaml
import os
import pandas as pd
from datetime import datetime

# Load credentials and channels from config.yaml
config_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

api_id = config['telegram']['api_id']
api_hash = config['telegram']['api_hash']
channels = config['channels']

client = TelegramClient('amharic_session', api_id, api_hash)

# List to collect messages
all_messages = []

async def fetch_messages():
    await client.start()
    print("Client connected...")

    for channel in channels:
        try:
            print(f"\n📥 Fetching from channel: {channel}")
            entity = await client.get_entity(channel)

            async for message in client.iter_messages(entity, limit=30):
                if message.text:
                    all_messages.append({
                        "channel": channel,
                        "date": message.date.strftime('%Y-%m-%d %H:%M:%S'),
                        "sender_id": message.sender_id,
                        "message": message.text.replace('\n', ' ').strip()
                    })
        except Exception as e:
            print(f"⚠️ Failed to fetch from {channel}: {e}")

    await client.disconnect()
    print("Disconnected.")

    # Save to CSV
    df = pd.DataFrame(all_messages)
    os.makedirs('data/raw', exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f'data/raw/telegram_messages_{timestamp}.csv'
    df.to_csv(csv_path, index=False)
    print(f"\n✅ Data saved to: {csv_path}")

if __name__ == '__main__':
    asyncio.run(fetch_messages())
