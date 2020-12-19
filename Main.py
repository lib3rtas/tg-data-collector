from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

from typing import List

import Constants as const
from process_group import get_groups_data

client = TelegramClient(
            'anon', 
            const.credentials['api_id'], 
            const.credentials['api_hash']
        )


async def main():
    hints = ["https://t.me/joinchat/VfhhtxOgLKILbKrKB4Y3TA"]
    await get_groups_data(client, hints)

with client:
    client.loop.run_until_complete(main())