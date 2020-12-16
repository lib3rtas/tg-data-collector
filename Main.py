#!/usr/bin/env python3
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

import Constants as crd

client = TelegramClient(
            'anon', 
            crd.credentials['api_id'], 
            crd.credentials['api_hash']
        )


async def main():
    # log into telegram
    await client.start()

    chat = await client.get_entity('https://t.me/a27362672727486747373728')
    # chat = await client.get_entity('https://t.me/geekschannel')
    users = await client.get_participants(chat)
    for user in users:
        if user.username is not None:
            print(user.username)


with client:
    client.loop.run_until_complete(main())
