#!/usr/bin/env python3
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

import Constants as const

client = TelegramClient(
            'anon', 
            const.credentials['api_id'], 
            const.credentials['api_hash']
        )


async def main():
    # log into telegram
    await client.start() # creates .session file for future usage

    #chat = await client.get_entity('https://t.me/a27362672727486747373728')
    chat = await client.get_entity('https://t.me/joinchat/VfhhtxOgLKILbKrKB4Y3TA')
    #chat = await client.get_entity('https://t.me/geekschannel')
    #chat = await client.get_entity('https://t.me/qanon_world')

    print(chat.stringify())
    #await get_all_messages(chat)
    #await get_all_participants(chat)

async def get_all_messages(chat):
    messages = await client.get_messages(chat, None, reverse=True)
    for message in messages:
        print(message.stringify())


async def get_all_participants(chat):
    users = await client.get_participants(chat)
    for user in users:
        print(user.stringify())
        #if user.username is not None:
        #    print(user.username)

with client:
    client.loop.run_until_complete(main())
