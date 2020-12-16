#!/usr/bin/env python3
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

import Credentials as crd

client = TelegramClient('anon',crd.api_id, crd.api_hash)
async def main():
    # log into telegram
    await client.start()

    chats = []
    last_date = None
    chunk_size = 200
    groups=[]
    result = await client(
                GetDialogsRequest(
                    offset_date=last_date,
                    offset_id=0,
                    offset_peer=InputPeerEmpty(),
                    limit=chunk_size,
                    hash = 0
                )
            )
    chats.extend(result.chats)
    
    for chat in chats:
        try:
            if chat.megagroup== True:
                groups.append(chat)
                print("Megagroup")
                print(chat.stringify())
        except:
            continue
    print(groups)

with client:
    client.loop.run_until_complete(main())