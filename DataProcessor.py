class TelegramGroup:
    def __init__(self, participants, messages):
        self.participants = participants
        self.messages     = messages


class TelegramChannel:
    def __init__(self, raw_participants, raw_messages):
        self.raw_participants = raw_participants
        self.raw_messages     = raw_messages
        self.parent_node = []
        self.child_nodes = []
        self.edges       = []
        self.transform_raw_participants()
    
    def transform_raw_participants(self):
        print("Test")