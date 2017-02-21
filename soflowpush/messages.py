import numbers

class ParseError(Exception):
    pass

class Message(object):
    TYPE = "BASE"

    def __init__(self, message_type=None, *args, **kwargs):
        self.message_type = message_type if message_type is not None else self.TYPE
        self.message_id = None

    @classmethod
    def parse(cls, dict_message, original=None):
        if not isinstance(dict_message, dict):
            raise ParseError
        for c in cls.__subclasses__():
            if c.TYPE == dict_message.get("message_type"):
                instance = c(**dict_message)
                instance.message_id = original.message_id if original is not None else None
                return instance
        raise ParseError


class SinglePush(Message):
    TYPE = "SINGLE"

    def __init__(self, user_id, message, target_timestamp, *args, **kwargs):


        super(SinglePush, self).__init__(*args, **kwargs)
        assert isinstance(message, str)
        self.message = message

        assert isinstance(user_id, str) or isinstance(user_id, int)
        self.user_id = user_id

        assert isinstance(target_timestamp, numbers.Number)
        self.target_timestamp = target_timestamp


class MultiPush(Message):
    TYPE = "MULTI"

    def __init__(self, user_ids, message, target_timestamp_start, target_timestamp_end, *args, **kwargs):
        super(MultiPush, self).__init__(*args, **kwargs)
        assert isinstance(target_timestamp_end, numbers.Number)
        self.target_timestamp_end = target_timestamp_end

        assert isinstance(target_timestamp_start, numbers.Number)
        self.target_timestamp_start = target_timestamp_start

        assert isinstance(message, str)
        self.message = message

        assert isinstance(user_ids, list)  # not just iterable. we need len
        self.user_ids = user_ids


class TestMessage(Message):
    TYPE = "TEST2"

    def __init__(self, value, *args, **kwargs):
        super(TestMessage, self).__init__(*args, **kwargs)
        self.value = value

