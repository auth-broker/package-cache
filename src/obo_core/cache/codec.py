import orjson

EncodedT = bytes | bytearray | memoryview
DecodedT = str | int | float | bool | list | dict
EncodableT = EncodedT | DecodedT
DecodableT = EncodedT | DecodedT


def safe_encode(value: EncodableT) -> EncodedT:
    if isinstance(value, EncodedT):
        # already encoded
        return value

    return orjson.dumps(value)


def safe_decode(value: DecodableT) -> DecodedT:
    if isinstance(value, DecodedT):
        # already decoded
        return value

    return orjson.loads(value)
