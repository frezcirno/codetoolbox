import chardet

def detect_encoding(data: bytes):
    """
    Detect the encoding of a text file, i.e. utf-8, us-ascii, etc.
    :param data: The data to be analyzed.
    """
    return chardet.detect(data)["encoding"]


def decode(data: bytes):
    """
    Decode the data using the specified encoding.
    :param data: The data to be decoded.
    :param encoding: The encoding to be used.
    """
    for encoding in ["utf-8", "ascii"]:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            pass

    detected_encoding = detect_encoding(data)
    return data.decode(detected_encoding)  # type: ignore
