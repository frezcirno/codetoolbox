import re
from typing import Optional

RX_FUNC_CALL = re.compile(r"(\w+)\s*\(")

# FUNC_MACROS = (
#     'PHP_FUNCTION', 'PHP_NAMED_FUNCTION', 'SPL_METHOD',
#     'PS_SERIALIZER_DECODE_FUNC', 'ZIPARCHIVE_METHOD',
#     'FUNC', 'FUNCC', 'RENAME(xx)'
# )
# 'GTEST_LOCK_EXCLUDED_(&UnitTest::mutex_) {'
RX_FUNC_MACRO_CALL = re.compile(r'\b[A-Z_][A-Z0-9_]*\s*\(\s*(.+?)\s*\)')

# //-style comments
# /*-style comments */
# 'string'
# "string"
RX_CLIKE_COMMENT = re.compile(
    r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
    re.DOTALL | re.MULTILINE
)


def __replacer(match):
    s = match.group(0)
    if s.startswith('/'):
        return " "  # note: a space and not an empty string
    else:
        return s


def remove_comments_regex(code: str):
    """ remove c-like source code comment """
    return re.sub(RX_CLIKE_COMMENT, __replacer, code)


def extract_function_name_regex(text: str) -> Optional[str]:
    func_heading = text.split('{', 1)[0]

    if matches := RX_FUNC_MACRO_CALL.search(func_heading):
        return matches.group(0).strip()

    if func_name := RX_FUNC_CALL.search(func_heading):
        return func_name.group(1)

    return None
