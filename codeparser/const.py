
CLIKE_RESERVED_KEYWORDS = (
    'auto', 'break', 'case', 'char', 'const', 'continue', 'default', 'do', 'double',
    'else', 'enum', 'extern', 'float', 'for', 'goto', 'if', 'int', 'long', 'register',
    'return', 'short', 'signed', 'sizeof', 'static', 'struct', 'switch', 'typedef',
    'union', 'unsigned', 'void', 'volatile', 'while', '_Alignas', '_Alignof', '_Atomic',
    '_Bool', '_Complex', '_Generic', '_Imaginary', '_Noreturn', '_Static_assert',
    '_Thread_local', 'inline', '__inline', '__inline__', '__attribute__', '__asm__',
)

CLIKE_BAD_FUNCNAME = CLIKE_RESERVED_KEYWORDS

CLIKE_BAD_TYPENAME = set(CLIKE_RESERVED_KEYWORDS) - {'auto', 'char', 'float', 'int', 'long', 'double', 'signed', 'unsigned', 'void'}
