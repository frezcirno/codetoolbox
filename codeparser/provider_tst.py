from collections import deque
from functools import lru_cache
from typing import Callable, List, Optional

from tree_sitter import Language, Node, Parser
import tree_sitter_c
import tree_sitter_cpp
import tree_sitter_java

from .const import CLIKE_BAD_FUNCNAME


class ParseLangNotSupportError(Exception):
    def __init__(self, lang):
        self.lang = lang

    def __str__(self):
        return "Language {} is not supported.".format(self.lang)


@lru_cache()
def get_language(lang: str) -> Language:
    lang_mapping = {
        "C": tree_sitter_c.language(),
        "C++": tree_sitter_cpp.language(),
        "Java": tree_sitter_java.language(),
        # "C#": "c_sharp",
        # "PHP": "php",
        # "Python": "python",
        # "Ruby": "ruby",
        # "Rust": "rust",
        # "Go": "go",
        # "JavaScript": "javascript",
    }
    return Language(lang_mapping[lang])


@lru_cache()
def get_parser(lang: str) -> Parser:
    parser = Parser()
    parser.language = get_language(lang)
    return parser


@lru_cache()
def get_query(lang: str, query: str):
    return get_language(lang).query(query)


def wrap_get_query(query: str):
    def inner(lang: str):
        return get_query(lang, query)

    return inner


def captures(lang: str, query: str, node: Node):
    return get_query(lang, query).captures(node)


include_path_query = wrap_get_query(
    """(translation_unit (preproc_include path: _ @include_path)) """
)
function_definition_query = wrap_get_query(
    """(translation_unit (function_definition) @function) """
)
function_declarator_query = wrap_get_query(
    """(function_declarator declarator: (identifier) @function_name) """
)
function_parameters_type_query = wrap_get_query(
    """(function_declarator parameters: (parameter_list (parameter_declaration type: _ @type)))"""
)
function_ret_query = wrap_get_query("""(function_definition type: _ @ret_type)""")
class_method_query = wrap_get_query(
    """(class_declaration body: (class_body (method_declaration) @method)) """
)
method_declaration_query = wrap_get_query(
    """(method_declaration name: (identifier) @method_name) """
)
method_parameters_query = wrap_get_query(
    """(method_declaration parameters: (formal_parameters (formal_parameter type: _ @type name: _ @name))) """
)
method_ret_query = wrap_get_query("""(method_declaration type: _ @ret_type)""")
method_invocation_query = wrap_get_query(
    """(method_invocation name: (identifier) @callee_name arguments: (argument_list _ @types)) """
)
import_header_query = wrap_get_query("""(import_declaration) @import_header """)
class_field_query = wrap_get_query(
    """(field_declaration declarator: (variable_declarator name: _ @class_field)) """
)

CLASS_QUERY = {
    "C++": "(class_specifier) @class (struct_specifier) @class",
}

CLASS_NAME_QUERY = {
    "C++": "(class_specifier name: _ @name) @class (struct_specifier name: _ @name) @class",
}

FUNCTION_DECL_CAPTURES = {
    "C": "(function_declarator declarator: _ @ret)",
    "C++": """(function_declarator declarator: _ @ret)
              (function_definition declarator: (operator_cast) @ret)
              (function_definition declarator: (qualified_identifier name: (operator_cast) @ret))""",
    "Java": "(method_declaration name: _ @ret) (constructor_declaration name: _ @ret)",
}

FUNCTION_DEFINITION_CAPTURES = {
    "C": "(function_definition) @ret",
    "C++": "(function_definition) @ret",
    "Java": "(method_declaration) @ret (constructor_declaration) @ret",
}


def capture_class(node: Node, lang: str):
    if not (capture := CLASS_QUERY.get(lang)):
        raise ParseLangNotSupportError(lang)

    nodes = [node for node, _ in captures(lang, capture, node)]

    return nodes


def capture_class_name(node: Node, lang: str):
    if not (capture := CLASS_NAME_QUERY.get(lang)):
        raise ParseLangNotSupportError(lang)

    nodes = [node for node, _ in captures(lang, capture, node)]

    return nodes


def capture_function_definitions(n: Node, lang: str):
    if not (capture := FUNCTION_DEFINITION_CAPTURES.get(lang)):
        raise ParseLangNotSupportError(lang)

    node = [node for node, _ in captures(lang, capture, n)]

    # remove node that inside another nodes
    node = [
        n
        for n in node
        if not any(
            n.start_byte > x.start_byte and n.end_byte < x.end_byte for x in node
        )
    ]

    # validate
    node = [
        n
        for n in node
        if not n.text.startswith(
            (b"else", b"if", b"for", b"while", b"do", b"switch", b"case", b"default")
        )
    ]

    return node


def capture_function_name(node: Node, lang: str):
    if not (capture := FUNCTION_DECL_CAPTURES.get(lang)):
        raise ParseLangNotSupportError(lang)

    res = captures(lang, capture, node)
    if not res:
        return None
    res = res[0][0]

    # validate
    if res.text.decode() in CLIKE_BAD_FUNCNAME:
        return None

    # C++ qualified identifier, e.g. Class::method
    if lang == "C++":
        while res and res.type == "qualified_identifier":
            res = res.child_by_field_name("name")

    return res


def ancester(node: Node) -> List[Node]:
    """Return [node.parent, node.parent.parent, ...]"""
    res = []
    p = node.parent
    while p:
        res.append(p)
        p = p.parent
    return res


def function_scope_cpp(fnode: Node, lang: str):
    """Return function scope (A::B::C::D::foo -> ["A", "B", "C", "D"])"""
    res = []

    name_node = capture_function_name(fnode, lang)
    if not name_node:
        name_node = fnode

    for p in ancester(name_node):
        if p.type == "namespace_definition":
            namespace_name = p.child_by_field_name("name")
            if namespace_name:
                res.extend(namespace_name.text.decode().split("::"))
            else:
                res.append("")
        elif p.type == "class_specifier" or p.type == "struct_specifier":
            res.append(p.child_by_field_name("name").text.decode())
        elif p.type == "qualified_identifier":
            res.append(p.child_by_field_name("scope").text.decode())

    res.reverse()
    return res


def function_scope_java(node: Node, lang: str):
    """Return function scope (A::B::C::D::foo -> ["A", "B", "C", "D"])"""
    res = []

    for p in ancester(node):
        if p.type == "class_declaration":
            res.append(p.child_by_field_name("name").text.decode())

    res.reverse()
    return res


def function_scope(node: Node, lang: str):
    if lang == "C":
        return []
    elif lang == "C++":
        return function_scope_cpp(node, lang)
    elif lang == "Java":
        return function_scope_java(node, lang)

    raise ParseLangNotSupportError(lang)


def traverse(
    node: Node,
    descend: Optional[Callable[[Node], bool]] = None,
    ret: Optional[Callable[[Node], bool]] = None,
):
    """Traverse the tree depth-first, yielding each node"""
    cursor = node.walk()
    while True:
        if not ret or ret(cursor.node):
            yield cursor.node
        if not descend or descend(cursor.node):
            if cursor.goto_first_child():
                continue
        if cursor.goto_next_sibling():
            continue
        while True:
            if not cursor.goto_parent():
                return
            if cursor.goto_next_sibling():
                break


def traverse_bfs(node: Node, descend: Optional[Callable[[Node], bool]] = None):
    """Traverse the tree breadth-first, yielding each node"""
    queue = deque()
    queue.append(node)
    while queue:
        node = queue.popleft()
        yield node
        if not descend or descend(node):
            queue.extend(node.children)
