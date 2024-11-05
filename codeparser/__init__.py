import os
import re
from collections import defaultdict
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Dict, List, Tuple, TypeVar, Union

from tree_sitter import Node, Tree

from .provider_re import (
    RX_FUNC_MACRO_CALL,
    extract_function_name_regex,
    remove_comments_regex,
)
from .provider_tst import (
    capture_function_definitions,
    capture_function_name,
    function_scope,
    get_parser,
    traverse,
)


T = TypeVar("T")


def splitlines(s: T) -> List[T]:
    if isinstance(s, bytes):
        return re.split(rb"\r\n|\n", s)  # type: ignore
    return re.split(r"\r\n|\n", s)  # type: ignore


def __preprocess_code(s: bytes, lang: str) -> bytes:
    if lang in ("C", "C++"):
        # remove __attribute((xxx)) and __declspec(xxx)
        s = re.sub(
            rb"__attribute(__)?\s*\(\s*\([^)]*\)\s*\)|attribute_deprecated|__declspec\s*\(\s*\w+\s*\)",
            b"",
            s,
        )

        # remove __asm__ and __asm
        s = re.sub(rb"__asm__|__asm|__packed|__force|__user|coroutine_fn", b"", s)
    return s


def parse_ast(s: bytes, lang: str):
    parser = get_parser(lang)
    # s = __preprocess_code(s, lang)
    tree = parser.parse(s)
    return tree


@lru_cache(maxsize=128)
def get_comment_ranges(s: bytes, lang: str) -> List[Tuple[int, int]]:
    """Both end including range"""
    lines = splitlines(s)
    tree = parse_ast(s, lang)
    comments = []
    nodes_to_expand: List[Node] = [tree.root_node]
    while nodes_to_expand:
        node = nodes_to_expand.pop(0)
        if not node.children:
            if (
                "comment" in node.type
                and lines[node.start_point[0]][: node.start_point[1]].strip() == b""
            ):
                # start with whitespace
                comments.append((node.start_point[0], node.end_point[0]))
        nodes_to_expand = node.children + nodes_to_expand
    return comments


def _tokenize(n: Node, lang: str) -> List[bytes]:
    return [
        node.text for node in traverse(n, ret=lambda x: not x.children and bool(x.text))
    ]


def tokenize(s: bytes, lang: str) -> List[bytes]:
    tree = parse_ast(s, lang)
    return _tokenize(tree.root_node, lang)


def remove_comments_ast(code: bytes, lang: str) -> bytes:
    tree = parse_ast(code, lang)
    comments = []
    nodes_to_expand: List[Node] = [tree.root_node]
    while nodes_to_expand:
        node = nodes_to_expand.pop(0)
        if (
            node.type in ("comment", "line_comment", "block_comment")
            and node.start_byte < node.end_byte
        ):
            comments.append((node.start_byte, node.end_byte))
        nodes_to_expand = node.children + nodes_to_expand
    okcode = b""
    last = 0
    for start, end in comments:
        okcode += code[last:start]
        okcode += b"\n" * code[start:end].count(b"\n")
        last = end
    return okcode + code[last:]


def normalization(code: str) -> str:
    # replace tab with 4 spaces
    code = code.replace("\t", "    ")

    # strip trailing spaces
    code1 = ""
    lines = splitlines(code)
    for line in lines:
        code1 += line.rstrip() + "\n"

    return code1


def remove_comments(code: str, lang: str, timeout=None) -> str:
    if lang in {"C", "C++", "Java"}:
        return remove_comments_regex(code)

    func = remove_comments_ast
    if timeout:
        import timeout_decorator

        func = timeout_decorator.timeout(timeout)(func)

    return func(code.encode(), lang).decode()


def treeify(n: Node) -> Dict:
    this = {}
    this["type"] = n.type
    this["start_byte"] = n.start_byte
    this["end_byte"] = n.end_byte
    this["sexp"] = n.sexp()
    this["children"] = [treeify(c) for c in n.children]
    return this


class Func:
    def __init__(
        self,
        lang: str,  # the language of the function
        ctx_code: bytes,  # the parsing context
        idx: int,  # func index in file, count from 0
        node: Node,  # the node in AST
        meta={},  # extra meta info
    ):
        self.lang = lang
        self.ctx_code = ctx_code
        self.idx = idx
        self.node = node
        self.meta = meta

    def __repr__(self):
        # encode to one line
        sample_code = self.code[:100].replace("\n", "\\n").replace("\r", "\\r")
        return f'Func("{sample_code})"'

    def __eq__(self, x: object) -> bool:
        if isinstance(x, (Func, str)):
            func1 = self.code
            func2 = x.code if isinstance(x, Func) else x
            func1 = remove_comments_regex(func1)
            func2 = remove_comments_regex(func2)
            func1 = re.sub(r"\s+", "", func1)
            func2 = re.sub(r"\s+", "", func2)
            return func1 == func2
        return super().__eq__(x)

    @property
    def name(self):
        if self.lang == "C++":
            res = extract_ast_function_name(self.node, self.lang)
            if res:
                return res
        return extract_function_name_regex(self.code)

    @property
    def scope(self):
        return function_scope(self.node, self.lang)

    @property
    def fullname(self):
        name = self.name
        if name is None:
            return None
        if self.lang == "C++":
            return "::".join(self.scope + [name])
        return name

    @property
    def start_line(self):
        return self.node.start_point[0]

    @property
    def end_line(self):
        return self.node.end_point[0]

    @property
    def start_byte(self):
        return self.node.start_byte

    @property
    def end_byte(self):
        return self.node.end_byte

    @property
    def line_range(self):
        return range(self.start_line, self.end_line + 1)

    @property
    def code_bytes(self):
        return self.node.text

    @property
    def code(self):
        return self.code_bytes.decode(errors="ignore")

    @property
    def code_lines(self) -> List[str]:
        return splitlines(self.code)

    @staticmethod
    def similarity2(lhs: "Func | str", rhs: "Func | str"):
        if isinstance(lhs, Func):
            lhs = lhs.code
        if isinstance(rhs, Func):
            rhs = rhs.code
        return SequenceMatcher(None, lhs, rhs).ratio()

    def similarity(self, rhs: "Func | str"):
        return Func.similarity2(self, rhs)

    def stmt_map(self):
        return get_stmt_map(self.node)


EmptyFunc = object()


def is_func_macro(func_name: str):
    # like `SOME_FUNC_MACRO ( real_func_name )`
    pass


def get_stmt_map(node: Node):
    def is_leaf(n):
        return "statement" in n.type and n.type != "compound_statement"

    stmt_map = defaultdict(set)
    for stmt_node in traverse(node, descend=None, ret=is_leaf):
        for line in range(stmt_node.start_point[0], stmt_node.end_point[0] + 1):
            stmt_map[line].add(stmt_node)

    return stmt_map


def extract_ast_function_name(node: Node, lang: str):
    func_name = capture_function_name(node, lang)
    if not func_name:
        return None
    func_name = func_name.text.decode()
    if RX_FUNC_MACRO_CALL.search(func_name):
        return None
    return func_name


def extract_ast_functions(
    tree: Tree, lang: str, keep_error_node=True, **kwargs
) -> List[Func]:
    captures = capture_function_definitions(tree.root_node, lang)

    functions = []
    func_seq = 0

    for node in captures:
        if node.has_error and not keep_error_node:
            continue
        functions.append(
            Func(
                lang=lang,
                ctx_code=tree.root_node.text,
                idx=func_seq,
                node=node,
                meta=kwargs,
            )
        )
        func_seq += 1

    return functions


def extract_functions(
    src: Union[str, bytes, Tree],
    lang: str,
    timeout=None,
    _remove_comments=False,
    **kwargs,
) -> List[Func]:
    """
    Get the functions of a piece of source code
    :param source: the source code
    :return: a list of functions
    """
    if isinstance(src, str):
        if _remove_comments:
            src = remove_comments(src, lang, timeout=timeout)
        src = src.encode("utf-8", errors="ignore")

    if isinstance(src, bytes):
        src = parse_ast(src, lang)

    assert isinstance(src, Tree)

    _extract = extract_ast_functions
    if timeout:
        import timeout_decorator

        _extract = timeout_decorator.timeout(timeout)(_extract)

    return _extract(src, lang, **kwargs)  # type: ignore


def _extract_functions_declarations(root: Node, lang: str) -> List[Func]:
    groups = capture_function_definitions(root, lang)

    functions = []
    func_seq = 0

    for node in groups:
        functions.append(
            Func(
                lang=lang,
                ctx_code=root.text,
                idx=func_seq,
                node=node,
            )
        )
        func_seq += 1

    return functions


def extract_function_declarations(code: bytes, lang: str) -> List[Func]:
    tree = parse_ast(code, lang)

    return _extract_functions_declarations(tree.root_node, lang)


def is_decl_lvar(node: Node):
    while node.parent and node.parent.child_by_field_name("declarator") == node:
        node = node.parent
    return node.type == "declaration"


def is_decl_fparam(node: Node):
    while node.parent and node.parent.child_by_field_name("declarator") == node:
        node = node.parent
    return node.type == "parameter_declaration"


# abstract options
ABST_WITH_NUM = 1
ABST_AS_TYPE = 2
ABST_NON_SYS = 4

SYS_FUNC = set()
DIR = os.path.dirname(os.path.abspath(__file__))
with open(DIR + "/sys_func.txt", "rb") as f:
    SYS_FUNC.update(f.read().splitlines())
with open(DIR + "/std_func.txt", "rb") as f:
    SYS_FUNC.update(f.read().splitlines())


def abstract_func_clike(
    func: Union[bytes, Node],
    lang: str,
    abstract_fname: int = ABST_AS_TYPE,
    abstract_lvar: int = ABST_WITH_NUM,
    abstract_fparam: int = ABST_WITH_NUM,
    abstract_label: int = ABST_AS_TYPE,
    abstract_gsym: int = ABST_WITH_NUM,
    abstract_field: int = False,
    abstract_type: int = False,
    abstract_literal: int = False,
    abstract_func_call: int = False,
):
    if isinstance(func, bytes):
        tree = parse_ast(func, lang)
        func = tree.root_node

    assert isinstance(func, Node)
    output = []

    # states
    lvar_map = {}
    symbol_map = {}
    label_map = {}
    field_map = {}
    vtype_map = {}

    counts = defaultdict(int)

    def alloc_number(elm_type):
        res = f"{elm_type}{counts[elm_type]}"
        counts[elm_type] += 1
        return res

    def is_leaf(n):
        return not n.children or n.type in [
            "concatenated_string",
            "string_literal",
            "char_literal",
            "number_literal",
            "sized_type_specifier",
        ]

    def is_inner(n):
        return not is_leaf(n)

    for node in traverse(func, descend=is_inner, ret=is_leaf):
        if (
            node.parent.type == "function_declarator"
            and node == node.parent.child_by_field_name("declarator")
        ):
            # int "ns::foo"(int a, int b) {
            if not abstract_fname:
                output.append(node.text)
                continue
            output.append(b"FNAME")

        elif node.type == "identifier" and is_decl_fparam(node):
            # int foo(int "a", int b)
            if not abstract_fparam:
                output.append(node.text)
                continue
            elif abstract_fparam == ABST_WITH_NUM:
                lvar_map[node.text] = alloc_number("FPARAM").encode()
                output.append(lvar_map[node.text])
            else:
                lvar_map[node.text] = b"FPARAM"
                output.append(lvar_map[node.text])

        elif node.type == "identifier" and is_decl_lvar(node):
            # int "x"
            if not abstract_lvar:
                output.append(node.text)
                continue
            elif abstract_lvar == ABST_WITH_NUM:
                lvar_map[node.text] = alloc_number("LVAR").encode()
                output.append(lvar_map[node.text])
            else:
                lvar_map[node.text] = b"LVAR"
                output.append(lvar_map[node.text])

        elif node.type == "field_identifier":
            if not abstract_field:
                output.append(node.text)
                continue
            elif abstract_lvar == ABST_WITH_NUM:
                if node.text not in field_map:
                    field_map[node.text] = alloc_number("FIELD").encode()
                output.append(field_map[node.text])
            else:
                field_map[node.text] = b"FIELD"
                output.append(field_map[node.text])

        elif (
            node.parent.type == "labeled_statement"
            and node.type == "statement_identifier"
        ):
            # "err":
            if not abstract_label:
                output.append(node.text)
                continue
            elif abstract_lvar == ABST_WITH_NUM:
                label_map[node.text] = alloc_number("LABEL").encode()
                output.append(label_map[node.text])
            else:
                output.append(b"LABEL")

        elif (
            node.parent.type == "goto_statement" and node.type == "statement_identifier"
        ):
            # goto "err";
            if not abstract_label:
                continue
            elif abstract_lvar == ABST_WITH_NUM:
                if node.text not in label_map:
                    label_map[node.text] = alloc_number("LABEL").encode()
                output.append(label_map[node.text])
            else:
                output.append(b"LABEL")

        elif node.type in ("sized_type_specifier", "primitive_type", "type_identifier"):
            # "int" x;
            if not abstract_type:
                output.append(node.text)
                continue
            elif abstract_type == ABST_WITH_NUM:
                if node.text not in vtype_map:
                    vtype_map[node.text] = alloc_number("VTYPE").encode()
                output.append(vtype_map[node.text])
            else:
                output.append(b"VTYPE")

        elif node.type in [
            "concatenated_string",
            "string_literal",
            "char_literal",
            "number_literal",
        ]:
            if not abstract_literal:
                output.append(node.text)
                continue
            output.append(
                b"STR"
                if node.type
                in ["concatenated_string", "string_literal", "char_literal"]
                else b"NUM"
            )

        elif node.parent.type == "call_expression" and node.type == "identifier":
            # res = "func"(1, 2, 3);
            if not abstract_func_call:
                output.append(node.text)
                continue

            if abstract_func_call & ABST_WITH_NUM:
                raise NotImplementedError

            if abstract_func_call & ABST_NON_SYS:
                # only abstract non-standard functions
                output.append(node.text if node.text in SYS_FUNC else b"FCALL")
            else:
                output.append(b"FCALL")

        elif node.type == "identifier":
            if node.text in lvar_map:
                output.append(lvar_map.get(node.text))
            elif abstract_gsym:
                # maybe a symbol defined in outer scope
                if abstract_gsym == ABST_WITH_NUM:
                    if node.text not in symbol_map:
                        symbol_map[node.text] = alloc_number("GSYM").encode()
                    output.append(symbol_map[node.text])
                else:
                    output.append(b"GSYM")
            else:
                output.append(node.text)

        elif "comment" in node.type or node.type in (
            "static",
            "const",
            "volatile",
            "inline",
            "extern",
            "register",
            "typedef",
        ):
            pass

        else:
            output.append(node.text)

    return output
