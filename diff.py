import difflib
import re
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple, TypeVar, Union

from loguru import logger


class InvalidDiffLineError(Exception):
    def __init__(self, line):
        self.line = line

    def __str__(self):
        return "Invalid diff line: {}".format(self.line)


T = TypeVar("T")


def splitlines(s: T) -> List[T]:
    if isinstance(s, bytes):
        return re.split(rb"\r\n|\n", s)  # type: ignore
    return re.split(r"\r\n|\n", s)  # type: ignore


@dataclass
class SimpleDiff:
    del_lines: List[str]  # not include the prefix '-', and stripped
    add_lines: List[str]  # not include the prefix '+', and stripped
    meta: dict = field(default_factory=dict)

    def __bool__(self):
        return bool(self.add_lines) or bool(self.del_lines)

    def __contains__(self, x: "SimpleDiff"):
        """x是否包含在self的变更中"""
        if not x.del_lines and not x.add_lines:
            logger.warning("Empty diff is in any diffs")

        if (
            x.del_lines
            and all(dl in self.del_lines for dl in x.del_lines)
            and x.add_lines
            and all(al in self.add_lines for al in x.add_lines)
        ):
            return True

        return False

    def reverse(self):
        """反转diff"""
        rev_del_lines = [line for line in self.add_lines]
        rev_add_lines = [line for line in self.del_lines]

        return SimpleDiff(rev_del_lines, rev_add_lines, self.meta.copy())

    def __repr__(self):
        del_lines = "\n".join(self.del_lines)
        add_lines = "\n".join(self.add_lines)
        return f"""SimpleDiff(
    del_lines=```\n{del_lines}\n```,
    add_lines=```\n{add_lines}\n```,
    meta={self.meta}
)"""


@dataclass
class DiffLine:
    content: str
    old_ln: Optional[int] = None
    new_ln: Optional[int] = None

    def __bool__(self):
        return bool(self.stripped)

    def __str__(self):
        return self.prefix_token + self.content

    @property
    def is_add(self):
        return self.old_ln is None

    @property
    def is_del(self):
        return self.new_ln is None

    @property
    def is_context(self):
        return self.old_ln is not None and self.new_ln is not None

    @property
    def prefix_token(self):
        return "+" if self.is_add else "-" if self.is_del else " "

    @property
    def stripped(self):
        return self.content.strip()


@dataclass
class DiffHunk:
    lines: List[DiffLine]
    focal: str = ""
    old_file: Optional[str] = None
    new_file: Optional[str] = None
    git_diff: Any = None

    def __str__(self):
        lines = [str(line) for line in self.lines]
        return "\n".join(lines)

    def __iter__(self):
        return iter(self.lines)

    @property
    def old_first_ln(self) -> Optional[int]:
        return self.lines[0].old_ln

    @property
    def new_first_ln(self) -> Optional[int]:
        return self.lines[0].new_ln

    @property
    def old_last_ln(self) -> Optional[int]:
        return self.lines[-1].old_ln

    @property
    def new_last_ln(self) -> Optional[int]:
        return self.lines[-1].new_ln

    @property
    def n_line(self) -> int:
        return len(self.lines)

    @property
    def del_lines(self):
        return [line for line in self.lines if line.is_del]

    @property
    def n_del_line(self):
        return len(self.del_lines)

    @property
    def add_lines(self):
        return [line for line in self.lines if line.is_add]

    @property
    def n_add_line(self):
        return len(self.add_lines)

    @property
    def n_changed_line(self):
        return self.n_del_line + self.n_add_line

    @property
    def location(self) -> str:
        if self.old_file and self.new_file:
            file = f"{self.old_file}->{self.new_file}"
        elif self.old_file:
            file = self.old_file
        elif self.new_file:
            file = self.new_file
        else:
            assert False
        return f"{file}:{self.old_first_ln}-{self.old_last_ln} {self.new_first_ln}-{self.new_last_ln}"

    @property
    def first_old_ln(self) -> Optional[int]:
        for line in self.lines:
            if line.old_ln:
                return line.old_ln
        return None

    @property
    def last_old_ln(self) -> Optional[int]:
        for line in reversed(self.lines):
            if line.old_ln:
                return line.old_ln
        return None

    @property
    def content(self) -> List[str]:
        return [line.content for line in self.lines]

    @property
    def before_content(self) -> List[str]:
        return [line.content for line in self.lines if line.old_ln]

    @property
    def after_content(self) -> List[str]:
        return [line.content for line in self.lines if line.new_ln]

    def as_simple_diff(self):
        """
        Return a SimpleDiff object.
        Note: the lines in SimpleDiff are stripped!
        """
        del_lines = [line.content.strip() for line in self.del_lines]
        add_lines = [line.content.strip() for line in self.add_lines]
        return SimpleDiff(del_lines, add_lines)


def split_hunks(raw_diff: Union[str, List[str]]) -> List[List[str]]:
    if isinstance(raw_diff, str):
        raw_diff = raw_diff.strip().split("\n")
    raw_hunks = []
    for line in raw_diff:
        if line.startswith("@@"):
            raw_hunks.append([line])
        elif raw_hunks:
            raw_hunks[-1].append(line)
    return raw_hunks


def parse_hunk_header(line: str) -> Tuple[int, int, int, int, str]:
    """Parse a hunk header line and return the old and new line numbers and context."""
    regex = r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@ ?(.*)?$"
    m = re.match(regex, line)
    if not m:
        raise ValueError(f"Invalid diff header: {line}")
    return (
        int(m.group(1)),
        m.group(2) and int(m.group(2)) or 0,
        int(m.group(3)),
        m.group(4) and int(m.group(4)) or 0,
        m.group(5) or "",
    )


def parse_hunk_body(content_lines: List[str], old_ln: int, new_ln: int):
    """
    Parse hunk content like
    b'''-a
    +b
    c
    '''
    """
    lines = []
    for line in content_lines:
        if line.startswith("-"):
            lines.append(DiffLine(line[1:], old_ln))
            old_ln += 1
        elif line.startswith("+"):
            lines.append(DiffLine(line[1:], new_ln=new_ln))
            new_ln += 1
        elif line.startswith(" "):
            lines.append(DiffLine(line[1:], old_ln, new_ln))
            old_ln += 1
            new_ln += 1
        elif line == r"\ No newline at end of file":
            pass
        elif line != "":
            raise InvalidDiffLineError(line)
    return lines


def parse_hunk(
    raw_hunk: Union[str, List[str]], old_file=None, new_file=None
) -> DiffHunk:
    """
    Parse one hunk like
    b'''@@ -1,3 +1,3 @@
    -a
    +b
    c
    '''
    """
    if isinstance(raw_hunk, str):
        raw_hunk = raw_hunk.strip().split("\n")
    old_start_ln, _, new_start_ln, _, focal = parse_hunk_header(raw_hunk[0])
    lines = parse_hunk_body(raw_hunk[1:], old_start_ln, new_start_ln)
    return DiffHunk(lines, focal, old_file, new_file)


def parse_hunks(
    raw_hunk: Union[str, List[str]], old_file=None, new_file=None
) -> List[DiffHunk]:
    """Parse raw hunks like
    b'''@@ -1,3 +1,3 @@
    -a
    @@ -1,3 +1,3 @@
    +a'''
    """
    return [
        parse_hunk(raw_hunk, old_file, new_file) for raw_hunk in split_hunks(raw_hunk)
    ]


def extract_add_del_lines(raw_unidiff_like: Union[str, List[str]]):
    """
    diff: git diff的结果
    """
    add_lines = []
    del_lines = []

    if isinstance(raw_unidiff_like, str):
        raw_unidiff_like = splitlines(raw_unidiff_like)

    for line in raw_unidiff_like:
        if line.startswith("+") and not line.startswith("+++") and line[1:].strip():
            add_lines.append(line[1:].strip())
        elif line.startswith("-") and not line.startswith("---") and line[1:].strip():
            del_lines.append(line[1:].strip())

    return SimpleDiff(del_lines, add_lines)


def diff_code_raw(
    before: Union[str, List[str]],
    after: Union[str, List[str]],
    n_context: int = 3,
    **kwargs,
):
    if isinstance(before, str):
        before = splitlines(before)
    if isinstance(after, str):
        after = splitlines(after)
    return difflib.unified_diff(before, after, lineterm="", n=n_context, **kwargs)


def diff_code(
    before: Union[str, List[str]], after: Union[str, List[str]], n: int = 3, **kwargs
) -> DiffHunk:
    unidiff = list(diff_code_raw(before, after, n, **kwargs))

    raw_hunks = split_hunks(unidiff[2:])
    hunk_lines = []
    for raw_hunk in raw_hunks:
        hunk = parse_hunk(raw_hunk)
        hunk_lines.extend(hunk.lines)

    return DiffHunk(hunk_lines)
