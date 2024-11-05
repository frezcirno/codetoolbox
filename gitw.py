import os
import shutil
import tempfile
from typing import Dict, List, Optional, Tuple, Union

import git
import joblib
from git.repo import Repo as GitRepo
from loguru import logger

from . import linguist, codeparser


# where to save the cloned repos
class config:
    REPOSITORY_BASE = "/data/repos/"
    REPO_CACHE_BASE = "/data/repo_cache/"
    GIT_BASE_URL = "git@github.com:"

    os.makedirs(REPOSITORY_BASE, exist_ok=True)
    os.makedirs(REPO_CACHE_BASE, exist_ok=True)


class GitError(Exception):
    pass


class BadRepoError(GitError):
    def __init__(self, e):
        self.e = e

    def __str__(self):
        return "Bad repo: {}".format(self.e)


class CloneError(GitError):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "Clone error: {}".format(self.url)


class NoRepoError(GitError):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return "No such repo: {}".format(self.repo)


class NoCommitError(GitError):
    def __init__(self, commit, project=None):
        self.commit = commit
        self.project = project

    def __str__(self):
        return "No such commit: {} in project {}".format(self.commit, self.project)


class NoFileError(GitError):
    def __init__(self, file):
        self.file = file

    def __str__(self):
        return "No such file: {}".format(self.file)


class NoMethodError(GitError):
    def __init__(self, method, all_methods):
        self.method = method
        self.all_methods = all_methods

    def __str__(self):
        all_methods_str = ", ".join(self.all_methods)
        return f"No such method: {self.method} in {all_methods_str}"


class ParseLangNotSupportError(GitError):
    def __init__(self, lang):
        self.lang = lang

    def __str__(self):
        return "Language {} is not supported.".format(self.lang)


# disable git credential prompts
os.environ.setdefault("GIT_TERMINAL_PROMPT", "0")

cacher = joblib.Memory(config.REPO_CACHE_BASE, verbose=0)


FAMOUS_PROJECT_ALIAS = {
    "linux": "torvalds/linux",
    "php": "php/php-src",
    "php-src": "php/php-src",
    "php+php-src": "php/php-src",
    "chromium+chromium_src": "chromium/chromium",
    "codeaurora+la_platform_vendor_qcom-opensource_wlan_qcacld-2.0": "digi-embedded/qcacld-2.0",
}


class TempRepo:
    def __init__(self, src_path, version):
        self.src_path = src_path
        self.version = version

    def __enter__(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        target_path = os.path.join(self.tmpdir.name, "repo")
        shutil.copytree(self.src_path, target_path, symlinks=True)
        Repo(target_path).checkout(self.version)
        return target_path

    def __exit__(self, exc_type, exc_value, traceback):
        self.tmpdir.cleanup()


class Repo:
    def __init__(self, repo: Union[str, GitRepo]):
        if isinstance(repo, str):
            repo = GitRepo(repo)
        self.repo = repo
        assert not repo.bare, "Bare repos are not supported"
        self.project_id = os.path.basename(repo.working_tree_dir)  # type: ignore
        self.tempd = None

    def __del__(self):
        # self.repo.git.clear_cache()
        if hasattr(self, "tempd") and self.tempd:
            shutil.rmtree(self.tempd)

    def __str__(self):
        return f"Repo({self.project_id} at {self.working_tree_dir})"

    @staticmethod
    def get_storage_path(project_id: str):
        repo_path = os.path.join(config.REPOSITORY_BASE, project_id.lower())

        # repo_path must be $REPOSITORY_BASE/x
        if not os.path.abspath(repo_path).startswith(config.REPOSITORY_BASE, 0, -2):
            raise BadRepoError(project_id)

        return repo_path

    @staticmethod
    def get_git_url(project_id: str):
        if project_id in FAMOUS_PROJECT_ALIAS:
            return f"{config.GIT_BASE_URL}{FAMOUS_PROJECT_ALIAS[project_id]}.git"
        if "+" in project_id:
            group, project = project_id.split("+", 1)
            return f"{config.GIT_BASE_URL}{group}/{project}.git"
        else:
            return f"{config.GIT_BASE_URL}{project_id}/{project_id}.git"

    @staticmethod
    def exists(project_id: str):
        repo_path = Repo.get_storage_path(project_id)
        return os.path.exists(repo_path)

    @staticmethod
    def clone_temp(git_url):
        tempd = tempfile.mkdtemp()
        repo = Repo.clone_from(git_url, tempd)
        repo.tempd = tempd
        return repo

    @staticmethod
    def open_project(project_id, clone=False, temp=False):
        repo_path = Repo.get_storage_path(project_id)
        if os.path.exists(repo_path):
            return Repo.open_path(repo_path)
        if not clone:
            raise NoRepoError(project_id)
        git_url = Repo.get_git_url(project_id)
        if temp:
            return Repo.clone_temp(git_url)
        repo_path = Repo.get_storage_path(project_id)
        return Repo.clone_from(git_url, repo_path)

    @staticmethod
    def open_path(repo_path):
        try:
            repo = GitRepo(repo_path)
            assert not repo.bare, "Bare repos are not supported"
            return Repo(repo)

        except git.InvalidGitRepositoryError as e:
            raise BadRepoError(e)

        except git.NoSuchPathError:
            raise NoRepoError(repo_path)

    @staticmethod
    def clone_from(git_url: str, to_path: str, **kwargs):
        for trial in range(3):
            try:
                logger.info(f"Cloning {git_url} to {to_path} ... ({trial + 1}/3)")
                return Repo(GitRepo.clone_from(git_url, to_path, **kwargs))
            except git.GitCommandError as ex:
                # if trial < 2:
                #     continue
                logger.error(ex)
                raise CloneError(git_url) from ex
        assert False

    def delete(self):
        working_dir = self.working_tree_dir
        del self.repo
        shutil.rmtree(working_dir)

    @property
    def git(self):
        return self.repo.git

    @property
    def head_commit(self):
        return Commit(self.repo.head.commit)

    @property
    def bare(self):
        return self.repo.bare

    @property
    def main_branch(self):
        return self.repo.active_branch.name

    @property
    def working_tree_dir(self) -> str:
        # assert not self.bare
        return self.repo.working_tree_dir  # type: ignore

    @property
    def description(self):
        return self.repo.description

    @property
    def tags(self):
        for tag in self.repo.tags:
            try:
                if tag.commit:
                    yield tag.name
            except ValueError:
                continue

    @property
    def branches(self):
        return self.repo.branches

    @property
    def remote_branches(self):
        return self.repo.remote().refs

    def commit(self, rev):
        try:
            return Commit(self.repo.commit(rev))
        except (git.BadName, ValueError):
            raise NoCommitError(rev, self.project_id)

    def checkout(self, rev, clean=True):
        if clean:
            try:
                self.repo.git.restore("--staged", ".")
                self.repo.git.restore(".")
                self.repo.git.clean("-dfx")
            except git.GitCommandError:
                pass
        self.repo.git.checkout(rev)
        assert self.commit(rev).id == self.head_commit.id, "checkout failed"

    def open(self, file_path):
        return open(os.path.join(self.working_tree_dir, file_path))

    @property
    def commits(self):
        return [Commit(c) for c in self.repo.iter_commits("--all")]

    @staticmethod
    @cacher.cache(ignore=["repo"])
    def _gen_commit_child_mapping(project_id, repo: "Repo") -> Dict[str, List[str]]:
        logger.info(f"Generating commit child mapping for {project_id}")
        commit_child_mapping: Dict[str, List[str]] = {}
        for c in repo.commits:
            for p in c.parents:
                commit_child_mapping.setdefault(p.hexsha, []).append(c.hexsha)
        return commit_child_mapping

    @property
    def commit_child_mapping(self) -> Dict[str, List[str]]:
        return Repo._gen_commit_child_mapping(self.project_id, self)

    @property
    def files(self) -> List[str]:
        ret = []
        for p, _, fs in os.walk(self.working_tree_dir):
            if p == ".git":
                continue
            for f in fs:
                ret.append(os.path.join(p, f))
        return ret

    @property
    def n_commits(self):
        return len(list(self.repo.iter_commits("--all")))


class Commit:
    def __init__(self, gitcommit: git.Commit):
        self.gitcommit = gitcommit
        self.repo = Repo(self.gitcommit.repo)

    def __hash__(self) -> int:
        return hash(self.hexsha)

    @property
    def git(self):
        return self.repo.git

    @property
    def hexsha(self):
        return self.gitcommit.hexsha

    @property
    def id(self):
        return self.gitcommit.hexsha

    @property
    def author(self):
        return self.gitcommit.author

    @property
    def author_email(self):
        return self.gitcommit.author.email

    @property
    def author_date(self):
        return self.gitcommit.authored_datetime

    @property
    def committer(self):
        return self.gitcommit.committer

    @property
    def committer_email(self):
        return self.gitcommit.committer.email

    @property
    def committer_date(self):
        return self.gitcommit.committed_datetime

    @property
    def message(self):
        return self.gitcommit.message

    @property
    def short_message(self):
        return self.gitcommit.message.split("\n", 1)[0]  # type: ignore

    @property
    def parents(self):
        return [Commit(c) for c in self.gitcommit.parents]

    @property
    def is_merge(self):
        return len(self.gitcommit.parents) > 1

    @property
    def is_root(self):
        return len(self.gitcommit.parents) == 0

    @property
    def tags(self):
        """
        git tag --points-at commit
        """
        return self.git.tag("--points-at", self.hexsha).split("\n")

    @property
    def tags_contain(self):
        """
        git tag --contains commit
        """
        return self.git.tag("--contains", self.hexsha).split("\n")

    @property
    def branchs_contain(self):
        """
        git branch --contains commit
        """
        return self.git.branch("--contains", self.hexsha).split("\n")

    @property
    def is_tagged(self):
        return len(self.tags) > 0

    def file_(self, file_path) -> str:
        return self.repo.git.show(f"{self.id}:{file_path}")

    @property
    def patch(self) -> str:
        return self.git.show(self.hexsha)

    @property
    def diff(self) -> "CommitDiff":
        if not hasattr(self, "_diff"):
            self._diff = self.make_diff()
        return self._diff

    def make_diff(self, create_patch=False, **kwargs) -> "CommitDiff":
        # Git Notes:
        # -w --ignore-all-space: ignore all! whitespace changes
        # -b --ignore-space-change: ignore changes in amount of whitespace
        # -B --ignore-blank-lines: ignore changes whose lines are all blank
        # --ignore-space-at-eol: ignore changes in whitespace at EOL
        # --ignore-cr-at-eol: ignore carriage-return at EOL
        # -W --method-context:
        # kwargs = {}
        kwargs["ignore_space_change"] = True
        kwargs["ignore_blank_lines"] = True
        kwargs["ignore_space_at_eol"] = True

        if parents := self.parents:
            if len(parents) > 1:
                # It's a merge commit.
                # PyDriller return empty list when diffing a merge commit. We follow it.
                # https://stackoverflow.com/questions/40986518/how-to-git-show-the-diffs-for-a-merge-commit
                return CommitDiff(self, [])

            diff = parents[0].gitcommit.diff(
                self.gitcommit, create_patch=create_patch, **kwargs
            )
        else:
            diff = self.gitcommit.diff(
                git.NULL_TREE, create_patch=create_patch, **kwargs
            )
        return CommitDiff(self, diff)

    @property
    def children(self):
        return self.repo.commit_child_mapping.get(self.hexsha, [])

    @property
    def modified_files(self):
        return self.gitcommit.stats.files.keys()

    def before_file(self, file_path):
        if self.is_root:
            return File(self.repo, self, file_path, "")
        return self.parents[0].file(file_path)

    def file(self, file_path):
        try:
            contents = (
                self.gitcommit.tree[file_path]
                .data_stream.read()
                .decode("utf-8", errors="ignore")
            )
        except KeyError:
            raise NoFileError(f"{file_path} in {self.hexsha}")
        return File(self.repo, self, file_path, contents)

    @property
    def file_mapping(self):
        return self.diff.file_mapping

    @property
    def file_mapping_list(self):
        return self.diff.file_mapping_list

    @property
    def reverse_file_mapping(self):
        return self.diff.reverse_file_mapping

    @property
    def reverse_file_mapping_list(self):
        return self.diff.reverse_file_mapping_list

    def get_b_path(self, a_path):
        assert a_path
        return self.file_mapping.get(a_path)

    def get_a_path(self, b_path):
        assert b_path
        return self.reverse_file_mapping.get(b_path)


class File:
    def __init__(self, repo: Repo, commit: Commit, file_path: str, contents: str):
        self.repo = repo
        self.commit = commit
        self.file_path = file_path
        self.contents = contents
        self.stripped = False  # whether the comments are stripped

    @property
    def language(self):
        lang = linguist.detect_code_language(self.contents, self.file_path)
        assert lang, f"Unknown language for file: {self.file_path}"
        return lang

    def remove_comments(self):
        newf = File(
            self.repo,
            self.commit,
            self.file_path,
            codeparser.remove_comments(self.contents, self.language),
        )
        newf.stripped = True
        return newf

    def methods(self, **kwargs):
        if hasattr(self, "_methods"):
            return self._methods
        self._methods = codeparser.extract_functions(
            self.contents, self.language, **kwargs
        )
        return self._methods

    def method(self, fullname, **kwargs):
        all_methods = []
        for m in self.methods(**kwargs):
            if m.fullname == fullname:
                return m
            all_methods.append(m.fullname)
        raise NoMethodError(fullname, all_methods)


class CommitDiff:
    def __init__(self, parent: Commit, di: Union[git.DiffIndex, List]):
        self.commit = parent
        self.di = di

    def __iter__(self):
        for d in self.di:
            yield FileChange(self, d)

    @property
    def hunks(self):
        return [d.diff for d in self.di]

    @property
    def modified_files(self):
        return [d.a_path for d in self]

    @property
    def file_mapping_list(self):
        return [(d.a_path, d.b_path) for d in self]

    @property
    def file_mapping(self):
        return {d.a_path: d.b_path for d in self}

    @property
    def reverse_file_mapping_list(self):
        return [(d.b_path, d.a_path) for d in self]

    @property
    def reverse_file_mapping(self):
        return {d.b_path: d.a_path for d in self}

    def a_methods(self, ignore_unsupported_lang, **kwargs):
        all_methods = []
        for d in self:
            methods = d.a_methods(
                commit_id=self.commit.id,
                ignore_unsupported_lang=ignore_unsupported_lang,
                **kwargs,
            )
            all_methods.extend(methods)
        return all_methods

    def b_methods(self, ignore_unsupported_lang, **kwargs):
        all_methods = []
        for d in self:
            methods = d.b_methods(
                commit_id=self.commit.id,
                ignore_unsupported_lang=ignore_unsupported_lang,
                **kwargs,
            )
            all_methods.extend(methods)
        return all_methods

    def method_mapping_list(
        self, **kwargs
    ) -> List[Tuple[codeparser.Func, codeparser.Func]]:
        # match
        a_methods = self.a_methods(ignore_unsupported_lang=True, **kwargs)
        b_methods = self.b_methods(ignore_unsupported_lang=True, **kwargs)
        res = []
        for a in a_methods:
            for b in b_methods:
                if a.fullname == b.fullname:
                    res.append((a, b))
                    b_methods.remove(b)
                    break
            else:
                res.append((a, None))

        # delete
        for b in b_methods:
            res.append((None, b))

        # only keep changed methods
        res = [(a, b) for a, b in res if a != b]

        return res


class FileChange:
    def __init__(self, parent: CommitDiff, d: git.Diff):
        self.cd = parent
        self.d = d

    def __repr__(self):
        return (
            f"<FileChange {self.cd.commit.hexsha[:8]} {self.a_path} -> {self.b_path}>"
        )

    @property
    def a_path(self):
        if self.d.new_file:
            return ""
        return self.d.a_path

    @property
    def b_path(self):
        if self.d.deleted_file:
            return ""
        return self.d.b_path

    @property
    def a_language(self):
        if not hasattr(self, "_a_language"):
            self._a_language = linguist.detect_language(self.a_path)
        return self._a_language

    @property
    def b_language(self):
        if not hasattr(self, "_b_language"):
            self._b_language = linguist.detect_language(self.b_path)
        return self._b_language

    @property
    def language(self):
        return self.b_language or self.a_language

    @property
    def is_rename_file(self):
        return self.d.renamed_file

    @property
    def is_new_file(self):
        return self.d.new_file

    @property
    def is_delete_file(self):
        return self.d.deleted_file

    @property
    def is_copy_file(self):
        return self.d.copied_file

    @staticmethod
    def _read_blob(blob: Optional[git.IndexObject]):
        # maybe uninitialized submodule
        return blob.data_stream.read() if blob is not None else None

    @staticmethod
    def _decode(name: str, content: bytes) -> Optional[str]:
        try:
            return content.decode("utf-8", "ignore")
        except (AttributeError, ValueError):
            logger.debug("Could not load the content for file %s", name)
            return None

    @property
    def a_blob(self) -> Optional[bytes]:
        return self._read_blob(self.d.a_blob)

    @property
    def a_contents(self) -> Optional[str]:
        a_blob = self.a_blob
        if a_blob is None:
            return None
        return self._decode(self.a_path, a_blob)

    @property
    def b_blob(self) -> Optional[bytes]:
        return self._read_blob(self.d.b_blob)

    @property
    def b_contents(self) -> Optional[str]:
        b_blob = self.b_blob
        if b_blob is None:
            return None
        return self._decode(self.b_path, b_blob)

    def a_methods(self, ignore_unsupported_lang, **kwargs):
        if self.a_path and self.language:
            a_blob = self.a_blob
            if a_blob:
                try:
                    # pass `a_path` as meta info
                    return codeparser.extract_functions(
                        a_blob, self.language, path=self.a_path, **kwargs
                    )
                except ParseLangNotSupportError:
                    if not ignore_unsupported_lang:
                        raise
        return []

    def b_methods(self, ignore_unsupported_lang, **kwargs):
        if self.b_path and self.language:
            b_blob = self.b_blob
            if b_blob:
                try:
                    # pass `b_path` as meta info
                    return codeparser.extract_functions(
                        b_blob, self.language, path=self.b_path, **kwargs
                    )
                except ParseLangNotSupportError:
                    if ignore_unsupported_lang:
                        return []
                    raise
        return []
