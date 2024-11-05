import fcntl
import json
import os
import shutil
import tempfile
from typing import Optional
from loguru import logger
from git import Repo

POOL_FILE = os.path.join(os.path.dirname(__file__), "repo_pool.json")
REPOSITORY_BASE = "/data/repos"


class NoRepoError(Exception):
    def __init__(self, repo):
        self.repo = repo

    def __str__(self):
        return "No such repo: {}".format(self.repo)


class FileLock:
    def __init__(self, file, mode="w"):
        self.file = open(file, "rb" if self.mode == "r" else "wb")
        self.mode = mode

    def __enter__(self):
        fcntl.flock(self.file, fcntl.LOCK_SH if self.mode == "r" else fcntl.LOCK_EX)

    def __exit__(self, exc_type, exc_val, exc_tb):
        fcntl.flock(self.file, fcntl.LOCK_UN)


class FileDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.lock_file = db_file + ".lock"

    class Transaction:
        def __init__(self, db_file, lock_file, mode):
            self.db_file = db_file
            self.lock_file = FileLock(lock_file, mode)

        def __enter__(self):
            self.lock_file.__enter__()
            self._load()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.lock_file.mode == "w":
                self._dump()
            self.lock_file.__exit__(exc_type, exc_val, exc_tb)

        def _load(self):
            if os.path.exists(self.db_file):
                with open(self.db_file, "r") as f:
                    obj = json.load(f)
                    self.lrucache = obj.get("lrucache", {})
                    self.badcache = set(obj.get("badcache", []))
            else:
                self.lrucache = {}
                self.badcache = set()

        def _dump(self):
            with tempfile.NamedTemporaryFile("w", delete=False) as f:
                json.dump(
                    {"lrucache": self.lrucache, "badcache": list(self.badcache)}, f
                )
                os.rename(f.name, self.db_file)

    def lock_read(self):
        return self.Transaction(self.db_file, self.lock_file, "r")

    def lock(self):
        return self.Transaction(self.db_file, self.lock_file, "w")


class LRURepoPoolSafe:
    def __init__(self, base, max_size=4000):
        self.repo_base = base
        self.max_size = max_size
        self.file_db = FileDB(POOL_FILE)

        # Validate the repo base
        with self.file_db.lock() as trx:
            for key in list(trx.lrucache.keys()):
                path = os.path.join(self.repo_base, key)
                if not os.path.exists(path):
                    del trx.lrucache[key]

    def has(self, git_url):
        key = self._digest(git_url)

        with self.file_db.lock_read() as trx:
            return key in trx.lrucache or key in trx.badcache

    def evict(self, git_url):
        key = self._digest(git_url)
        repo_path = os.path.join(self.repo_base, key)
        repo_lock = os.path.join(self.repo_base, key + ".lock")

        with FileLock(repo_lock):
            with self.file_db.lock() as trx:
                if key in trx.lrucache:
                    del trx.lrucache[key]
                    if os.path.exists(repo_path):
                        shutil.rmtree(repo_path)
                    if os.path.exists(repo_lock):
                        os.unlink(repo_lock)

    def get(self, git_url) -> Optional[Repo]:
        key = self._digest(git_url)
        repo_path = os.path.join(self.repo_base, key)
        repo_lock = os.path.join(self.repo_base, key + ".lock")

        with self.file_db.lock() as trx:
            # The repo is already cloned and in the cache
            if key in trx.lrucache:
                trx.lrucache[key] += 1
                return Repo(repo_path)

            # The url is bad
            if key in trx.badcache:
                return None

            if len(trx.lrucache) >= self.max_size:
                # remove the least used item
                least_used = min(trx.lrucache.items(), key=lambda x: x[1])
                least_key = least_used[0]
                least_path = os.path.join(self.repo_base, least_key)
                least_lock = os.path.join(self.repo_base, least_key + ".lock")
                if os.path.exists(least_path):
                    shutil.rmtree(least_path)
                if os.path.exists(least_lock):
                    os.unlink(least_lock)
                del trx.lrucache[least_key]

        # acquire a file lock of the repo
        # if the lock is acquired, we are the only process/thread that is cloning the repo
        # otherwise, we wait for the other process to finish cloning
        with FileLock(repo_lock):
            # The repo has been cloned by another process
            # We can just open it
            if os.path.exists(repo_path):
                inst = Repo(repo_path)
                return inst

            with self.file_db.lock() as trx:
                # The url is bad
                if key in trx.badcache:
                    return None

            # Clone the repo
            try:
                inst = Repo.clone_from(git_url, repo_path)
            except Exception as e:
                # Clone failed, bad git url
                logger.error(e)
                with self.file_db.lock() as trx:
                    trx.badcache.add(key)
                return None

            # Clone succeeded
            with self.file_db.lock() as trx:
                if key not in trx.lrucache:
                    trx.lrucache[key] = 1
                else:
                    trx.lrucache[key] += 1
                return Repo(repo_path)

    def _digest(self, git_url: str):
        git_url = git_url.strip().lower()
        git_url = git_url.rstrip("/")
        if git_url.endswith(".git"):
            git_url = git_url[:-4]
        if git_url.startswith("https://"):
            group, repo = git_url.split("/")[-2:]
        else:
            group, repo = git_url.split(":")[-1].split("/")
        return f"{group}+{repo}"


pool = LRURepoPoolSafe(REPOSITORY_BASE, 5000)


def get(git_url) -> Repo:
    repo = pool.get(git_url)
    if repo is None:
        raise NoRepoError(git_url)
    return repo
