import json
import shutil
import subprocess
import tempfile
from os import PathLike
from pathlib import Path

# Check exist for github-linguist
if not shutil.which("github-linguist"):
    raise FileNotFoundError(
        "github-linguist is not installed.\n"
        "Please refer to https://github.com/github-linguist/linguist for installation."
    )


def detect_language(
    file: PathLike = None,
    src: bytes = None,
    suffix: str = None,
):
    if file:
        file = Path(file)
        src = file.open("rb").read()
        suffix = file.suffix
    elif src and suffix:
        pass
    else:
        raise ValueError("Either file or src and suffix must be provided.")

    with tempfile.NamedTemporaryFile("wb", suffix=suffix) as tmp_f:
        # create temporary file to allow linguistic to escape .gitignore
        tmp_f.write(src)
        tmp_f.flush()
        output = subprocess.check_output(
            f"github-linguist -j {tmp_f.name}",
            shell=True,
            text=True,
            stderr=subprocess.PIPE,
        )
        """
        example output: 
        {
            "./codebox/rdb.py": {
                "lines":185,
                "sloc":162,
                "type":"Text",
                "mime_type":"application/x-python",
                "language":"Python",
                "large":false,
                "generated":false,
                "vendored":false
            }
        }
        """
        return list(json.loads(output).values())[0]["language"]


if __name__ == "__main__":
    print(detect_language(file=__file__))
    print(detect_language(src=b"print('Hello, World!')", suffix=".py"))
