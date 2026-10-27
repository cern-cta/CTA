import os
import tempfile
from typing import Union

Value = Union[str, int]
Labels = dict[str, Value]


def escape(value: Value) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def sample(metric: str, labels: Labels, value: Value, *, prefix: str) -> str:
    label_str = ",".join(f'{k}="{escape(v)}"' for k, v in labels.items())
    return f"{prefix}_{metric}{{{label_str}}} {value}"


def write_atomic(path: str, content: str) -> None:
    """Write via rename so node_exporter's textfile collector never reads a partial file."""
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".%s." % os.path.basename(path))
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
