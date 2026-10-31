import sys
from pathlib import Path

# ctaops-udev is an RPM-only helper, not a pip package, so its module is not
# importable through site-packages. Make tools/ctaops-udev/src/ importable for
# the tests in this directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
