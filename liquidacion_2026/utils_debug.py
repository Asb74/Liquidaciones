from pathlib import Path
from datetime import datetime

DEBUG_FILE = Path("debug_pipeline.txt")


def debug_write(title, content):
    with open(DEBUG_FILE, "a", encoding="utf-8") as f:
        f.write("\n")
        f.write("=" * 60 + "\n")
        f.write(f"{datetime.now()} - {title}\n")
        f.write("=" * 60 + "\n")
        f.write(str(content))
        f.write("\n")
