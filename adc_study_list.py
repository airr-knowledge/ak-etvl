from pathlib import Path
import sys

CACHE_DIR = Path("cache_lists")

def read_list_from_file(cache_name):
    cache_file = CACHE_DIR / f"{cache_name}_cache_list.txt"
    exclude_file = CACHE_DIR / f"{cache_name}_cache_exclude.txt"

    with open(cache_file) as f:
        cache_list = [line.strip() for line in f if line.strip()]

    if not exclude_file.exists():
        return cache_list

    with open(exclude_file) as f:
        exclude = {line.strip() for line in f if line.strip()}

    return [item for item in cache_list if item not in exclude]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <cache_name>")
        sys.exit(1)

    print(" ".join(read_list_from_file(sys.argv[1])))