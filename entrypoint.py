#!/usr/bin/env python3

import os
import sys
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from json import JSONDecodeError
from json import dump as dump_json
from json import loads as load_json
from os import environ, makedirs
from os.path import dirname, isfile
from subprocess import PIPE, CalledProcessError, Popen, run
from threading import Thread
from time import sleep, time
from typing import Any, Generator, Iterable, List, Optional, TypedDict

from plex_refresh import scan_paths as scan_plex


class RcloneItem(TypedDict):
    Path: str
    Size: int
    ModTime: str


RCLONE_CONF = "/config/rclone/rclone.conf"

CONF_SEED = environ.get("RCLONE_CONFIG_SEED")
SOURCE = environ.get("SOURCE")
DEST = environ.get("DEST")
RCLONE_EXTRA_FLAGS = environ.get("RCLONE_EXTRA_FLAGS", None)
EXTRA_FLAGS = RCLONE_EXTRA_FLAGS.split(",") if RCLONE_EXTRA_FLAGS else []
# Add any command-line arguments passed to the script
CMD_ARGS = sys.argv[1:]
MAX_PATH_LENGTH_STR = environ.get("MAX_PATH_LENGTH", None)
MAX_PATH_LENGTH = int(MAX_PATH_LENGTH_STR) if MAX_PATH_LENGTH_STR else None
PLEX_PREFIX = environ.get("PLEX_PREFIX", None)
STATUS_DIR = environ.get("STATUS_DIR", None)

status: dict[str, Any] = {
    "updated_at": None,
    "state": "idle",
    "move": None,
    "last_move": None,
}


def write_status(**changes: Any):
    """Merge changes into the status and write it out, if STATUS_DIR is set."""
    if not STATUS_DIR:
        return

    status.update(changes)
    status["updated_at"] = time()

    path = os.path.join(STATUS_DIR, "status.json")
    # Not mkstemp: its 0600 mode would hide the file from a reader running as another user
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w") as f:
            dump_json(status, f)
        os.replace(tmp_path, path)
    except OSError as e:
        print(f"Failed to write status: {e}")


def rclone_ls(dir: str) -> List[RcloneItem] | None:
    args = [
        "rclone",
        "lsjson",
        "--recursive",
        "--files-only",
        "--no-mimetype",
        "--tpslimit",
        "4",
        *EXTRA_FLAGS,
        *CMD_ARGS,
        dir,
    ]
    result = run(args, stdout=PIPE, text=True)
    if result.returncode != 0:
        return None
    return load_json(result.stdout)


def rclone_delete(path: str):
    args = ["rclone", "delete", *EXTRA_FLAGS, *CMD_ARGS, f"{DEST}/{path}"]
    run(args, check=True)


def parse_log_line(line: str) -> tuple[str, Optional[dict[str, Any]]]:
    """Return the text to print for an rclone --use-json-log line, and its stats if any."""
    line = line.rstrip("\n")
    try:
        entry = load_json(line)
    except JSONDecodeError:
        return line, None
    if not isinstance(entry, dict):
        return line, None

    msg = str(entry.get("msg", "")).strip()
    if entry.get("object"):
        msg = f"{entry['object']}: {msg}"
    return msg, entry.get("stats")


def rclone_move(source: str, dest: str, include_files: Optional[List[str]] = None):
    args = [
        "rclone",
        "move",
        *EXTRA_FLAGS,
        *CMD_ARGS,
        "--use-json-log",
        "--stats",
        "10s",
        # stats are logged at INFO by default, which the default log level hides
        "--stats-log-level",
        "NOTICE",
        "--delete-empty-src-dirs",
    ]

    if include_files:
        args.extend(["--include-from", "-"])

    args.extend([source, dest])

    paths = include_files or [
        os.path.relpath(f, source) for f, _ in get_file_sizes(source)
    ]
    move: dict[str, Any] = {"started_at": time(), "stats": None}
    write_status(state="moving", move=move)

    proc = Popen(args, stdin=PIPE if include_files else None, stderr=PIPE, text=True)
    if include_files:
        assert proc.stdin
        proc.stdin.write("\n".join(include_files) + "\n")
        proc.stdin.close()

    assert proc.stderr
    for line in proc.stderr:
        text, stats = parse_log_line(line)
        print(text)
        if stats is not None:
            move["stats"] = stats
            write_status(move=move)
    returncode = proc.wait()

    write_status(
        state=loop_state(bool(glob(f"{source}/*"))),
        move=None,
        last_move={
            "finished_at": time(),
            "ok": returncode == 0,
            "paths": paths,
            "bytes": (move["stats"] or {}).get("bytes", 0),
        },
    )
    if returncode != 0:
        raise CalledProcessError(returncode, args)


def rclone_touch(path: str):
    args = ["rclone", "touch", *EXTRA_FLAGS, *CMD_ARGS, path]
    run(args, check=True)


def rclone_rcat(contents: str, dest: str):
    args = ["rclone", "rcat", *EXTRA_FLAGS, *CMD_ARGS, dest]
    run(args, input=contents, check=True)


def check_file_exists(file_path: str) -> tuple[str, bool]:
    """Check if a file exists at the destination. Returns (file_path, exists)."""
    exists = bool(rclone_ls(f"{DEST}/{file_path}"))
    return (file_path, exists)


def get_existing_files(include_files: List[str]) -> List[str]:
    """Check which files already exist at the destination using parallel processing."""
    existing_files: List[str] = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all file existence checks
        future_to_file = {
            executor.submit(check_file_exists, f): f for f in include_files
        }

        # Collect results
        for future in future_to_file:
            file_path, exists = future.result()
            if exists:
                existing_files.append(file_path)

    return existing_files


def truncate_names(dir: str):
    if not MAX_PATH_LENGTH:
        return

    for f in os.scandir(dir):
        if f.is_dir():
            truncate_names(f.path)
        else:
            if len(f.path) > int(MAX_PATH_LENGTH):
                path, ext = os.path.splitext(f.path)
                length = int(MAX_PATH_LENGTH) - len(ext) - 1
                new_path = f"{path[:length]}{ext}"
                print(f"Truncating {f.path} to {new_path}")
                os.rename(f.path, new_path)


cleanup_thread: Optional[Thread] = None


def cleanup():
    def _cleanup():
        size_limit = environ.get("RCLONE_SIZE_LIMIT")
        if not size_limit:
            return

        size_limit = int(size_limit)

        assert DEST, "DEST must be set"
        files: List[RcloneItem] = rclone_ls(DEST) or []
        while True:
            usage = sum(f["Size"] for f in files)
            if usage < size_limit:
                break

            print(
                f"Destination usage is {usage}, which is greater than {size_limit}, cleaning up"
            )

            oldest = min(files, key=lambda f: f["ModTime"])
            print(f"Deleting {oldest['Path']}")
            rclone_rcat("", f"{DEST}/{oldest['Path']}")
            rclone_touch(f"{DEST}/{oldest['Path']}")
            rclone_delete(oldest["Path"])
            files.remove(oldest)

    global cleanup_thread
    if cleanup_thread and cleanup_thread.is_alive():
        print("Cleanup already running")
    else:
        cleanup_thread = Thread(target=_cleanup)
        cleanup_thread.start()


def loop_state(source_has_files: bool) -> str:
    if cleanup_thread and cleanup_thread.is_alive():
        return "cleanup"
    return "waiting" if source_has_files else "idle"


def get_file_sizes(dir: str) -> Generator[tuple[str, int], None, None]:
    for f in os.scandir(dir):
        if f.is_dir():
            yield from get_file_sizes(f.path)
        else:
            yield (os.path.join(dir, f.name), f.stat().st_size)


def moved_dirs(source: str, files: Iterable[str]) -> list[str]:
    """Absolute directories containing `files`, which may be absolute or relative to `source`."""
    return list({dirname(os.path.join(source, f)) for f in files})


def refresh_plex(paths: list[str]):
    if not PLEX_PREFIX:
        print("Plex: No prefix set, skipping")
        return

    paths = [os.path.join(PLEX_PREFIX, os.path.relpath(p, SOURCE)) for p in paths]
    print(f"Plex: Refreshing {paths}")
    for lib, path in scan_plex(paths):
        print(f"Plex: Scanned {path} in {lib}")


def main():
    if not SOURCE or not DEST:
        raise ValueError("SOURCE and DEST must be set")

    if CONF_SEED and not isfile(RCLONE_CONF):
        makedirs(dirname(RCLONE_CONF), exist_ok=True)
        with open(RCLONE_CONF, "w") as f:
            f.write(b64decode(CONF_SEED).decode("utf-8"))

    if STATUS_DIR:
        makedirs(STATUS_DIR, exist_ok=True)

    try:
        prev_file_sizes: dict[str, int] = {}

        while True:
            sleep(30)

            source_has_files = bool(glob(f"{SOURCE}/*"))
            write_status(state=loop_state(source_has_files))

            if source_has_files:
                # not empty dir
                print("Waiting for files to stop changing...")
            else:
                # empty dir
                prev_file_sizes = {}
                continue

            truncate_names(SOURCE)

            include_files: list[str] | None = []
            new_file_sizes = dict(get_file_sizes(SOURCE))
            for f, size in new_file_sizes.items():
                # if file hasn't changed size since last round,
                # assume it's done and should be included
                if f in prev_file_sizes and prev_file_sizes[f] == size:
                    include_files.append(os.path.relpath(f, SOURCE))

            if len(include_files) > 0:
                # prioritise potentially existing files as this should result
                # in cleaning up the source dir faster
                existing_files = get_existing_files(include_files)
                if existing_files:
                    include_files = existing_files
                    print(
                        f"Of which {existing_files} already exist at destination, moving those first"
                    )

                cleanup()

                if len(include_files) == len(new_file_sizes):
                    include_files = None
                    print("All files appear to be done, moving all")

                rclone_move(SOURCE, DEST, include_files)

                refresh_plex(moved_dirs(SOURCE, include_files or new_file_sizes))

                cleanup()

            prev_file_sizes = new_file_sizes
    except Exception:
        if cleanup_thread and cleanup_thread.is_alive():
            print("Error during move, waiting for cleanup to finish before exiting")
            cleanup_thread.join()
        raise


if __name__ == "__main__":
    main()
