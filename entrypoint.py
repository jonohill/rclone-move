#!/usr/bin/env python3

import os
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from json import loads as load_json
from os import environ, makedirs
from os.path import dirname, isfile
from subprocess import PIPE, run
from threading import Thread
from time import sleep
from typing import Generator, Optional, List, TypedDict

from plex_refresh import scan_paths as scan_plex


class RcloneItem(TypedDict):
    Path: str
    Size: int
    ModTime: str


RCLONE_CONF = '/config/rclone/rclone.conf'

CONF_SEED = environ.get('RCLONE_CONFIG_SEED')
SOURCE = environ.get('SOURCE')
DEST = environ.get('DEST')
RCLONE_EXTRA_FLAGS = environ.get('RCLONE_EXTRA_FLAGS', None)
EXTRA_FLAGS = RCLONE_EXTRA_FLAGS.split(',') if RCLONE_EXTRA_FLAGS else []
MAX_PATH_LENGTH_STR = environ.get('MAX_PATH_LENGTH', None)
MAX_PATH_LENGTH = int(MAX_PATH_LENGTH_STR) if MAX_PATH_LENGTH_STR else None
PLEX_PREFIX = environ.get('PLEX_PREFIX', None)

if not SOURCE or not DEST:
    raise ValueError('SOURCE and DEST must be set')

if CONF_SEED and not isfile(RCLONE_CONF):
    makedirs(dirname(RCLONE_CONF), exist_ok=True)
    with open(RCLONE_CONF, 'w') as f:
        f.write(b64decode(CONF_SEED).decode('utf-8'))

def rclone_ls(dir: str) -> List[RcloneItem] | None:
    args = ['rclone', 'lsjson',
        '--recursive',
        '--files-only',
        '--no-mimetype',
        '--tpslimit', '4',
        *EXTRA_FLAGS,
        dir
    ]
    result = run(args, stdout=PIPE, text=True)
    if result.returncode != 0:
        return None
    return load_json(result.stdout)


def rclone_delete(path: str):
    args = ['rclone', 'delete', *EXTRA_FLAGS, f'{DEST}/{path}']
    run(args, check=True)


def rclone_move(source: str, dest: str, include_files: Optional[List[str]] = None):
    args = ['rclone', 'move', *EXTRA_FLAGS, '--progress', '--delete-empty-src-dirs']

    if include_files:
        args.extend(['--include-from', '-'])

    args.extend([source, dest])

    if include_files:
        include_input = '\n'.join(include_files) + '\n'
        run(args, input=include_input, text=True, check=True)
    else:
        run(args, check=True)


def rclone_touch(path: str):
    args = ['rclone', 'touch', *EXTRA_FLAGS, path]
    run(args, check=True)


def rclone_rcat(contents: str, dest: str):
    args = ['rclone', 'rcat', *EXTRA_FLAGS, dest]
    run(args, input=contents, check=True)


def check_file_exists(file_path: str) -> tuple[str, bool]:
    """Check if a file exists at the destination. Returns (file_path, exists)."""
    exists = bool(rclone_ls(f'{DEST}/{file_path}'))
    return (file_path, exists)


def get_existing_files(include_files: List[str]) -> List[str]:
    """Check which files already exist at the destination using parallel processing."""
    existing_files: List[str] = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all file existence checks
        future_to_file = {executor.submit(check_file_exists, f): f for f in include_files}
        
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
                new_path = f'{path[:length]}{ext}'
                print(f"Truncating {f.path} to {new_path}")
                os.rename(f.path, new_path)

cleanup_thread: Optional[Thread] = None

def cleanup():
    def _cleanup():
        size_limit = environ.get('RCLONE_SIZE_LIMIT')
        if not size_limit:
            return

        size_limit = int(size_limit)

        assert DEST, "DEST must be set"
        files: List[RcloneItem] = rclone_ls(DEST) or []
        while True:
            usage = sum(f['Size'] for f in files)
            if usage < size_limit:
                break

            print(f"Destination usage is {usage}, which is greater than {size_limit}, cleaning up")
            
            oldest = min(files, key=lambda f: f['ModTime'])
            print(f"Deleting {oldest['Path']}")
            rclone_rcat('', f"{DEST}/{oldest['Path']}")
            rclone_touch(f"{DEST}/{oldest['Path']}")
            rclone_delete(oldest['Path'])
            files.remove(oldest)

    global cleanup_thread
    if cleanup_thread and cleanup_thread.is_alive():
        print('Cleanup already running')
    else:
        cleanup_thread = Thread(target=_cleanup)
        cleanup_thread.start()


def get_file_sizes(dir: str) -> Generator[tuple[str, int], None, None]:
    for f in os.scandir(dir):
        if f.is_dir():
            yield from get_file_sizes(f.path)
        else:
            yield (os.path.join(dir, f.name), f.stat().st_size)


def refresh_plex(paths: list[str]):
    if not PLEX_PREFIX:
        print('Plex: No prefix set, skipping')
        return

    paths = [os.path.join(PLEX_PREFIX, os.path.relpath(p, SOURCE)) for p in paths]
    print(f'Plex: Refreshing {paths}')
    for lib, path in scan_plex(paths):
        print(f'Plex: Scanned {path} in {lib}')


try:
    prev_file_sizes: dict[str, int] = {}

    while True:
        sleep(5)

        if glob(f'{SOURCE}/*'):
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
                print(f"Of which {existing_files} already exist at destination, moving those first")

            cleanup()

            if len(include_files) == len(new_file_sizes):
                include_files = None
                print("All files appear to be done, moving all")

            rclone_move(SOURCE, DEST, include_files)

            dirs = list(set(dirname(f) for f in (include_files or new_file_sizes.keys())))
            refresh_plex(dirs)

            cleanup()

        prev_file_sizes = new_file_sizes
except Exception:
    if cleanup_thread and cleanup_thread.is_alive():
        print('Error during move, waiting for cleanup to finish before exiting')
        cleanup_thread.join()
    raise
