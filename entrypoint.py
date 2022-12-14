#!/usr/bin/env python3

import os
from base64 import b64decode
from glob import glob
from json import loads as load_json
from os import environ, makedirs
from os.path import dirname, isfile
from subprocess import PIPE, run
from threading import Thread
from time import sleep
from typing import Optional

from plex_refresh import scan_paths as scan_plex

RCLONE_CONF = '/config/rclone/rclone.conf'

CONF_SEED = environ.get('RCLONE_CONFIG_SEED')
SOURCE = environ.get('SOURCE')
DEST = environ.get('DEST')
EXTRA_FLAGS = environ.get('RCLONE_EXTRA_FLAGS', None)
EXTRA_FLAGS = EXTRA_FLAGS.split(',') if EXTRA_FLAGS else []
MAX_PATH_LENGTH = environ.get('MAX_PATH_LENGTH', None)
MAX_PATH_LENGTH = int(MAX_PATH_LENGTH) if MAX_PATH_LENGTH else None
PLEX_PREFIX = environ.get('PLEX_PREFIX', None)

if not SOURCE or not DEST:
    raise ValueError('SOURCE and DEST must be set')

if CONF_SEED and not isfile(RCLONE_CONF):
    makedirs(dirname(RCLONE_CONF), exist_ok=True)
    with open(RCLONE_CONF, 'w') as f:
        f.write(b64decode(CONF_SEED).decode('utf-8'))

def rclone_ls():
    args = ['rclone', 'lsjson', 
        '--recursive', 
        '--files-only',
        '--no-mimetype',
        '--tpslimit', '4',
        *EXTRA_FLAGS,
        DEST
    ]
    result = run(args, stdout=PIPE, text=True)
    return load_json(result.stdout)


def rclone_delete(path: str):
    args = ['rclone', 'delete', *EXTRA_FLAGS, f'{DEST}/{path}']
    run(args, check=True)


def rclone_move(source: str, dest: str):
    args = ['rclone', 'move', *EXTRA_FLAGS, '--progress', '--delete-empty-src-dirs', source, dest]
    run(args, check=True)


def rclone_touch(path: str):
    args = ['rclone', 'touch', *EXTRA_FLAGS, path]
    run(args, check=True)


def rclone_rcat(contents: str, dest: str):
    args = ['rclone', 'rcat', *EXTRA_FLAGS, dest]
    run(args, input=contents, check=True)


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

        files: list = rclone_ls()
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


def get_file_sizes(dir: str):
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
    while True:
        # if source dir not empty
        if glob(f'{SOURCE}/*'):

            # wait for files to stop changing
            file_paths = []
            files = ""
            while True:
                print("Waiting for files to stop changing...")
                file_sizes = list(get_file_sizes(SOURCE))
                new_files = ','.join(f'{f} {s}' for f, s in file_sizes)
                file_paths = [f for f, _ in file_sizes]
                if files != new_files:
                    files = new_files
                    sleep(5)
                else:
                    break

            cleanup()

            truncate_names(SOURCE)
            rclone_move(SOURCE, DEST)

            dirs = list(set(dirname(f) for f in file_paths))
            refresh_plex(dirs)

            cleanup()
        else:
            sleep(60)
except Exception:
    if cleanup_thread and cleanup_thread.is_alive():
        print('Error during move, waiting for cleanup to finish before exiting')
        cleanup_thread.join()
    raise
