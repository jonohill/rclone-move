#!/usr/bin/env python3

import os
from base64 import b64decode
from glob import glob
from json import loads as load_json
from os import environ, makedirs
from os.path import dirname, isfile
from subprocess import PIPE, run
from time import sleep

from .plex_refresh import scan_paths as scan_plex


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
    args = ['rclone', 'lsjson', '--recursive', '--files-only', DEST]
    result = run(args, stdout=PIPE, text=True)
    return load_json(result.stdout)


def rclone_delete(path: str):
    args = ['rclone', 'delete', *EXTRA_FLAGS, f'{DEST}/{path}']
    run(args, check=True)


def rclone_move(source: str, dest: str):
    args = ['rclone', 'move', *EXTRA_FLAGS, '--progress', '--delete-empty-src-dirs', source, dest]
    run(args, check=True)


def rclone_cleanup(path: str):
    args = ['rclone', 'cleanup', *EXTRA_FLAGS, path]
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


def cleanup():
    size_limit = environ.get('RCLONE_SIZE_LIMIT')
    if not size_limit:
        return

    size_limit = int(size_limit)

    while True:
        files = rclone_ls()
        usage = sum(f['Size'] for f in files)
        if usage < size_limit:
            break

        print(f"Destination usage is {usage}, which is greater than {size_limit}, cleaning up")
        
        oldest = min(files, key=lambda f: f['ModTime'])
        print(f"Deleting {oldest['Path']}")
        rclone_rcat('', f"{DEST}/{oldest['Path']}")
        rclone_cleanup(f"{DEST}/{oldest['Path']}")
        rclone_delete(oldest['Path'])


def get_file_sizes(dir: str):
    for f in os.scandir(dir):
        if f.is_dir():
            yield from get_file_sizes(f.path)
        else:
            yield (os.path.join(dir, f.name), f.stat().st_size)


def refresh_plex(paths: list[str]):
    if not PLEX_PREFIX:
        return

    paths = [f'{PLEX_PREFIX}/{p}' for p in paths]
    for lib, path in scan_plex(paths):
        print(f'Plex: Scanned {path} in {lib}')


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

        truncate_names(SOURCE)

        cleanup()
        rclone_move(SOURCE, DEST)
        cleanup()

        dirs = list(set(dirname(f) for f in file_paths))
        refresh_plex(dirs)

    else:
        sleep(60)
