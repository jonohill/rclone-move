#!/usr/bin/env python3

import os
from base64 import b64decode
from glob import glob
from json import loads as load_json
from os import environ, makedirs
from os.path import dirname, isfile
from subprocess import PIPE, run
from time import sleep

RCLONE_CONF = '/config/rclone/rclone.conf'

CONF_SEED = environ.get('RCLONE_CONFIG_SEED')
SOURCE = environ.get('SOURCE')
DEST = environ.get('DEST')
EXTRA_FLAGS = environ.get('RCLONE_EXTRA_FLAGS', None)
EXTRA_FLAGS = EXTRA_FLAGS.split(',') if EXTRA_FLAGS else []

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
        rclone_rcat('', f'{oldest["Path"]}')
        rclone_cleanup(f"{DEST}/{oldest['Path']}")
        rclone_delete(oldest['Path'])


def get_file_sizes(dir: str):
    for f in os.scandir(dir):
        if f.is_dir():
            yield from get_file_sizes(f.path)
        else:
            yield (os.path.join(dir, f.name), f.stat().st_size)


while True:
    # if source dir not empty
    if glob(f'{SOURCE}/*'):

        # wait for files to stop changing
        files = ""
        while True:
            print("Waiting for files to stop changing...")
            new_files = ','.join(f'{f} {s}' for f, s in get_file_sizes(SOURCE))
            if files != new_files:
                files = new_files
                sleep(5)
            else:
                break

        cleanup()
        rclone_move(SOURCE, DEST)
        cleanup()

    else:
        sleep(60)
