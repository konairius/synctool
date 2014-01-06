from _sha1 import sha1
from genericpath import isfile, getmtime, getsize, isdir
from os import listdir
from os.path import join
import socket

__author__ = 'konsti'


import logging

logger = logging.getLogger(__name__)


def scan(folder):
    if socket.gethostname() != folder.host.name:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(folder.path):
        archive = folder.child_by_name(name)
        path = join(folder.path, name)
        if isfile(path):
            if archive is None or archive.mtime != getmtime(path) or archive.size != getsize(path):
                if archive is not None:
                    archive.delete()
                folder.add_file(name=name, fhash=_calculate_hash(path), mtime=getmtime(path), size=getsize(path))
        elif isdir(path):
            if archive is None:
                archive = folder.add_folder(name)
            scan(archive)


def _calculate_hash(file_path: str):
    #logger.info('Analysing file at: ' + file_path)
    _hash = sha1()
    with open(file_path, 'rb') as data:
        for chunk in iter(lambda: data.read(1024 * _hash.block_size), b''):
            _hash.update(chunk)
        value = _hash.hexdigest()
        return value
