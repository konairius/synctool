#!/usr/bin/env python3

from _sha1 import sha1
import argparse
from datetime import datetime
from genericpath import isfile, getmtime, getsize, isdir
from os import listdir
from os.path import join
import socket
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import set_session, Base, Folder, Host

__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def scan(folder):
    if not folder.host.is_local:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(folder.path):
        archive = folder.child_by_name(name)
        path = join(folder.path, name)
        if isfile(path):
            if archive is None or archive.mtime != datetime.fromtimestamp(getmtime(path)) or archive.size != getsize(
                    path):
                if archive is not None:
                    archive.delete()
                folder.add_file(name=name, fhash=_calculate_hash(path), mtime=datetime.fromtimestamp(getmtime(path)),
                                size=getsize(path))
        elif isdir(path):
            if archive is None:
                archive = folder.add_folder(name)
            scan(archive)


def _calculate_hash(file_path: str):
    logger.info('Calculating hash for: %s ' % file_path)
    _hash = sha1()
    with open(file_path, 'rb') as data:
        for chunk in iter(lambda: data.read(1024 * _hash.block_size), b''):
            _hash.update(chunk)
        value = _hash.hexdigest()
        return value


def daemon(session):
    set_session(session)
    host = Host.by_name(socket.gethostname())
    while True:
        for root in host.roots:
            scan(root)
            session.commit()
        sleep(3600)


def main():
    parser = argparse.ArgumentParser(description='Filesystem Scanner')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', default='sqlite:///:memory:')
    parser.add_argument('--dir', dest='dirs', type=str, metavar='URI',
                        help='The directory you want to add to the Database', action='append')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--create_schema', action='store_true')
    parser.add_argument('--config_only', action='store_true')
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    database = create_engine(args.database, echo=False)

    if args.create_schema:
        Base.metadata.create_all(database)

    session = sessionmaker(bind=database)()
    set_session(session)
    print(args.dirs)
    for root_dir in args.dirs:
        root_uri = '%s::%s' % (socket.gethostname(), root_dir)
        print(root_uri)
        Folder.by_uri(root_uri)

    session.commit()

    if args.config_only:
        exit(0)

    daemon(session)

    session.commit()
    session.close_all()


if __name__ == '__main__':
    main()
