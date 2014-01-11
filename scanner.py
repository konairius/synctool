#!/usr/bin/env python3
# coding=utf-8
"""
This is the scanner daemon, basically just a filesystem walker that checks if the database is up-to-date
and requests others to do the updates if necessary.
"""

import argparse
from datetime import datetime
from genericpath import isfile, getmtime, getsize, isdir
from os import listdir
from os.path import join
import socket
from time import sleep
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from base import set_session, Host, HashRequest, session, remove_surrogate_escaping, restore_utf8


__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def scan(folder):
    """
    Main scanner function
    @param folder: the folder you want to scan
    @raise AttributeError: if folder is not local
    """
    if not folder.host.is_local:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(restore_utf8(folder.path)):
        archive = folder.child_by_name(remove_surrogate_escaping(name))
        path = join(folder.path, name)
        if isfile(restore_utf8(path)):
            if archive is None or archive.mtime != datetime.fromtimestamp(getmtime(path)) or archive.size != getsize(
                    path):
                if archive is not None:
                    archive.delete()
                request_hash(name=name, folder=folder, mtime=datetime.fromtimestamp(getmtime(path)),
                             size=getsize(path))
        elif isdir(restore_utf8(path)):
            if archive is None:
                archive = folder.add_folder(remove_surrogate_escaping(name))
            scan(archive)

    for file in folder.files:
        if not isfile(restore_utf8(file.path)):
            file.delete()

    for child in folder.folders:
        if not isdir(restore_utf8(child.path)):
            child.delete()


def request_hash(name, folder, mtime, size):
    """
    Creates a HashRequest for the given file
    @param name: The filename
    @param folder: The parent Folder
    @param mtime: timestamp as a datetime.datetime object
    @param size: Filesize as Integer in Bytes
    """
    name = remove_surrogate_escaping(name)
    request = session().query(HashRequest).filter(HashRequest.name == name, HashRequest.folder == folder,
                                                  HashRequest.size == size, HashRequest.mtime == mtime).first()
    if request is None:
        request = HashRequest(name=name, folder=folder, mtime=mtime, size=size, host=folder.host)
        session().add(request)
        session().flush()


def daemon(interval):
    """
    @param interval: Integer, seconds between to scan runs
    """
    host = Host.by_name(socket.gethostname())
    while True:
        logger.debug('Stating scanner round')
        for root in host.roots:
            scan(root)
            session().commit()
        sleep(interval)


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Scanner Tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-i', '--interval', type=int, metavar='SECONDS', default=360,
                        help='Interval between two Scan runs, defaults to 1 hour')
    args = parser.parse_args(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Scanner started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    database = create_engine(args.database, echo=False)
    s = sessionmaker(bind=database)()
    set_session(s)
    daemon(args.interval)


if __name__ == '__main__':
    exit(main())
