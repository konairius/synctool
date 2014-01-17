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
from sqlalchemy.orm.exc import NoResultFound

from base import Host, HashRequest, fix_encoding, restore_utf8


__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def scan(folder, session):
    """
    Main scanner function
    @param folder: the folder you want to scan
    @param session: The Session used for Querying
    @return a list of objects that need to be added to the database
    @raise AttributeError: if folder is not local
    """

    delete_objects = list()
    add_objects = list()

    session.begin_nested()
    if not folder.host.is_local:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(folder.path):
        archive = folder.child_by_name(fix_encoding(name), session, eager=True)
        path = join(folder.path, name)

        if isfile(restore_utf8(path)):
            if archive is None or archive.mtime != datetime.fromtimestamp(getmtime(path)) or archive.size != getsize(
                    path):
                if archive is not None:
                    delete_objects.append(archive)

                add_objects.append(request_hash(name=name, folder=folder,
                                                mtime=datetime.fromtimestamp(getmtime(path)),
                                                size=getsize(path), session=session))

        elif isdir(restore_utf8(path)):
            if archive is None:
                archive = folder.add_folder(fix_encoding(name))
            add, delete = scan(folder=archive, session=session)

            add_objects += add
            delete_objects += delete

        for file in folder.files:
            if not isfile(restore_utf8(file.path)):
                delete_objects.append(file)

        for child in folder.folders:
            if not isdir(restore_utf8(child.path)):
                delete_objects.append(child)

    session.commit()
    return filter(None, add_objects), filter(None, delete_objects)


def request_hash(name, folder, mtime, size, session):
    """
    Creates a HashRequest for the given file
    @param name: The filename
    @param folder: The parent Folder
    @param mtime: timestamp as a datetime.datetime object
    @param size: Filesize as Integer in Bytes
    @param session: The Session used for Querying
    """
    name = fix_encoding(name)
    try:
        session.query(HashRequest).filter(HashRequest.name == name, HashRequest.folder == folder,
                                          HashRequest.mtime == mtime).one()
    except NoResultFound:
        request = HashRequest(name=name, folder=folder, mtime=mtime, size=size, host=folder.host)
        return request
    return None


def daemon(args):
    """
    @param args: Args namespace as returned by argparse
    """

    database = create_engine(args.database, echo=False)
    session_maker = sessionmaker(bind=database)
    session = session_maker()

    host = Host.by_name(socket.gethostname(), session)
    while True:
        logger.debug('Stating scanner round')
        for root in host.roots:
            add, delete = scan(root, session)
            #print(add)
            #print(delete)
            session.add_all(add)
            for obj in delete:
                session.delete(obj)
            session.commit()
        sleep(args.interval)


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Scanner Tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-i', '--interval', type=int, metavar='SECONDS', default=3600,
                        help='Interval between two Scan runs, defaults to 10 hours')
    args = parser.parse_args(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Scanner started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    daemon(args)


if __name__ == '__main__':
    exit(main())
