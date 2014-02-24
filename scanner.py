#!/usr/bin/env python3
# coding=utf-8
"""
This is the scanner daemon, basically just a filesystem walker that checks if the database is up-to-date
and requests others to do the updates if necessary.
"""

import asyncio
from datetime import datetime
from genericpath import isfile, getmtime, getsize, isdir
import logging
from os import listdir
from os.path import join

from sqlalchemy.orm.exc import NoResultFound

from base import HashRequest, fix_encoding, restore_utf8
import database


__author__ = 'konsti'

logger = logging.getLogger(__name__)


def scan(folder):
    """
    Main scanner function
    @param folder: the folder you want to scan
    @return a list of objects that need to be added to the database
    @raise AttributeError: if folder is not local
    """

    loop = asyncio.get_event_loop()

    session = database.get_session()
    if not folder.host.is_local:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(folder.path):
        archive = folder.child_by_name(fix_encoding(name), eager=True)
        path = join(folder.path, name)

        if isfile(restore_utf8(path)):
            if archive is None or archive.mtime != datetime.fromtimestamp(getmtime(path)) or archive.size != getsize(
                    path):
                if archive is not None:
                    session.delete(archive)
                loop.call_soon(request_hash, name=name, folder=folder,
                               mtime=datetime.fromtimestamp(getmtime(path)),
                               size=getsize(path))

        elif isdir(restore_utf8(path)):
            if archive is None:
                archive = folder.add_folder(fix_encoding(name))
            loop.call_soon(scan, folder=archive)

        for file in folder.files:
            if not isfile(restore_utf8(file.path)):
                session.delete(file)

        for child in folder.folders:
            if not isdir(restore_utf8(child.path)):
                session.delete(child)

    session.commit()


def request_hash(name, folder, mtime, size):
    """
    Creates a HashRequest for the given file
    @param name: The filename
    @param folder: The parent Folder
    @param mtime: timestamp as a datetime.datetime object
    @param size: Filesize as Integer in Bytes
    """
    session = database.get_session()
    name = fix_encoding(name)
    try:
        session.query(HashRequest).filter(HashRequest.name == name, HashRequest.folder == folder,
                                          HashRequest.mtime == mtime).one()
    except NoResultFound:
        request = HashRequest(name=name, folder=folder, mtime=mtime, size=size, host=folder.host)
        session.add(request)
        session.commit()
        return request
    return None


def run(folder, interval):
    """
    The runner for the asyncio framework
    @param folder: The Folder that should be Scanned
    @param interval: The interval in which it should be scanned, None if only once, else the time in seconds
    """
    loop = asyncio.get_event_loop()
    loop.call_soon(scan, folder)
    if interval:
        loop.call_later(interval, run, folder, interval)
