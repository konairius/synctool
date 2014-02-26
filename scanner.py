#!/usr/bin/env python3
# coding=utf-8
"""
This is the scanner daemon, basically just a filesystem walker that checks if the database is up-to-date
and requests others to do the updates if necessary.
"""

import asyncio
from datetime import datetime
import time
from functools import partial
from genericpath import isfile, getmtime, getsize, isdir
import logging
from os import listdir
from os.path import join
from pathlib import Path

from sqlalchemy.orm.exc import NoResultFound

from base import HashRequest, fix_encoding, restore_utf8, Folder, File, FilesystemObject
import config
import database


__author__ = 'konsti'

logger = logging.getLogger(__name__)


@asyncio.coroutine
def add_child(path: Path):
    """
    Adds a Directory to the Database
    @param path: a pathlib Path object
    """
    logger.info('Adding Item: [%r]' % path)
    session = database.get_global_session()
    parent = Folder.by_uri('%s::%s' % (config.HOSTNAME, path.parent), session)

    if path.is_dir():
        child = parent.add_folder(path.name)
        yield from walk(child)
    elif path.is_file():
        child = parent.add_file(name=path.name, mtime=datetime.fromtimestamp(path.stat().st_mtime),
                                size=path.stat().st_size, fhash=None)
    else:
        raise RuntimeError('%s is neither File nor Folder')
    session.add(child)
    return child


@asyncio.coroutine
def check_item(path: Path):
    """
    Checks if an Filesystem Object matches it's Database entry
    @param path: a pathlib Path object
    @raise RuntimeError: if the object is not in the database
    """

    logger.debug('Checking Item: [%r]' % path)
    session = database.get_global_session()
    db_item = FilesystemObject.by_uri('%s::%s' % (config.HOSTNAME, path), session)
    if path.is_dir() and isinstance(db_item, Folder):
        result = yield from walk(db_item)
        return result
    if path.is_file() and isinstance(db_item, (File, HashRequest)):
        if path.stat().st_size == db_item.size and datetime.fromtimestamp(path.stat().st_mtime) == db_item.mtime:
            return True
    logger.info('Check failed on %r' % db_item)
    return False


@asyncio.coroutine
def walk(root):
    """
    @param root: a Folder
    @return:
    """
    adds = list()
    checks = list()
    logger.debug('Filesystem walker entered %s' % root.path)
    for child_name in listdir(root.path):
        child = root.child_by_name(child_name, database.get_global_session())
        child_path = Path(root.path) / child_name
        if child is None:
            if child_path.is_dir() or child_path.is_file():
                adds.append(asyncio.Task(add_child(child_path)))
            else:
                logger.warn(
                    'Ignored Filesystem Object: [%r] because it is neither file nor directory.' % child_path)
        else:
            checks.append(asyncio.Task(check_item(child_path)))

    if len(adds) > 0:
        yield from asyncio.wait(adds)
    if len(checks) > 0:
        yield from asyncio.wait(checks)

    logger.debug('Filesystem walker completed %s' % root.path)
    return adds, checks


def scan(folder):
    """
    Main scanner function
    @param folder: the folder you want to scan
    @return a list of objects that need to be added to the database
    @raise AttributeError: if folder is not local
    """

    session = database.get_session()
    loop = asyncio.get_event_loop()
    folder = Folder.by_id(folder.id, session)

    if not folder.host.is_local:
        raise AttributeError('%r is not local' % folder)

    for name in listdir(folder.path):
        archive = folder.child_by_name(fix_encoding(name), session, eager=True)
        path = join(folder.path, name)

        if isfile(restore_utf8(path)):
            if archive is None or archive.mtime != datetime.fromtimestamp(getmtime(path)) or archive.size != getsize(
                    path):
                if archive is not None:
                    session.delete(archive)
                loop.call_soon(request_hash, name, folder,
                               datetime.fromtimestamp(getmtime(path)),
                               getsize(path), session)

        elif isdir(restore_utf8(path)):
            if archive is None:
                archive = folder.add_folder(fix_encoding(name))
            loop.call_soon(scan, archive)

        for file in folder.files:
            if not isfile(restore_utf8(file.path)):
                session.delete(file)

        for child in folder.folders:
            if not isdir(restore_utf8(child.path)):
                session.delete(child)

    session.commit()


def request_hash(name, folder, mtime, size, session):
    """
    Creates a HashRequest for the given file
    @param name: The filename
    @param folder: The parent Folder
    @param mtime: timestamp as a datetime.datetime object
    @param size: Filesize as Integer in Bytes
    @param session: SQLAlchemy session
    """
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


def run(folder, interval, future=None):
    """
    The runner for the asyncio framework
    @param future: is used to determine if the function was called as result of a callback
    @param folder: The Folder that should be Scanned
    @param interval: The interval in which it should be scanned, None if only once, else the time in seconds
    """
    #TODO: Make it use the eventloop sleep
    runner = partial(run, folder, interval)
    if future is not None:
        time.sleep(interval)
    future = asyncio.async(walk(folder))
    future.add_done_callback(runner)
