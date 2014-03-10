#!/usr/bin/env python3
# coding=utf-8
"""
This is the scanner daemon, basically just a filesystem walker that checks if the database is up-to-date
and requests others to do the updates if necessary.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
from functools import partial
import logging
from os import listdir
from pathlib import Path

from base import HashRequest, Folder, File, FilesystemObject
import config
import database


__author__ = 'konsti'

logger = logging.getLogger(__name__)

WALKER_POOL = None


@asyncio.coroutine
def add_child(path: Path):
    """
    Adds a Directory to the Database
    @param path: a pathlib Path object
    """
    logger.info('Adding Item: [%r]' % path)
    session = database.get_global_session()
    parent = yield from Folder.by_uri('%s::%s' % (config.HOSTNAME, path.parent), session)

    if path.is_dir():
        child = parent.add_folder(path.name)
        yield from walk(child)
    elif path.is_file():
        child = parent.add_file(name=path.name, mtime=datetime.fromtimestamp(path.stat().st_mtime),
                                size=path.stat().st_size, fhash=None)
    else:
        raise RuntimeError('%s is neither File nor Folder' % path)
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
    db_item = yield from FilesystemObject.by_uri('%s::%s' % (config.HOSTNAME, path), session)
    if path.is_dir() and isinstance(db_item, Folder):
        adds, checks = yield from walk(db_item)
        return checks
    if path.is_file() and isinstance(db_item, (File, HashRequest)):
        if path.stat().st_size == db_item.size and datetime.fromtimestamp(path.stat().st_mtime) == db_item.mtime:
            return True, db_item
    logger.info('Check failed on [%r]' % db_item)
    return False, db_item


@asyncio.coroutine
def walk(root):
    """
    @param root: a Folder
    @return:
    """
    adds = list()
    checks = list()
    babes = list()
    logger.debug('Filesystem walker entered %s' % root.path)
    for child_name in listdir(root.path):
        babes.append((
            asyncio.Task(root.child_by_name(child_name, database.get_global_session())), Path(root.path) / child_name))
    for child, child_path in babes:
        yield from asyncio.wait_for(child, None)
        child = child.result()
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


def run(folder, interval, future=None):
    """
    The runner for the asyncio framework
    @param future: is used to determine if the function was called as result of a callback
    @param folder: The Folder that should be Scanned
    @param interval: The interval in which it should be scanned, None if only once, else the time in seconds
    """
    #TODO: Make it use the eventloop sleep
    global WALKER_POOL
    WALKER_POOL = ThreadPoolExecutor(max_workers=4)
    runner = partial(run, folder, interval)
    if future and interval:
        time.sleep(interval)
    future = asyncio.async(walk(folder))
    if not interval:
        return
    future.add_done_callback(runner)
