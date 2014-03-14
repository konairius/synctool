#!/usr/bin/env python3
# coding=utf-8
"""
This is the scanner daemon, basically just a filesystem walker that checks if the database is up-to-date
and requests others to do the updates if necessary.
"""

from datetime import datetime
from queue import Queue
from threading import Thread
import time
import logging
from os import listdir
from pathlib import Path

from sqlalchemy.orm.exc import NoResultFound

from base import Folder, FilesystemObject
import config
import database
import database_logging


__author__ = 'konsti'

logger = logging.getLogger(__name__)


class Worker(object):
    """
    scans a directory consumed from queue
    """

    def __init__(self, queue: Queue):
        """
        @param queue: The queue new consumed are consumed from
        """
        self._queue = queue
        self._thread = Thread(target=self._loop)
        self._thread.daemon = True

        self._thread.start()
        logging.debug('Worker Started: %s' % self._thread.name)

    def _loop(self):
        """
        Executes the daemon loop
        """
        while True:
            item = self._queue.get(block=True)
            try:
                self._scan(item)
            except Exception as e:
                logging.exception(e)
                time.sleep(30)
            finally:
                self._queue.task_done()

    def _scan(self, item: Path):
        """
        @param item: a File or a Folder
        """
        if item.is_dir():
            self._scan_folder(item)
        elif item.is_file():
            self._scan_file(item)

    def _scan_file(self, file: Path):
        """
        @param file: a File
        """

        logger.debug('Scanning File: %s' % file)
        session = database.get_session(new_engine=False)
        try:
            db_item = FilesystemObject.by_uri('%s::%s' % (config.HOSTNAME, file), session)
        except NoResultFound:
            self._add_child(file)
            session.close()
            return

        if file.stat().st_size == db_item.size and datetime.fromtimestamp(file.stat().st_mtime) == db_item.mtime:
            pass
        else:
            session.delete(db_item)
            session.commit()
            self._queue.put(file)
            logging.info('Cache mismatch: %r' % db_item)
        session.close()

    def _scan_folder(self, folder: Path):
        """
        @param folder: a Folder
        """
        logger.debug('Scanning Folder: %s' % folder)
        session = database.get_session(new_engine=False)
        try:
            FilesystemObject.by_uri('%s::%s' % (config.HOSTNAME, folder), session)
        except NoResultFound:
            self._add_child(folder)
        finally:
            session.close()

        for child_name in listdir(str(folder)):
            self._queue.put(folder / child_name)

    def _add_child(self, path: Path):
        """
        Adds a Directory to the Database
        @param path: a pathlib Path object
        """
        logger.info('Adding Item: [%r]' % path)
        session = database.get_session(new_engine=False)
        parent = Folder.by_uri('%s::%s' % (config.HOSTNAME, path.parent), session)
        if path.is_dir():
            child = parent.add_folder(path.name)
            self._queue.put(path)
        elif path.is_file():
            child = parent.add_file(name=path.name, mtime=datetime.fromtimestamp(path.stat().st_mtime),
                                    size=path.stat().st_size, fhash=None)
        else:
            session.close()
            raise RuntimeError('%s is neither File nor Folder' % path)
        session.add(child)
        session.commit()
        session.close()


def run(folder, interval):
    """
    @param folder: The Folder that should be Scanned
    @param interval: The interval in which it should be scanned, None if only once, else the time in seconds
    """

    database_logging.configure_logger()
    logging.info('Scanner started with: folder=%s, interval=%s' % (folder, interval))

    folder = Path(folder.path)

    dir_queue = Queue()
    init(workers=40, queue=dir_queue)

    while True:
        start = datetime.now()
        if dir_queue.empty():  # Only add the Root folder if the Queue is empty.
            dir_queue.put(folder)
        dir_queue.join()
        logging.debug('Scanner round Completed in %s' % (datetime.now() - start))
        if interval is None:
            break  # This is used for debugging
        time.sleep(interval)


def init(workers, queue):
    """
    Starts the worker Threads
    @param queue: the queue passed to the Worker Threads
    @param workers: Number of worker Threads
    """
    worker_threads = list()
    for _ in range(workers):
        worker_threads.append(Worker(queue))

