#!/usr/bin/env python3
# coding=utf-8
"""
This is a Tool to Evaluate a Synctool database
"""
import argparse
from collections import namedtuple, Sequence
import logging
import sys

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from base import Folder, File
import database_logging


__author__ = 'konsti'

logger = logging.getLogger(__name__)

Change = namedtuple('Change', ['type', 'source', 'target'])


class ChangeSet(Sequence):
    """
    This is a list of changes that need to be made in order to sync to folders
    @param source: the source folder
    @param dst: the target folder
    @param session: The Session used for Querying
    """

    def __init__(self, source, dst, session):
        self.changes = self._get_changes(source, dst, session)

    def __len__(self):
        return self.changes.__len__()

    def __getitem__(self, index):
        return self.changes.__getitem__(index)

    @staticmethod
    def _get_changes(source: Folder, dst: Folder, session):
        changes = list()

        for file in source.files:
            dst_file = dst.child_by_name(file.name, session)
            if dst_file is None:
                dst_file = File(name=file.name, host=dst.host, folder=dst, hash=file.hash, mtime=file.mtime,
                                size=file.size)
                obj = session.query(File).filter(and_(File.hash == file.hash, File.host == dst.host)).first()
                if obj is None:
                    change = Change(type='COPY', source=file, target=dst_file)
                else:
                    change = Change(type='LCOPY', source=obj, target=dst_file)

                logger.info('Adding change: %s' % str(change))
                changes.append(change)

            elif file.hash == dst_file.hash:
                pass
            else:
                if file.mtime > dst_file.mtime:
                    change = Change(type='REPLACE', source=file, target=dst_file)
                else:
                    change = Change(type='CONFLICT', source=file, target=dst_file)
                logger.info('Adding change: %s' % str(change))
                changes.append(change)

        for folder in source.folders:
            dst_folder = dst.child_by_name(folder.name, session)
            if dst_folder is None:
                dst_folder = Folder(name=folder.name, parent=dst, host=dst.host)
                changes.append(Change(type='COPY', source=folder, target=dst_folder))
            else:
                changes += ChangeSet._get_changes(source=folder, dst=dst_folder, session=session)

        for file in dst.files:
            src_file = source.child_by_name(file.name, session)
            if src_file is None:
                change = Change(type='DELETE', source='', target=file)
                logger.info('Adding change: %s' % str(change))
                changes.append(change)

        for folder in dst.folders:
            src_folder = source.child_by_name(folder.name, session)
            if src_folder is None:
                change = Change(type='DELETE', source='', target=folder)
                logger.info('Adding change: %s' % str(change))
                changes.append(change)

        return changes

    def get_string(self, fmt: str):
        """
        @param fmt: a String containing <SOURCE>, <TARGET> and <TYPE>
        @return a String containing all changes in the specified format
        """
        output = str()
        for change in self:
            buffer = fmt
            buffer = buffer.replace('<SOURCE>', change.source)
            buffer = buffer.replace('<TARGET>', change.target)
            buffer = buffer.replace('<TYPE>', change.type)
            output += (buffer + '\n')
        return output

    def get_size(self):
        """
        @return: The size of the synced files in byte
        """
        size = 0
        for change in self:
            if change.type in ('COPY', 'CONFLICT', 'REPLACE'):
                size += change.source.size
        return size


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Configuration tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--source', type=str, metavar='URI', required=True)
    parser.add_argument('--dst', type=str, metavar='URI', required=True)
    parser.add_argument('--format', type=str, metavar='FORMAT', default='<SOURCE>::<TYPE>::<TARGET>')

    args = parser.parse_args(args)

    database = create_engine(args.database, echo=False)

    session_maker = sessionmaker(bind=database)

    session = session_maker()

    database_logging.DBSession = session_maker()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, handlers=(database_logging.SQLAlchemyHandler(),
                                                           logging.StreamHandler()))
        logger.debug('Tool started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO, handlers=(database_logging.SQLAlchemyHandler(),
                                                          logging.StreamHandler()))

    try:
        source = Folder.by_uri(args.source, session)
        logger.info('Source: %s', source)
        dst = Folder.by_uri(args.dst, session)
        logger.info('Target: %s', dst)

        changes = ChangeSet(source=source, dst=dst, session=session)
        print('%sGB' % (changes.get_size() / 2 ** 30))

    except Exception as e:
        logger.exception(e)
        return -1
    finally:
        session.close_all()
    return 0


if __name__ == '__main__':
    exit(main())
