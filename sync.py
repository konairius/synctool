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
from sqlalchemy.orm.exc import NoResultFound

from base import Folder, File


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
                obj = session.query(File).filter(and_(File.hash == file.hash, File.host == dst.host)).first()
                if obj is None:
                    changes.append(Change(type='COPY', source=file.uri, target='%s%s' % (dst.uri, file.name)))
                else:
                    changes.append(Change(type='LCOPY', source=obj.uri, target='%s%s' % (dst.uri, file.name)))

            elif file.hash == dst_file.hash:
                pass
            else:
                if file.mtime > dst_file.mtime:
                    changes.append(Change(type='REPLACE', source=file.uri, target=dst_file.uri))
                else:
                    changes.append(Change(type='CONFLICT', source=file.uri, target=dst_file.uri))

        for folder in source.folders:
            dst_folder = dst.child_by_name(folder.name, session)
            if dst_folder is None:
                changes.append(Change(type='COPY', source=folder.uri, target='%s%s' % (dst.uri, folder.name)))
            else:
                changes += ChangeSet._get_changes(source=folder, dst=dst_folder, session=session)

        for file in dst.files:
            src_file = source.child_by_name(file.name, session)
            if src_file is None:
                changes.append(Change(type='DELETE', source='', target=file.uri))

        for folder in dst.folders:
            src_folder = source.child_by_name(folder.name, session)
            if src_folder is None:
                changes.append(Change(type='DELETE', source='', target=folder.uri))

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

    def get_size(self, session):
        """
        @param session: The Session used for Querying
        @return: The size of the synced files in byte
        """
        size = 0
        for change in self:
            if change.type in ('COPY', 'CONFLICT', 'REPLACE'):
                size += Folder.by_uri(change.source, session).size
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
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Tool started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    database = create_engine(args.database, echo=False)

    session_maker = sessionmaker(bind=database)

    session = session_maker()

    try:
        source = Folder.by_uri(args.source, session)
        logger.info('Source: %s', source)
        dst = Folder.by_uri(args.dst, session)
        logger.info('Target: %s', dst)

        changes = ChangeSet(source=source, dst=dst, session=session)
        print('%sGB' % (changes.get_size(session)/2**30))

    except Exception as e:
        logger.exception(e)
        return -1
    finally:
        session.close_all()
    return 0


if __name__ == '__main__':
    exit(main())
