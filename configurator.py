#!/usr/bin/env python3
# coding=utf-8
"""
This module is used to configure the host in the database it creates host entry and configures root folders
"""
import argparse
import logging
import socket
import sys

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from base import Base, set_session, Host


__author__ = 'konsti'

logger = logging.getLogger(__name__)


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Configuration tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--add', dest='add', type=str, metavar='URI',
                        help='The directory you want to add to the Database', action='append')
    parser.add_argument('--remove', dest='remove', type=str, metavar='URI',
                        help='The root you want to remove from the Database', action='append')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--create_schema', action='store_true')
    parser.add_argument('--clear_database', action='store_true')
    args = parser.parse_args(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Configurator started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    database = create_engine(args.database, echo=False)

    if args.clear_database:
        Base.metadata.drop_all(database)

    if args.create_schema or args.clear_database:
        Base.metadata.create_all(database)

    session = sessionmaker(bind=database)()
    set_session(session)

    try:
        if args.add is not None:
            host = Host.by_name(socket.gethostname())
            for root_dir in args.add:
                try:
                    host.add_root(root_dir)
                except AttributeError as error:
                    logger.error(error)

        if args.remove is not None:
            host = Host.by_name(socket.gethostname())
            for root_dir in args.remove:
                try:
                    host.remove_root(root_dir)
                except AttributeError as error:
                    logger.error(error)

    except OperationalError as error:
        logger.error(error)
        session.rollback()
        return -1

    session.commit()
    session.close_all()
    return 0


if __name__ == '__main__':
    exit(main())
