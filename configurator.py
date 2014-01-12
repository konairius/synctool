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
from sqlalchemy.orm.exc import NoResultFound

from base import Base, Host, fix_encoding, Region


__author__ = 'konsti'

logger = logging.getLogger(__name__)


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Configuration tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('-n', '--name', type=str, metavar='"HOSTNAME"', default=socket.gethostname())
    parser.add_argument('-r', '--region', type=str, metavar='"HOSTNAME"', default=None)
    parser.add_argument('--add', dest='add', type=str, metavar='PATH',
                        help='The directory you want to add to the Database', action='append')
    parser.add_argument('--remove', dest='remove', type=str, metavar='PATH',
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

    if args.region is not None:
        try:
            region = Region.by_name(name=args.region.lower(), session=session)
        except NoResultFound:
            region = Region.create_new(name=args.region.lower())
            session.add(region)
    else:
        region = None

    try:
        host = Host.by_name(args.name, session)
    except NoResultFound:
        logger.info('Host not found, creating new One')
        host = Host.create_new(name=args.name, region=region)
        session.add(host)

    if host.region != region:
        host.region = region

    if args.add is not None:
        for root_dir in args.add:
            root_dir = fix_encoding(root_dir)
            try:
                root = host.add_root(root_dir, session)
                session.add(root)
            except AttributeError as error:
                logger.error(error)

    if args.remove is not None:
        for root_dir in args.remove:
            root_dir = fix_encoding(root_dir)
            try:
                host.remove_root(root_dir, session)
            except AttributeError as error:
                logger.error(error)

    try:
        session.commit()
    except OperationalError as error:
        logger.error(error)
        session.rollback()
        return -1
    finally:
        session.close_all()
    return 0


if __name__ == '__main__':
    exit(main())
