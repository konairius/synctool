#!/usr/bin/env python3
# coding=utf-8
"""
looks in the database if there is something to do,
 if it finds an HashRequest it will try to fulfill it
"""
from _md5 import md5
import argparse
import logging
from multiprocessing import Pool
import socket
import sys
from time import sleep
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from base import HashRequest, File, fix_encoding, Host, Region

__author__ = 'konsti'
logger = logging.getLogger(__name__)


def calculate_hash(ip, port, server_id):
    """
    @param ip: The Server IP
    @param port: The Port the File is served on
    @param server_id: The database server_id
    @return: the hash
    """
    logger.info('Calculating hash for: %s ' % server_id)

    sock = socket.socket()
    sock.connect((ip, port))
    # noinspection PyArgumentList
    sock.send(bytes('%s\n' % server_id, encoding='ascii'))
    sock.settimeout(5000)
    try:
        _hash = md5()
        while True:
            data = sock.recv(2 ** 14)
            _hash.update(data)
            if data == bytes():
                break

    finally:
        sock.close()
    return _hash.hexdigest()


def get_request(session_maker):
    """
    @param session_maker: The SQLAlchemy Session Maker
    @return: a random request guarantied to be locked and unique
    """
    session = session_maker()
    me = Host.by_name(socket.gethostname(), session)
    if me.region is not None:
        # noinspection PyComparisonWithNone
        query = session.query(HashRequest).join(HashRequest.host).join(Host.region).filter(
            and_(HashRequest.locked == False, HashRequest.server != None,
                 Region.id == me.region.id)).with_for_update()
    else:
        # noinspection PyComparisonWithNone
        query = session.query(HashRequest).join(HashRequest.host).filter(
            and_(HashRequest.locked == False, HashRequest.server != None,
                 HashRequest.host == me)).with_for_update()
    try:
        request = query.first()
        if request is None:
            return None
        request.locked = True
        session.commit()
        return request
    except Exception as error:
        session.rollback()
        raise error

    finally:
        session.close()


def work(session_maker):
    """
    This is where the work is done
    @param session_maker: The SQLAlchemy Session Maker
    """
    request = get_request(session_maker)
    session = session_maker()
    if request is None:
        return False
    try:
        request = session.merge(request)
        fhash = calculate_hash(request.server.ip, request.server.port, request.server.id)

        file = File(name=fix_encoding(request.name), folder=request.folder, mtime=request.mtime,
                    size=request.size,
                    host=request.host, hash=fhash)
        session.add(file)
        session.delete(request.server)
        session.delete(request)
        session.commit()
    except Exception as e:
        logger.exception(e)
        session.rollback()

    return True


def daemon(args):
    """
    @param args: Args namespace as returned by argparse
    """

    database = create_engine(args.database, echo=False)
    session_maker = sessionmaker(bind=database)
    while True:
        if not work(session_maker):
            sleep(args.interval)


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Scanner Tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-i', '--interval', type=int, metavar='SECONDS', default=30,
                        help='Interval between two Scan runs, defaults to 1 hour')
    parser.add_argument('-n', '--number', help='Number of processes used', type=int, metavar='NUMBER', default=1)
    args = parser.parse_args(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Hasher started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    with Pool(processes=args.number) as pool:
        for _ in range(args.number):
            pool.apply_async(daemon, args=(args,))
        pool.close()
        pool.join()


if __name__ == '__main__':
    exit(main())
