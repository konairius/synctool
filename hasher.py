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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import set_session, session, HashRequest, File

__author__ = 'konsti'
logger = logging.getLogger(__name__)


def calculate_hash(request):
    """
    @param request: The HashRequest MUST be locked in the database
    """
    if not request.locked:
        raise RuntimeError('%s is not locked' % request)

    logger.info('Calculating hash for: %s ' % request)

    sock = socket.socket()
    sock.connect((request.server.ip, request.server.port))
    sock.send(bytes('%s\n' % request.server.id, encoding='ascii'))
    sock.settimeout(5)
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


def get_request():
    """
    @return: a random request guarantied to be locked and unique
    """
    try:
        for request in session().query(HashRequest).filter(HashRequest.locked == False).with_for_update():
            if request.server is not None:
                request.locked = True
                session().commit()
                return request
    except Exception as e:
        logger.error(e)


def work():
    """
    This is where the work is done
    """
    request = get_request()
    if request is None:
        return
    fhash = calculate_hash(request)

    file = File(name=request.name, folder=request.folder, mtime=request.mtime, size=request.size,
                host=request.host, hash=fhash)

    session().add(file)
    session().flush()
    request.server.delete()
    request.delete()
    session().commit()


def daemon(interval, database):
    """
    @param database: The database used for Communication
    @param interval: Integer, seconds between to scan runs
    """

    database = create_engine(database, echo=False)
    s = sessionmaker(bind=database)()
    set_session(s)
    while True:
        work()
        s.commit()
        sleep(interval)


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

    #daemon(args.interval, args.database)

    with Pool(processes=args.number) as pool:
        for _ in range(args.number):
            pool.apply_async(daemon, args=(args.interval, args.database))
        pool.close()
        pool.join()


if __name__ == '__main__':
    exit(main())
