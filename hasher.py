#!/usr/bin/env python3
# coding=utf-8
"""
looks in the database if there is something to do,
 if it finds an HashRequest it will try to fulfill it
"""
from _md5 import md5
import argparse
import asyncio
import logging
import socket
import sys

from sqlalchemy import and_, func

import database
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


def get_request():
    """
    @return: a random request guarantied to be locked and unique
    """
    session = database.get_session()
    me = Host.by_name(socket.gethostname(), session)
    if me.region is not None:
        # noinspection PyComparisonWithNone,PyPep8
        query = session.query(HashRequest).join(HashRequest.host).join(Host.region).filter(
            and_(HashRequest.locked == False, HashRequest.server != None,
                 Region.id == me.region.id)).order_by(func.random()).with_for_update()
    else:
        # noinspection PyComparisonWithNone,PyPep8
        query = session.query(HashRequest).join(HashRequest.host).filter(
            and_(HashRequest.locked == False, HashRequest.server != None,
                 HashRequest.host == me)).order_by(func.random()).with_for_update()
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


def work(interval):
    """
    This is where the work is done
    @param interval: The interval between two calls if there are no request
    """
    logger.debug('Worker Running!')
    loop = asyncio.get_event_loop()
    session = database.get_session()
    request = get_request()
    if request is None:
        loop.call_later(interval, work, interval)
        logger.debug('Waiting for Request, check again in %s seconds.' % interval)
        return False
    try:
        logger.info('Calculating Hash for: %s' % request)
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

    loop.call_soon(work, interval)
    return True


def run(number, interval):

    """
    @param number: Number of Processes to spawn
    @param interval: Time to wait between checking for new Requests
    """
    logger.debug('Hasher running with:(number:%s, interval:%s)' % (number, interval))
    loop = asyncio.get_event_loop()
    for _ in range(number):
        loop.call_soon(work, interval)


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

if __name__ == '__main__':
    exit(main())
