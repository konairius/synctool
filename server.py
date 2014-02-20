#!/usr/bin/env python3
# coding=utf-8
"""
This module searches for files that need to be served to a hasher and does so
"""
import argparse
import logging
import socket
import socketserver
import sys
import threading
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy.orm.exc import NoResultFound

from base import Host, Server


__author__ = 'konsti'
logger = logging.getLogger(__name__)


class TCPRequestHandler(socketserver.StreamRequestHandler):
    """
    The basic request Handler for our Server
    """

    def handle(self):
        """
        Called by super
        """
        logger.info('Serving Request from %s:%s' % self.client_address)
        session = self.server.session_maker()
        try:

            server_id = int(self.rfile.readline().strip())
            server = Server.by_id(server_id, session)
            with open(server.request.path, 'rb') as data:
                for chunk in iter(lambda: data.read(2 ** 14), b''):
                    self.request.sendall(chunk)
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()
            self.request.close()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    Just the tcp server
    """
    pass


def announce_server(request, ip, port, session):
    """
    Creates a database entry to announce the server is severing a file
    @param request: the request that is going to be served
    @param ip: the ip on which the server is serving
    @param port: the port the server will listen on
    @param session: The Session used for Querying
    """
    try:
        session.query(Server).filter(Server.request == request).one()
    except NoResultFound:
        s = Server(ip=ip, port=port, request_id=request.id)
        logger.info('Serving requests: %s' % s)
        return s


def daemon(args):
    """
    @param args: Args namespace as returned by argparse
    """

    database = create_engine(args.database, echo=False)
    session_maker = sessionmaker(bind=database)
    session = session_maker()

    host = session.query(Host).options(subqueryload(Host.requests)).filter(Host.name == socket.gethostname()).first()
    server = ThreadedTCPServer((socket.gethostname(), args.port), TCPRequestHandler)
    server.session_maker = session_maker
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    logger.info('Starting TCPServer in thread: %s' % server_thread.name)
    ip, port = server.server_address

    while True:
        logger.debug('Updating Request list')
        try:
            announces = list()
            for request in host.requests:
                announces.append(announce_server(request, ip, port, session))
            session.add_all(filter(None, announces))
            session.commit()
        except Exception as e:
            logger.exception(e)
            session.rollback()
        sleep(args.interval)
    server.shutdown()


def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Scanner Tool')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-i', '--interval', type=int, metavar='SECONDS', default=360,
                        help='Interval between two Scan runs, defaults to 1 hour')
    parser.add_argument('-p', '--port', type=int, metavar='PORT', default=0)
    args = parser.parse_args(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logger.debug('Server started with following arguments: %s' % args)
    else:
        logging.basicConfig(level=logging.INFO)

    daemon(args)


if __name__ == '__main__':
    exit(main())
