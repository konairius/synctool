# coding=utf-8
"""
Main Module, creates eventloop parses config and so on...
"""
import argparse
import asyncio
import sys
import config
import database
import database_logging

__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def run(configfile):
    """
    This method is called after initializing the framework and starts the configured servers.
    @param configfile: Path referring to the xml configfile.
    """
    logger.debug('run() method started!')
    loop = asyncio.get_event_loop()
    for role in config.get_roles(configfile):
        loop.call_soon(role)
    logger.debug('run() is done!')

def main(args=sys.argv[1:]):
    """
    main :)
    @param args: the args used for the parser, just useful for testing
    """
    parser = argparse.ArgumentParser(description='Tool MainExecutable')
    parser.add_argument('-d', '--database', type=str, metavar='"Connection String"', required=True)
    parser.add_argument('-c', '--config', type=str, metavar='"Config Path"', default='configfile.xml')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args(args)

    database.DATABASE_STRING = args.database

    database_logging.configure_logger()

    loop = asyncio.get_event_loop()
    loop.call_soon(run, args.config)
    loop.run_forever()

    return 0


if __name__ == '__main__':
    exit(main())