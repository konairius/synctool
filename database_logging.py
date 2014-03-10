# coding=utf-8
"""
Defines the Database logging
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import logging.handlers
from multiprocessing.context import Process
import pickle
import socket
import socketserver
import traceback
import struct

import database


__author__ = 'konsti'

from sqlalchemy import Column
from sqlalchemy.types import DateTime, Integer, String
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

DBSession = None

LOGGING_POOL = None


def start_logging_process():
    """
    Starts the logging Process that does the database Logging
    """
    tcpserver = LoggingSocketReceiver()
    process = Process(target=tcpserver.serve_until_stopped)
    process.start()
    return process


def configure_logger():
    """
    Sets up the Python logger creates a DatabaseLogger that logs to the database.
    """
    global DBSession, LOGGING_POOL
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    socket_handler = logging.handlers.SocketHandler('localhost',
                                                    logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    root_logger.addHandler(socket_handler)
    logging.info('Logging Started!')


@asyncio.coroutine
def commit():
    """
    Coroutine, commits the DBSession
    """
    DBSession.commit()


class Log(Base):
    """

    @param logger: the name of the logger. (e.g. myapp.views)
    @param level: info, debug, or error?
    @param trace: the full traceback printout
    @param msg: any custom log you may have included
    """
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)  # auto incrementing
    hostname = Column(String, default=socket.gethostname())
    logger = Column(String)  # the name of the logger. (e.g. myapp.views)
    level = Column(String)  # info, debug, or error?
    trace = Column(String)  # the full traceback printout
    msg = Column(String)  # any custom log you may have included
    created_at = Column(DateTime, default=func.now())  # the current timestamp

    def __init__(self, logger=None, level=None, trace=None, msg=None, created_at=None):
        self.logger = logger
        self.level = level
        self.trace = trace
        self.msg = msg
        self.created_at = created_at

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Log: %s - %s>" % (self.created_at.strftime('%m/%d/%Y-%H:%M:%S'), self.msg[:50])


class SQLAlchemyHandler(logging.Handler):
    """
    Basic Database logging Handler
    """

    def emit(self, record):
        """
        @param record: Asynchronously emits logging records
        """
        exc = record.__dict__['exc_info']
        try:
            trace = traceback.format_exc(exc)
        except AttributeError:
            trace = None

        #record = logging.LogRecord()
        log = Log(
            logger=record.name,
            level=record.levelname,
            trace=trace,
            msg=record.getMessage(),
            created_at=datetime.fromtimestamp(record.created))
        DBSession.add(log)
        DBSession.commit()
        #LOGGING_POOL.submit(self._handle, log)

    @staticmethod
    def _handle(log):
        """
        @param log: Log
        """
        DBSession.add(log)
        DBSession.commit()

    def flush(self):
        """
            Commits the log to the database
        """
        DBSession.commit()


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """

    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unpickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handle_log_record(record)

    @staticmethod
    def unpickle(data):
        """
        @param data: the data received from socket
        @return: the unpacked data structure
        """
        return pickle.loads(data)

    def handle_log_record(self, record):
        """
        @param record: logging.LogRecord
        """
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name

        logger = logging.getLogger(name)
        logger.handle(record)


class LoggingSocketReceiver(socketserver.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver suitable for testing.
    """

    allow_reuse_address = 1

    def __init__(self, host='localhost',
                 port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
                 handler=LogRecordStreamHandler):

        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None

    def serve_until_stopped(self):

        """
        Does at it says
        """
        import select

        global LOGGING_POOL, DBSession
        logging.basicConfig(handlers=(logging.StreamHandler(), SQLAlchemyHandler()))
        LOGGING_POOL = ThreadPoolExecutor(max_workers=4)
        DBSession = database.get_session()

        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()], [], [],
                                       self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort