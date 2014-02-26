# coding=utf-8
"""
Defines the Database logging
"""
import asyncio
import logging
import socket
import traceback

import database


__author__ = 'konsti'

from sqlalchemy import Column
from sqlalchemy.types import DateTime, Integer, String
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

DBSession = None


def configure_logger():
    """
    Sets up the Python logger creates a DatabaseLogger that logs to the database.
    """
    global DBSession
    DBSession = database.get_session()
    logging.basicConfig(level=logging.DEBUG, handlers=(SQLAlchemyHandler(), logging.StreamHandler()))


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
        @param record: logging.Record
        """
        trace = None
        exc = record.__dict__['exc_info']
        if exc:
            trace = traceback.format_exc(record.exc_info)
        log = Log(
            logger=record.name,
            level=record.levelname,
            trace=trace,
            msg=record.getMessage())
        DBSession.add(log)
        DBSession.commit()
        #asyncio.async(commit())

    def __del__(self):
        DBSession.commit()