# coding=utf-8
"""
Defines the Database logging
"""
import logging
import socket
import traceback

__author__ = 'konsti'

from sqlalchemy import Column
from sqlalchemy.types import DateTime, Integer, String
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

DBSession = None


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

    def __init__(self, logger=None, level=None, trace=None, msg=None):
        self.logger = logger
        self.level = level
        self.trace = trace
        self.msg = msg

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
            trace = traceback.fromat_exc(exc)
        log = Log(
            logger=record.__dict__['name'],
            level=record.__dict__['levelname'],
            trace=trace,
            msg=record.__dict__['msg'])
        DBSession.add(log)
        if log.level == 'ERROR':
            DBSession.commit()

    def __del__(self):
        DBSession.commit()