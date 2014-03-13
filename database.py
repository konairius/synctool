# coding=utf-8
"""
Contains Global Database settings and convenience functions
"""
import threading

import sqlalchemy


__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

DATABASE_STRING = None

_DATABASE = None

GLOBAL_SESSION = None
GLOBAL_SESSION_LOCK = threading.Lock()


def get_database():
    """
    @return A Instance of the database defined in DATABASE_STRING
    @raise RuntimeError: If DATABASE_STRING is not set or invalid
    """

    global _DATABASE

    if _DATABASE is None:
        _DATABASE = sqlalchemy.create_engine(DATABASE_STRING, echo=False, pool_size=100)
    return _DATABASE


def get_session(new_engine=False):
    """
    @param new_engine: If true a new Engine is created from DATABASE_STRING
    @return A session for the current Database
    @raise RuntimeError: If DATABASE_STRING is not set or invalid
    """
    if new_engine:
        engine = sqlalchemy.create_engine(DATABASE_STRING, echo=False, pool_size=100)
        session_maker = sqlalchemy.orm.sessionmaker(bind=engine)
    else:
        session_maker = sqlalchemy.orm.sessionmaker(bind=get_database())
    return session_maker()


def get_global_session():
    """
    @return: A globally shared Session
    """
    global GLOBAL_SESSION
    global GLOBAL_SESSION_LOCK
    with GLOBAL_SESSION_LOCK:
        if None is GLOBAL_SESSION:
            GLOBAL_SESSION = get_session()
    return GLOBAL_SESSION