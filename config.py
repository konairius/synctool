# coding=utf-8
"""
This module contains the static methods for parsing the XML Configfile
"""
from functools import partial
from time import sleep

__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def get_roles(configfile):
    """
    @param configfile: The Path of the xml configfile
    @return a callable that creates the described role
    """
    return [partial(sleep, 10000), ]