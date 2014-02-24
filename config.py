# coding=utf-8
"""
This module contains the static methods for parsing the XML Configfile
"""
from functools import partial
from base import Folder
import hasher
import scanner

__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)


def get_roles(configfile):
    """
    @param configfile: The Path of the xml configfile
    @return a callable that creates the described role
    """
    #TODO: Dummy Implementation

    roles = list()
    roles.append(partial(hasher.run, 3, 120))


    folder = Folder
    roles.append(partial(scanner.run, 3, 120))

    print(roles)

    return roles