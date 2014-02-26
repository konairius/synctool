# coding=utf-8
"""
This module contains the static methods for parsing the XML Configfile
"""
from functools import partial

from base import Host
import scanner
import database


__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

HOSTNAME = 'konsti-desktop'


def get_roles(configfile):
    """
    @param configfile: The Path of the xml configfile
    @return a callable that creates the described role
    """
    #TODO: Dummy Implementation

    roles = list()
    #roles.append(partial(hasher.run, 3, 120))

    session = database.get_global_session()

    host = Host.by_name('konsti-desktop', database.get_global_session())
    #folder = host.add_root('/media/konsti/Storage/makeMKV', database.get_global_session())
    folder = host.add_root('/home/konsti/Code', database.get_global_session())
    session.add(host)
    session.add(folder)
    roles.append(partial(scanner.run, folder, 15))

    print(roles)

    return roles