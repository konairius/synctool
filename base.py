# coding=utf-8
"""
The Base Module where all Classes used by multiple Daemons are specified
"""
from html.parser import HTMLParser
import os
import socket
from sqlalchemy.orm.exc import NoResultFound

__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, and_, DateTime, Boolean, BigInteger, Unicode
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class DBObject(object):
    """
    Baseclass for all database Objects
    """

    def __init__(self, *args, **kwargs):
        """
        Just a dummy
        """
        pass

    id = Column(Integer, primary_key=True)

    # noinspection PyMethodParameters
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def by_id(cls, obj_id, session):
        """
        @param obj_id: id in the database table
        @param session: The Session used for Querying
        @return: the Object
        @raise AttributeError: if the object was not found
        """
        obj = session.query(cls).filter_by(id=obj_id).one()
        if None is obj:
            raise AttributeError('%s has no object with id %s' % (cls.__name__, obj_id))
        logger.debug('Restored Object from Database: %r' % obj)
        return obj


class FilesystemObject(DBObject):
    """
    Baseclass for Filesystem Objects(Folder, File)
    """

    @classmethod
    def by_uri(cls, uri, session):
        """
        @param uri: <hostname>::<path>
        @param session: The Session used for Querying
        @return: The demanded object
        @raise AttributeError: if the uri is invalid
        """
        if not '::' in uri:
            raise AttributeError('%s is not a valid URI' % uri)
        hostname, path = uri.split(sep='::', maxsplit=1)
        host = Host.by_name(hostname, session=session)
        obj = host.descendant_by_path(path, session=session)
        logger.debug('Restored Object from Database: %r' % obj)
        return obj


class Region(Base, DBObject):
    """
    Represents a "Region" a group of hosts with good connectivity
    """
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False, unique=True)

    @classmethod
    def create_new(cls, name):
        """
        Creates a new Region
        @param name: The name of the new host
        """
        return cls(name=name)


    @classmethod
    def by_name(cls, name, session):
        """
        @param session: The Session used for Querying
        @param name: the hostname you're looking for
        @return: the host you're looking for
        @raise NoResultFound: when the host doesn't exist
        """
        try:
            obj = session.query(cls).filter(cls.name == name).one()
        except Exception as e:
            logger.debug('<%s>: %s' % (cls.__name__, e))
            raise e
        return obj

    def __repr__(self):
        return '<Region(id=%s, name=%s)>' % (self.id, self.name)


class Host(Base, DBObject):
    """
    Database representation of a host
    """
    name = Column(Unicode, nullable=False, unique=True)
    region_id = Column(Integer, ForeignKey('region.id'))
    region = relationship('Region', backref=backref('hosts'))

    roots = relationship('Folder', primaryjoin="and_(Host.id==Folder.host_id, Folder.parent_id==None)")

    @property
    def is_local(self):
        """
        @return: True if the hostname is the local one
        """
        if socket.gethostname() != self.name:
            return False
        return True

    @classmethod
    def create_new(cls, name, region):
        """
        Creates a new Host
        @param name: The name of the new host
        @param region: The region it is in
        """
        return cls(name=name, region=region)


    @classmethod
    def by_name(cls, name, session):
        """
        @param session: The Session used for Querying
        @param name: the hostname you're looking for
        @return: the host you're looking for
        @raise NoResultFound: when the host doesn't exist
        """
        try:
            obj = session.query(cls).filter(cls.name == name).one()
        except Exception as e:
            logger.debug('<%s>: %s' % (cls.__name__, e))
            raise e
        return obj

    def descendant_by_path(self, path, session):
        """
        This will try to find the root closets to the target,
        if it didn't find a valid root, it will try to create one,
        witch will fail if path is not local.
        @param session: The Session used for Querying
        @param path: the path you're looking for
        @return: the FilesystemObject you're looking for
        """
        best = None
        best_score = 0
        for root in self.roots:
            if root.name == path:
                return root
            score = _matching_chars(path, root.path)
            if score > best_score:
                best = root
                best_score = score

        if best is None:
            raise NoResultFound('%s is not in a valid root of %s' % (path, self))

        rel_path = path[len(best.path):]
        while best.path != path and rel_path is not None and rel_path != os.sep and rel_path != '':
            if os.sep in rel_path:
                name, rel_path = tuple(rel_path.split(sep=os.sep, maxsplit=1))
            else:
                name, rel_path = rel_path, None
            best = best.child_by_name(name, session=session)
            if best is None:
                raise NoResultFound('%s is not in %s' % (path, self))
        return best

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Host(id=%s, name=%s)>' % (self.id, self.name)

    def add_root(self, path, session):
        """
        @param path: The path of the now root
        @param session: The Session used for Querying
        @return: The New created root(Folder)
        @raise AttributeError: If the folder already exist
        """
        if not path[-1] == os.sep:
            path += os.sep

        old_root = session.query(Folder).filter(
            and_(Folder.name == path, Folder.parent_id is None, Folder.host_id == self.id)).first()

        if old_root is not None:
            raise AttributeError('%s already exist' % old_root)

        return Folder(name=path, host=self)

    def remove_root(self, path, session):
        """
        @param path: The path of the root you want to remove
        @param session: The Session used for Querying
        @raise AttributeError: if the root doesnt exit
        """
        if not path[-1] == os.sep:
            path += os.sep

        # noinspection PyComparisonWithNone
        old_root = session.query(Folder).filter(
            and_(Folder.name == path, Folder.host_id == self.id, Folder.parent_id == None)).first()
        if old_root is None:
            raise AttributeError('%s doesnt exist' % path)
        old_root.delete()


class Folder(Base, FilesystemObject):
    """
    Represents a Folder, Surprise
    """
    __table_args__ = (
        UniqueConstraint('parent_id', 'host_id', 'name'),
    )
    #id is the same in baseclass but for some reason strange things are happening, so we need this
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)

    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host', cascade='all')

    parent_id = Column(Integer, ForeignKey('folder.id'))
    parent = relationship('Folder', remote_side=[id, host_id], backref=backref('folders', lazy='subquery'),
                          cascade='all', lazy='subquery')

    @property
    def path(self):
        """
        @return: the path, honorees the local path separator
        """
        if None is self.parent:
            return self.name
        return '%s%s%s' % (self.parent.path, restore_utf8(self.name), os.sep)

    @property
    def uri(self):
        """
        @return:the uri(<hostname>::<path>
        """
        return '%s::%s' % (self.host.name, self.path)

    @property
    def size(self):
        """
        @return: the size of the underlying structure
        """
        result = 0
        for file in self.files:
            result += file.size
        for folder in self.folders:
            result += folder.size
        return result

    def __str__(self):
        return self.uri

    def __repr__(self):
        return '<Folder(%s)>' % self.uri

    def add_folder(self, name):
        """
        Creates a Folder as child of the object
        @param name: the name of the Folder
        @return: the new created Folder
        """
        folder = Folder(parent=self, name=name, host=self.host)
        logger.debug('Created new Object: %r' % folder)
        return folder

    def add_file(self, name, fhash, mtime, size):
        """
        Creates a File as child of the object
        @param size: Integer in Byte
        @param mtime: datetime.datetime
        @param fhash: hexdigested md5 hash
        @param name: the name of the File
        @return: the new created File
        """
        file = File(folder=self, name=name, hash=fhash, mtime=mtime, size=size, host=self.host)
        logger.debug('Created new Object: %r' % file)
        return file

    def child_by_name(self, name, session, eager=False):
        """
        @param session: The Session used for Querying
        @param name: the name
        @param eager: Ignored for now
        @return: The File or folder
        """
        obj = session.query(File).filter(and_(File.name == name, File.folder_id == self.id)).with_lockmode(None).first()
        if None is obj:
            obj = session.query(Folder).filter(
                and_(Folder.name == name, Folder.parent_id == self.id)).with_lockmode(None).first()

        if obj is not None:
            logger.debug('Restored Object from Database: %r' % obj)
        else:
            logger.debug('Failed to find object with name: %s' % name)

        return obj


class File(Base, FilesystemObject):
    """
    Represents a File
    """
    __table_args__ = (
        UniqueConstraint('folder_id', 'host_id', 'name'),
    )
    #id is the same in baseclass but for some reason strange things are happening, so we need this
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    hash = Column(String, nullable=False)
    mtime = Column(DateTime, nullable=False)
    size = Column(BigInteger, nullable=False)
    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host')

    folder_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    folder = relationship('Folder', backref=backref('files', lazy='subquery'), lazy='subquery')

    @property
    def path(self):
        """
        @return: the path, honorees the local path separator
        """
        if self.folder is None:
            return restore_utf8(self.name)
        return '%s%s' % (self.folder.path, restore_utf8(self.name))

    @property
    def uri(self):
        """
        @return:the uri(<hostname>::<path>
        """
        return '%s::%s' % (self.host, self.path)

    def __repr__(self):
        return '<File(%s)>' % self.uri


class Server(Base, DBObject):
    """
    Entry a Server creates when is starts serving a file and removes when it gets accepted
    """
    ip = Column(String, nullable=False)
    port = Column(Integer, nullable=False)

    request_id = Column(Integer, ForeignKey('hashrequest.id'), nullable=False)
    request = relationship('HashRequest', backref=backref('server', uselist=False))

    def __repr__(self):
        return '<Server(ip=%s, port=%s, request_id=%s)>' % (self.ip, self.port, self.request_id)


class HashRequest(Base, DBObject):
    """
    Represents a hash request, generate by a scanner, it will be fulfilled by a harsher and a server
    """

    name = Column(Unicode, nullable=False)
    mtime = Column(DateTime, nullable=False)
    size = Column(BigInteger, nullable=False)

    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host', backref=backref('requests'))

    folder_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    folder = relationship('Folder')

    locked = Column(Boolean, default=False)

    def __repr__(self):
        return '<HashRequest(id=%s, host_id=%s, locked=%s)>' % (self.id, self.host_id, self.locked)

    @property
    def path(self):
        """
        @return: the path, honorees the local path separator
        """
        return '%s%s' % (self.folder.path, restore_utf8(self.name))


def _matching_chars(left, right):
    index = 0
    for char in left:
        try:
            if right[index] == char:
                index += 1
            else:
                break
        except IndexError:
            break
    return index


def fix_encoding(string, method='xmlcharrefreplace'):
    """
    @param string: The String you want to escape
    @param method: the method used to correct the string, valid Values are:
                   'ignore', 'replace', 'backslashreplace', 'xmlcharrefreplace'
    @return: The escaped String
    """
    assert method in ('ignore', 'replace', 'backslashreplace', 'xmlcharrefreplace'), 'invalid removal method'
    return string.encode('utf-8', method).decode('utf-8')


def restore_utf8(string):
    """
    @param string: a String with encoded with 'xmlcharrefreplace'
    @return: The Original UTF-8 String
    """
    parser = HTMLParser()
    return parser.unescape(string)