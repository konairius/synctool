__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class Host(Base):
    __tablename__ = 'hosts'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    root = relationship('Folder', primaryjoin="and_(Host.id==Folder.host_id, Folder.name=='<root>')", uselist=False)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<host(%s)>' % self.name


class Folder(Base):
    __tablename__ = 'folders'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    host_id = Column(Integer, ForeignKey('hosts.id'))
    host = relationship('Host')

    parent_id = Column(Integer, ForeignKey('folders.id'))
    parent = relationship('Folder', remote_side=[id, host_id], backref='folders')

    @property
    def path(self):
        if None is self.parent:
            return '/'
        return '%s%s/' % (self.parent.path, self.name)

    @property
    def uri(self):
        return '%s::%s' % (self.host, self.path)

    def __str__(self):
        return self.uri

    def __repr__(self):
        return '<Folder(%s)>' % self.uri


class File(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    hash = Column(String)
    mtime = Column(DateTime)
    host_id = Column(Integer, ForeignKey('hosts.id'))
    host = relationship('Host')

    folder_id = Column(Integer, ForeignKey('folders.id'))
    folder = relationship('Folder', backref=backref('files'))

    @property
    def path(self):
        return '%s%s/' % (self.folder.path, self.name)

    @property
    def uri(self):
        return '%s::%s' % (self.host, self.path)

    def __repr__(self):
        return '<File(%s)>' % self.uri