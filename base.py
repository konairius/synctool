__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class Host(Base):
    __tablename__ = 'hosts'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    root = relationship('Folder', primaryjoin="and_(Host.id==Folder.host_id, Folder.name=='<root>')", uselist=False,
                        cascade='all')

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<host(%s)>' % self.name

    def new_root(self):
        """
        returns the new folder
        Creates a new root folder for this host,
        it will raise an Runtime exception if root already exists.
        """

        if not self.root is None:
            raise RuntimeError('%r already has a root' % self)
        self.root = Folder(name='<root>', host=self)
        return self.root


class Folder(Base):
    __tablename__ = 'folders'
    __table_args__ = (
        UniqueConstraint('parent_id', 'name'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    host_id = Column(Integer, ForeignKey('hosts.id'), nullable=False)
    host = relationship('Host', cascade='all')

    parent_id = Column(Integer, ForeignKey('folders.id'))
    parent = relationship('Folder', remote_side=[id, host_id], backref='folders', cascade='all')

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

    def add_folder(self, name):
        return Folder(parent=self, name=name, host=self.host)

    def add_file(self, name):
        return File(folder=self, name=name, host=self.host)


class File(Base):
    __tablename__ = 'files'
    __table_args__ = (
        UniqueConstraint('folder_id', 'name'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    hash = Column(String)
    mtime = Column(DateTime)
    host_id = Column(Integer, ForeignKey('hosts.id'), nullable=False)
    host = relationship('Host', cascade='all')

    folder_id = Column(Integer, ForeignKey('folders.id'), nullable=False)
    folder = relationship('Folder', backref=backref('files'), cascade='all')

    @property
    def path(self):
        return '%s%s/' % (self.folder.path, self.name)

    @property
    def uri(self):
        return '%s::%s' % (self.host, self.path)

    def __repr__(self):
        return '<File(%s)>' % self.uri