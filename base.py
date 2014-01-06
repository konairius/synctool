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
        return '<Host(id=%s, name=%s)>' % (self.id, self.name)

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

    def add_queue(self):
        return Queue(host_id=self.id)


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
    size = Column(Integer)
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


class Job(Base):
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True)

    source_id = Column(Integer, ForeignKey('folders.id'), nullable=False)
    source = relationship('Folder', foreign_keys=[source_id])

    target_id = Column(Integer, ForeignKey('folders.id'), nullable=False)
    target = relationship('Folder', foreign_keys=[target_id])

    queue_id = Column(Integer, ForeignKey('queues.id'), nullable=False)
    queue = relationship('Queue', backref=backref('jobs'))

    def __str__(self):
        return '<Job(%s ==> %s)>' % (self.source.uri, self.target.uri)

    def __repr__(self):
        return '<Job(%s ==> %s)(id=%s, queue_id=%s)>' % (self.source.uri, self.target.uri, self.id, self.queue_id)


class Queue(Base):
    __tablename__ = 'queues'

    id = Column(Integer, primary_key=True)

    host_id = Column(Integer, ForeignKey('hosts.id'), nullable=False)
    host = relationship('Host', backref=backref('queues'))

    def __repr__(self):
        return '<Queue(id=%s, host_id=%s)>' % (self.id, self.host_id)

    def add_job(self, source, target):
        return Job(queue_id=self.id, source=source, target=target)