__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint, create_engine
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base()

global_session = None


def session():
    global global_session
    if global_session is None:
        logger.warning('Database Session not set: Creating Dummy Session!')
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(engine)
        s = sessionmaker(bind=engine)
        global_session = s()
    return global_session


def set_session(new_session):
    logger.info('Database Session set to: %s' % new_session.get_bind())
    global global_session
    global_session = new_session


class DBObject(object):
    def __init__(self, *args, **kwargs):
        pass

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)


class Host(Base, DBObject):
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
        root = Folder(name='<root>', host=self)
        session().add(root)
        session().flush()
        self.root = root
        return self.root

    def add_queue(self):
        queue = Queue(host_id=self.id)
        session().add(queue)
        session().flush()

        return queue


class Folder(Base, DBObject):
    __table_args__ = (
        UniqueConstraint('parent_id', 'name'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host', cascade='all')

    parent_id = Column(Integer, ForeignKey('folder.id'))
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
        folder = Folder(parent=self, name=name, host=self.host)
        session().add(folder)
        session().flush()
        return folder

    def add_file(self, name):
        file = File(folder=self, name=name, host=self.host)
        session().add(file)
        session().flush()
        return file


class File(Base, DBObject):
    __table_args__ = (
        UniqueConstraint('folder_id', 'name'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    hash = Column(String)
    mtime = Column(DateTime)
    size = Column(Integer)
    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host', cascade='all')

    folder_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    folder = relationship('Folder', backref=backref('files'), cascade='all')

    @property
    def path(self):
        return '%s%s/' % (self.folder.path, self.name)

    @property
    def uri(self):
        return '%s::%s' % (self.host, self.path)

    def __repr__(self):
        return '<File(%s)>' % self.uri


class Job(Base, DBObject):
    source_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    source = relationship('Folder', foreign_keys=[source_id])

    target_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    target = relationship('Folder', foreign_keys=[target_id])

    queue_id = Column(Integer, ForeignKey('queue.id'), nullable=False)
    queue = relationship('Queue', backref=backref('jobs'))

    def __str__(self):
        return '<Job(%s ==> %s)>' % (self.source.uri, self.target.uri)

    def __repr__(self):
        return '<Job(%s ==> %s)(id=%s, queue_id=%s)>' % (self.source.uri, self.target.uri, self.id, self.queue_id)


class Queue(Base, DBObject):
    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host', backref=backref('queues'))

    def __repr__(self):
        return '<Queue(id=%s, host_id=%s)>' % (self.id, self.host_id)

    def add_job(self, source, target):
        job = Job(queue_id=self.id, source=source, target=target)
        session().add(job)
        session().flush()
        return job