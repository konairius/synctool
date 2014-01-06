__author__ = 'konsti'

import logging

logger = logging.getLogger(__name__)

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint, create_engine, Float
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base()

global_session = None


def session():
    global global_session
    if global_session is None:
        logger.warning('Database Session not set: Creating Dummy Session!')
        engine = create_engine('sqlite:///:memory:', echo=False)
        #engine = create_engine('sqlite:///test.sqlite', echo=False)
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

    @classmethod
    def by_id(cls, obj_id):
        obj = session().query(cls).filter_by(id=obj_id).first()
        if None is obj:
            raise RuntimeError('%s has no object with id %s' % (cls.__name__, obj_id))
        logger.debug('Restored Object from Database: %r' % obj)
        return obj

    def delete(self):

        session().delete(self)
        session().flush()
        logger.debug('Deleted Object: %r' % self)


class Host(Base, DBObject):
    name = Column(String, nullable=False, unique=True)

    _root = relationship('Folder', primaryjoin="and_(Host.id==Folder.host_id, Folder.name=='<root>')", uselist=False,
                         cascade='all')

    @property
    def root(self):
        if self._root is None:
            self._root = Folder(name='<root>', host=self)
            session().add(self._root)
            session().flush()
        return self._root

    @classmethod
    def by_name(cls, name):
        obj = session().query(cls).filter_by(name=name).first()
        if None is obj:
            obj = Host(name=name)
            session().add(obj)
            session().flush()
            logger.debug('Created new Object: %r' % obj)
        else:
            logger.debug('Restored Object from Database: %r' % obj)
        return obj

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Host(id=%s, name=%s)>' % (self.id, self.name)

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
        logger.debug('Created new Object: %r' % folder)
        return folder

    def add_file(self, name, fhash, mtime, size):
        file = File(folder=self, name=name, hash=fhash, mtime=mtime, size=size, host=self.host)
        session().add(file)
        session().flush()
        logger.debug('Created new Object: %r' % file)
        return file

    def child_by_name(self, name):
        obj = session().query(File).filter_by(name=name, folder_id=self.id).first()
        if None is obj:
            obj = session().query(Folder).filter_by(name=name, parent_id=self.id).first()
        if obj is not None:
            logger.debug('Restored Object from Database: %r' % obj)
        else:
            logger.debug('Failed to find object with name: %s' % name)
        return obj


class File(Base, DBObject):
    __table_args__ = (
        UniqueConstraint('folder_id', 'name'),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    hash = Column(String)
    mtime = Column(Float)
    size = Column(Integer)
    host_id = Column(Integer, ForeignKey('host.id'), nullable=False)
    host = relationship('Host')

    folder_id = Column(Integer, ForeignKey('folder.id'), nullable=False)
    folder = relationship('Folder', backref=backref('files'))

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
        logger.debug('Created new Object: %r' % job)
        return job