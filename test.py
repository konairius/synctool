from datetime import datetime
import socket

__author__ = 'konsti'

import logging

logging.basicConfig(level=logging.DEBUG)

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from base import File, Host, Base, set_session, Job, Folder

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=False)
#database = create_engine('postgres://konsti:slojit@fileserver.local/uranos', echo=False)

Session = sessionmaker(bind=database)


class FileTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        set_session(self.session)

    def test_create(self):
        host = Host.by_name(name=socket.gethostname())
        root = host.add_root('/')
        host.add_root('C:/')
        self.assertIsNotNone(root)
        dev = root.add_folder('dev')
        file = dev.add_file(name='null', fhash=1, mtime=datetime.now(), size=3)
        file2 = File.by_id(file.id)
        self.assertEqual(file, file2)
        dev2 = Folder.by_uri('%s::/dev' % socket.gethostname())
        self.assertEqual(dev, dev2)
        file3 = File.by_uri('%s::/dev/null' % socket.gethostname())
        self.assertEqual(file, file3)
        file.delete()
        root2 = Folder.by_uri('%s::C:/' % socket.gethostname())
        self.assertEqual(root2.path, 'C:/')

        dev.add_file(name='null', fhash=1, mtime=datetime.now(), size=3)

        self.assertRaises(IntegrityError, dev.add_file, 'null', 1, datetime.now(), 3)

    # @staticmethod
    # def test_real():
    #     host = Host.by_name('konsti-desktop')
    #     code = host.add_root('/home/konsti/Code')
    #     scan(code)

    def tearDown(self):
        #self.session.commit()
        self.session.close_all()


class TestJobs(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        set_session(self.session)
        self.host = Host.by_name(name=socket.gethostname())
        self.session.flush()
        self.queue = self.host.add_queue()
        self.root = self.host.add_root('/')
        self.source = self.root.add_folder('source')
        self.target = self.root.add_folder('target')

    def test_add_job(self):
        job = self.queue.add_job(source=self.source, target=self.target)
        job2 = Job.by_id(job.id)
        self.assertEqual(job, job2)

    def tearDown(self):
        self.session.commit()


if __name__ == '__main__':
    unittest.main()
