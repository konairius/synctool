from scannerDeamon import scan

__author__ = 'konsti'

import logging

logging.basicConfig(level=logging.DEBUG)

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from base import File, Host, Base, set_session, Job

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=False)
Session = sessionmaker(bind=database)


class FileTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        set_session(self.session)

    def test_create(self):
        host = Host.by_name(name='localhost')
        root = host.root
        self.assertIsNotNone(root)
        dev = root.add_folder('dev')
        file = dev.add_file(name='null', fhash=1, mtime=2.0, size=3)
        file2 = File.by_id(file.id)
        self.assertEqual(file, file2)

        file.delete()

        file = dev.add_file(name='null', fhash=1, mtime=2.0, size=3)

        self.assertRaises(IntegrityError, dev.add_file, 'null', 1, 2.0, 3)

    def test_real(self):
        host = Host.by_name('konsti-desktop')
        code = host.root.add_folder('home').add_folder('konsti').add_folder('Code')
        scan(code)

    def tearDown(self):
        self.session.close_all()


class TestJobs(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        set_session(self.session)
        self.host = Host.by_name(name='localhost')
        self.session.flush()
        self.queue = self.host.add_queue()
        self.root = self.host.root
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
