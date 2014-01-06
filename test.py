__author__ = 'konsti'

import logging

logging.basicConfig(level=logging.DEBUG)

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from base import File, Host, Base, set_session

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=False)
Session = sessionmaker(bind=database)


class FileTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)

    def test_create(self):
        session = Session()
        set_session(session)
        host = Host(name='localhost')
        root = host.new_root()
        self.assertIsNotNone(root)
        self.assertRaises(RuntimeError, host.new_root)
        dev = root.add_folder('dev')
        file = dev.add_file('null')
        file2 = session.query(File).filter_by(folder=dev, name='null').first()

        self.assertEqual(file, file2)

        self.assertRaises(IntegrityError, dev.add_file, 'null')


class TestJobs(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        set_session(self.session)
        self.host = Host(name='localhost')
        self.session.add(self.host)
        self.session.flush()
        self.queue = self.host.add_queue()
        self.root = self.host.new_root()
        self.source = self.root.add_folder('source')
        self.target = self.root.add_folder('target')

    def test_add_job(self):
        job = self.queue.add_job(source=self.source, target=self.target)

    def tearDown(self):
        self.session.commit()


if __name__ == '__main__':
    unittest.main()
