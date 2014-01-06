from sqlalchemy.exc import IntegrityError

__author__ = 'konsti'

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from base import File, Host, Base

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=False)
Session = sessionmaker(bind=database)


class FileTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)

    def test_create(self):
        session = Session()
        host = Host(name='localhost')
        session.add(host)
        root = host.new_root()
        self.assertRaises(RuntimeError, host.new_root)
        dev = root.add_folder('dev')
        file = dev.add_file('null')
        file2 = session.query(File).filter_by(folder=dev, name='null').first()

        self.assertEqual(file, file2)
        session.commit()

        dev.add_file('null')
        self.assertRaises(IntegrityError, session.commit)


class TestJobs(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(database)
        Base.metadata.create_all(database)
        self.session = Session()
        self.host = Host(name='localhost')
        self.session.add(self.host)
        self.session.flush()
        self.queue = self.host.add_queue()
        self.session.add(self.queue)
        self.root = self.host.new_root()
        self.source = self.root.add_folder('source')
        self.target = self.root.add_folder('target')
        self.session.flush()

    def test_add_job(self):
        job = self.queue.add_job(source=self.source, target=self.target)
        self.session.add(job)
        self.session.flush()
        self.session.commit()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
