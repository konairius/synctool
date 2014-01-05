from sqlalchemy.exc import IntegrityError

__author__ = 'konsti'

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from base import File, Host, Base

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=True)
Session = sessionmaker(bind=database)

Base.metadata.drop_all(database)
Base.metadata.create_all(database)


class FileTest(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
