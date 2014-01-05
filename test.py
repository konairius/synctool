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
        print(repr(host))
        root = host.new_root()
        dev = root.add_folder('dev')
        file = dev.add_file('null')
        session.add(file)
        file2 = session.query(File).filter_by(folder=dev, name='null').first()
        self.assertEqual(file, file2)
        session.commit()


if __name__ == '__main__':
    unittest.main()
