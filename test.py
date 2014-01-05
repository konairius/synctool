__author__ = 'konsti'

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from base import File, Host, Base, Folder

#Setup test database
database = create_engine('sqlite:///test.sqlite', echo=True)
Session = sessionmaker(bind=database)

Base.metadata.drop_all(database)
Base.metadata.create_all(database)


class FileTest(unittest.TestCase):
    def test_create(self):
        session = Session()
        h = Host(name='localhost')
        folder1 = Folder(host=h, name='<root>')
        folder2 = Folder(host=h, parent=folder1, name='dev')
        f = File(folder=folder2, name='null', hash='0')
        session.add(f)
        f2 = session.query(File).filter_by(folder=folder2, name='null').first()
        self.assertEqual(f, f2)
        print(repr(f2.folder))
        print(repr(f2))
        print(folder1.folders)
        print(folder2.files)
        print(repr(h.root))


if __name__ == '__main__':
    unittest.main()
