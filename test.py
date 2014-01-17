# coding=utf-8
"""
My unittest
"""
import os
from tempfile import mkdtemp
import configurator

__author__ = 'konsti'

import logging

logging.basicConfig(level=logging.DEBUG)

import unittest

#Setup test database
#database = create_engine('sqlite:///test.sqlite', echo=False)
#database = create_engine('postgres://konsti:slojit@localhost/uranos', echo=False)


class ConfiguratorTest(unittest.TestCase):
    def setUp(self):
        self.tempfile = '%s/unittest.db' % mkdtemp()
        self.cs = 'sqlite:///%s' % self.tempfile

    def test_do_noting(self):
        args = ['--debug', '--database', self.cs]
        result = configurator.main(args=args)
        self.assertEqual(result, 0)

    def test_add_root_without_schema(self):
        args = ['--debug', '--database', self.cs, '--add', '/tmp']
        result = configurator.main(args=args)
        self.assertEqual(result, -1)

    def test_create_schema(self):
        args = ['--debug', '--database', self.cs, '--create_schema']
        result = configurator.main(args=args)
        self.assertEqual(result, 0)

    def test_add_root(self):
        args = ['--debug', '--database', self.cs, '--create_schema', '--add', '/tmp/\udcdf']
        result = configurator.main(args=args)
        self.assertEqual(result, 0)

    def test_add_and_remove_root(self):
        args = ['--debug', '--database', self.cs, '--create_schema', '--add', '/tmp']
        result = configurator.main(args=args)
        self.assertEqual(result, 0)
        args = ['--debug', '--database', self.cs, '--create_schema', '--remove', '/tmp']
        result = configurator.main(args=args)
        self.assertEqual(result, 0)

    def test_remove_non_existent(self):
        args = ['--debug', '--database', self.cs, '--create_schema', '--remove', '/tmp']
        result = configurator.main(args=args)
        self.assertEqual(result, 0)

    def tearDown(self):
        try:
            os.remove(self.tempfile)
        except FileNotFoundError:
            pass


if __name__ == '__main__':
    unittest.main()
