#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unittests for sessionization
"""
import unittest
import os
import hashlib

from src import sessionization


class SessionizationTest(unittest.TestCase):

    def setUp(self):
        """
        Initializes basic sessionization class to test with.
        :return:
        """
        self.log_file = './input/log.csv'
        self.inactivity_period = './input/inactivity_period.txt'
        self.output_file = './output/sessionization.txt'
        self.sessionization = sessionization.Sessionization(log_file=self.log_file,
                                                            inactivity_period=self.inactivity_period,
                                                            output_file=self.output_file)

    def test_basic_run(self):
        """
        Tests if sessionization runs on basic example.
        """
        self.sessionization.run()
        self.output_file = './output/sessionization.txt'
        self.log_file = './input/log.csv'
        self.sessionization = sessionization.Sessionization(log_file=self.log_file,
                                                            inactivity_period=self.inactivity_period,
                                                            output_file=self.output_file)
        self.sessionization.run()

    def test_inactivity_times(self):
        """
        Tests different sessionization inactivity periods.
        :return:
        """
        def read_file(fl):
            with open(fl, 'rb') as file:
                return file.read()

        for inactivity_period in range(1, 5):
            fh = open("tmp", 'w')
            fh.write('{}'.format(inactivity_period))
            fh.close()
            self.sessionization = sessionization.Sessionization(log_file=self.log_file,
                                                                inactivity_period='tmp',
                                                                output_file='./output/{}.csv'.format(inactivity_period))
            self.sessionization.run()
        os.remove("tmp")

        assert(hashlib.md5(read_file('./output/3.csv')).hexdigest() ==
               hashlib.md5(read_file('./output/4.csv')).hexdigest())  # Should be equal
        assert(hashlib.md5(read_file('./output/2.csv')).hexdigest() !=
               hashlib.md5(read_file('./output/3.csv')).hexdigest())  # Shouldn't be equal

        for inactivity_period in range(1, 5):
            os.remove('./output/{}.csv'.format(inactivity_period))

    def test_empty_file(self):
        """
        Ensures that the code runs correctly with empty files
        """
        # An empty file with no header should not run silently.
        fh = open("tmp", 'w')
        fh.write('\n')
        fh.close()
        self.sessionization = sessionization.Sessionization(log_file='tmp',
                                                            inactivity_period=self.inactivity_period,
                                                            output_file=self.output_file)
        self.assertRaises(AssertionError, self.sessionization.run)

        # Has Header but no data should run fine and produce an empty output
        fh = open("tmp", 'w')
        fh.write('ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser\n')
        fh.close()
        self.sessionization = sessionization.Sessionization(log_file='tmp',
                                                            inactivity_period=self.inactivity_period,
                                                            output_file=self.output_file)
        self.sessionization.run()
        assert(os.stat(self.output_file).st_size == 0)
        os.remove("tmp")


if __name__ == '__main__':
    import sys
    unittest.main()
