#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unittests for sessionization
"""
import unittest
import os
import hashlib
from IPython import embed

from src import sessionization

class SessionizationTest(unittest.TestCase):

    def setUp(self):
        self.log_file = './input/log.csv'
        self.inactivity_period = './input/inactivity_period.txt'
        self.output_file = './output/sessionization.txt'
        self.sessionization = sessionization.Sessionization(log_file=self.log_file,
                                                            inactivity_period=self.inactivity_period,
                                                            output_file=self.output_file)

    def test_download(self):
        storage_dir = os.path.join('/mnt', 'd', 'steph', 'SEC')  # TODO: change this to something reasonable
        log_index = os.path.join('/mnt', 'd', 'steph', 'SEC', 'LogFiles.html')
        obj = sessionization.PublicEDGARLogFiles(log_index=log_index,
                                                 storage_dir=storage_dir)
        obj.get_log(obj.log_urls[100])  # 'log20030411.zip'
        assert(os.path.exists(os.path.join(storage_dir, 'log20030411.csv')))

    def test_clean(self):
        self.sessionization.run()

    def test_inactivitytimes(self):
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

    def test_empty_file(self):
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
    sys.exit(unittest.main())
