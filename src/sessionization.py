#!/usr/bin/env python
"""
TODO: Fill DOC STRING
"""

import datetime

__author__ = "Stephen J. Wilson"
__version__ = "1.0.1"
__email__ = "contact@stephenjwilson.com"

import urllib.request
import zipfile
import io
import os
from IPython import embed


class Sessionization(object):
    """Processes the EDGAR log files.
    This class is called to process an EDGAR log file and "Sessionize" the log into sessions based on IP address.
    In this approach, a two support classes are used to represent the sessions (session) and the set of sessions
    (SessionSet).

     A "sessionization" output file is created that corresponds to the sessions present in the log file.
    """

    def __init__(self, log_file='./input/log.csv', inactivity_period='./input/inactivity_period.txt',
                 output_file='./output/sessionization.txt'):
        """Constructor for the Sessionization object

        :param log_file: This is the path to a EGDAR log file in csv format. See the \
        [SEC website](https://www.sec.gov/dera/data/edgar-log-file-data-set.html) for mor detail.
        :param inactivity_period: This is plain text file containing a single line with a single int value.
        :param output_file: The file path for the output
        """

        self.log_file = log_file
        self.output_file = output_file
        fh = open(inactivity_period, 'r')
        self.inactivity_period = int(fh.read().strip())
        fh.close()
        self.header = ''

        self.extracted_fields = ['ip', 'date', 'time', 'cik', 'accession', 'extention']
        self.time_format = '%H:%M:%S'
        self.date_format = '%Y-%m-%d'
        self.quote_character = '"'
        self.delimiter = ','

    def run(self):
        """
        Processes the EDGAR log file.
        """
        with open(self.log_file, 'r') as stream:
            stream = self.cleanse(stream)
            session_set = SessionSet(self.output_file,self.inactivity_period)
            for row in stream:
                session_set.process_row(row)

    def clean_row(self, row, header_map):
        """
        Cleans a row of an EDGAR file
        :param row: The row that will be cleaned and processed
        :param header_map: This is a dictionary that maps the header name to the appropriate column
        :return: Returns a dictionary that includes all information from the extracted_fields and a datetime object
        """
        # cleans a row of input
        row = row.strip().split(self.delimiter)  # Split and strip the row
        row_data = {}  # Data will be stored in a dictionary
        for field in self.extracted_fields:
            row_data[field] = row[header_map[field]]  # Access the appropriate row according to the header

        # Get datetime objects from the date and time information
        datetime_object = datetime.datetime.strptime('{} {}'.format(row_data['date'], row_data['time']),
                                                     '{} {}'.format(self.date_format, self.time_format))
        row_data['dt'] = datetime_object
        return row_data

    def cleanse(self, stream):
        """
        Cleans and parses the data from the data stream
        :param stream: The file data stream. The first line of the file should be a header
        :return:
        """
        # TODO: Check if file has data

        # Set header
        self.header = stream.readline().strip().split(self.delimiter)
        # Ensure all fields are in the header
        assert(len(self.extracted_fields) == len(set(self.header).intersection(self.extracted_fields)))

        # Get the indices of the fields to extract
        header_map = {}
        for field in self.extracted_fields:
            header_map[field] = self.extracted_fields.index(field)

        # return cleaned data
        for row in stream:
            yield self.clean_row(row, header_map)

    def set_extracted_fields(self, extracted_fields):
        self.extracted_fields = extracted_fields

    def get_extracted_fields(self):
        return self.extracted_fields

class SessionSet(object):

    def __init__(self, output_file, inactivity_period):
        self.output_file = output_file
        self.inactivity_period = inactivity_period
        self.current_time = None
        self.sessions = []
        self.index = {}
        self.output_file_fh = open(self.output_file, 'w')

    def __del__(self):
        # Close any active sessions
        for session in self.sessions:
            self.output_file_fh.write(session.close_session())
        # Close the file
        self.output_file_fh.close()
        # Delete the data
        del self.index
        self.sessions.clear()

    def process_row(self, row):
        # Set the current time if not set before. Will be used to clean out old sessions
        if isinstance(self.current_time, type(None)):
            self.current_time = row['dt']
        # Try to get the appropriate session if it exists
        try:
            ind = self.index[row['ip']]  # get the index of the ip
            session = self.sessions[ind]
            session.update_session(row['dt'])
        except KeyError:
            session = Session(self.inactivity_period, row['ip'], row['dt'])
            self.sessions.append(session)
            self.index[row['ip']] = len(self.sessions) - 1  # Add to the index
        # Clean out old sessions
        row_time = session.time_last
        if row_time != self.current_time:
            self.current_time = row['dt']
            self.update_sessions()

    def update_sessions(self):
        to_del = []
        for i in range(0, len(self.sessions)):
            session = self.sessions[i]
            duration = self.current_time - session.time_last
            data = session.check_session(duration)
            if not isinstance(data, type(None)):
                self.output_file_fh.write(data)
                to_del.append(i)

        # delete sessions that ended
        self.sessions = [v for i, v in enumerate(self.sessions) if i not in to_del]  # TODO: potential improvement needed at scale
        self.index = {v.ip: i for i, v in enumerate(self.sessions)} # update the index


class Session(object):

    def __init__(self, inactivity_period, ip, time_first, duration=1, document_number=1,
                 time_format='%H:%M:%S', date_format='%Y-%m-%d'):  # TODO: ensure formats are passes
        self.inactivity_period = inactivity_period
        self.ip = ip
        self.time_first = time_first
        self.time_last = time_first
        self.duration = duration
        self.document_number = document_number
        self.time_format = time_format
        self.date_format = date_format
        self.quote_character = '"'
        self.delimiter = ','

    def _format_csv(self, s):
        s = '{}'.format(s)
        if ',' in s:
            return '{}{}{}'.format(self.quote_character, s, self.quote_character)
        else:
            return s

    def __repr__(self):
        return str(self)

    def __str__(self):
        data = [self._format_csv(x) for x in
                [self.ip, self.time_first.strftime('{} {}'.format(self.date_format, self.time_format)),
                 self.time_last.strftime('{} {}'.format(self.date_format, self.time_format)),
                 self.duration, self.document_number]]
        return self.delimiter.join(data)+'\n'

    def close_session(self):
        return str(self)

    def check_session(self, duration):
        # session existed, update session
        if duration.seconds > self.inactivity_period:  # Checks if session is still active
            # close this session
            return self.close_session()
        else:
            return None

    def update_session(self, current_time):
        self.document_number += 1
        self.time_last = current_time
        tmp = self.time_last - self.time_first
        self.duration = tmp.seconds + 1


class PublicEDGARLogFiles(object):

    def __init__(self, log_index, storage_dir):
        self.log_index = log_index
        self.log_urls = []
        self.storage_dir = storage_dir
        self.downloaded_logs = [] # all the downloaded log files

        # Process log_index
        fh = open(self.log_index)
        for line in fh:
            self.log_urls.append('http://{}'.format(line.strip()))
        fh.close()

    def get_log(self, log_url):
        f = urllib.request.urlopen(log_url)
        r = f.read()
        file_name = log_url.split('/')[-1]
        with open(os.path.join(self.storage_dir, file_name), "wb") as fh:
            fh.write(r)
        z = zipfile.ZipFile(io.BytesIO(r))
        z.extractall(self.storage_dir)
