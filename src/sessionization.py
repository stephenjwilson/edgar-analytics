#!/usr/bin/env python
"""
This file contains four classes that enable the tracking and processing of sessions for SEC EDGAR log files.
Sessionization is the top-level class that creates a SessionSet, which keeps track of individual Sessions.
To initialize, a log-file, an inactivity_period, and output_file are needed. All are file paths, and the
inactivity_period is simply an int stored on a single line of the file.

Example:
        import sessionization
        log_file = './input/log.csv'
        inactivity_period = './input/inactivity_period.txt'
        output_file = './output/sessionization.txt'
        sessionizationObj = sessionization.Sessionization(log_file=log_file,
                                                            inactivity_period=inactivity_period,
                                                            output_file=output_file)
        sessionizationObj.run()
"""

__author__ = "Stephen J. Wilson"
__version__ = "1.0.1"
__email__ = "contact@stephenjwilson.com"

import urllib.request
import zipfile
import io
import os
import re
import sys
import datetime
from IPython import embed

class Sessionization(object):
    """Processes the EDGAR log files.
    This class is called to process an EDGAR log file and "Sessionize" the log into sessions based on IP address.
    In this approach, a two support classes are used to represent the sessions (session) and the set of sessions
    (SessionSet).

     A "sessionization" output file is created that corresponds to the sessions present in the log file.

     Example:
        import sessionization
        log_file = './input/log.csv'
        inactivity_period = './input/inactivity_period.txt'
        output_file = './output/sessionization.txt'
        sessionizationObj = sessionization.Sessionization(log_file=log_file,
                                                            inactivity_period=inactivity_period,
                                                            output_file=output_file)
        sessionizationObj.run()

    """

    def __init__(self, log_file='./input/log.csv', inactivity_period='./input/inactivity_period.txt',
                 output_file='./output/sessionization.txt'):
        """Constructor for the Sessionization object

        :param log_file: This is the path to a EDGAR log file in csv format. See the \
        [SEC website](https://www.sec.gov/dera/data/edgar-log-file-data-set.html) for mor detail.
        :param inactivity_period: This is plain text file containing a single line with a single int value.
        :param output_file: The file path for the output


        Example:
            import sessionization
            log_file = './input/log.csv'
            inactivity_period = './input/inactivity_period.txt'
            output_file = './output/sessionization.txt'
            sessionizationObj = sessionization.Sessionization(log_file=log_file,
                                                                inactivity_period=inactivity_period,
                                                                output_file=output_file)
            sessionizationObj.run()
        """

        self.log_file = log_file
        self.output_file = output_file
        fh = open(inactivity_period, 'r')
        self.inactivity_period = int(fh.read().strip())  # will error with inputs that cannot be cast to int
        fh.close()
        self.header = ''

        # Private Constants
        self._extracted_fields = ['ip', 'date', 'time']# 'cik', 'accession', 'extention']
        self._reDT = re.compile(r'(\d{4})-(\d{2})-(\d{2})(\d{2}):(\d{2}):(\d{2})')
        self._quote_character = '"'
        self._delimiter = ','

    def run(self):
        """
        Processes the EDGAR log file.
        """
        with open(self.log_file, 'r') as stream:
            stream = self.cleanse(stream)
            session_set = SessionSet(self.output_file, self.inactivity_period)
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
        row = row.strip().split(self._delimiter)  # Split and strip the row
        row_data = {}  # Data will be stored in a dictionary
        for field in self._extracted_fields:
            row_data[field] = row[header_map[field]]  # Access the appropriate row according to the header

        # Get datetime objects from the date and time information
        datetime_object = datetime.datetime(*map(int, self._reDT.match(row_data['date']+row_data['time']).groups()))
        row_data['dt'] = datetime_object
        return row_data

    def cleanse(self, stream):
        """
        Cleans and parses the data from the data stream
        :param stream: The file data stream. The first line of the file should be a header
        :return: Yields rows that are cleaned and in dictionary format. See clean_row.
        """

        # Set header
        self.header = stream.readline().strip().split(self._delimiter)
        # Ensure all fields are in the header
        assert (len(self._extracted_fields) == len(set(self.header).intersection(self._extracted_fields)))

        # Get the indices of the fields to extract
        header_map = {}
        for field in self._extracted_fields:
            header_map[field] = self._extracted_fields.index(field)

        # Return cleaned data
        for row in stream:
            yield self.clean_row(row, header_map)


class SessionSet(object):
    """
    This class will hold the sessions from an EDGAR file. Initialized, it keeps track of the active session, and
    it closes all sessions when the class it deleted. This is relevant behaviour for the EOF or the end of a data
    stream.
    """

    def __init__(self, output_file, inactivity_period):
        """ Constructor of the Session Set

        :param output_file: This is the file path to the output
        :param inactivity_period: This is the int (in seconds) of the inactivity period.
        """
        self.output_file = output_file
        self.inactivity_period = inactivity_period
        self.current_time = None
        self.sessions = []
        self.index = {}
        self.output_file_fh = open(self.output_file, 'w')

    def __del__(self):
        """
        This is the clean-up of the SessionSet object when deleted. Tt closes all sessions when the class it deleted.
        This is relevant behaviour for the EOF or the end of a data stream. Additionally, it closes the output file
        opened in the initialization.
        """
        # Close any active sessions
        for session in self.sessions:
            self.output_file_fh.write(str(session))
        # Close the file
        self.output_file_fh.close()
        # Delete the data
        del self.index
        self.sessions.clear()

    def process_row(self, row):
        """
        This function processes each row of the EDGAR file and determines if a session needs to be created or updated.
        :param row: This is a dictionary of the row, with the appropriate headers as keys. In particular, 'ip' is used
                    to uniquely identify a session, and 'dt' is a datetime object identifying the time of a request.
                    The columns 'cik', 'accession', 'extention' are not used, as duplicate accessions are counted as
                    separate requests.
        """
        # Set the current time if not set before. Will be used to clean out old sessions
        if isinstance(self.current_time, type(None)):
            self.current_time = row['dt']

        # Clean out old sessions if the time-stamp has changed
        row_time = row['dt']
        if row_time != self.current_time:
            self.current_time = row['dt']
            self.update_sessions()

        # Try to get the appropriate session if it exists
        try:  # Get existing session
            ind = self.index[row['ip']]  # get the index of the ip
            session = self.sessions[ind]
            session.update_session(row['dt'])
        except KeyError:  # Make a session
            session = Session(self.inactivity_period, row['ip'], row['dt'])
            self.sessions.append(session)
            self.index[row['ip']] = len(self.sessions) - 1  # Add to the index

    def update_sessions(self):
        """
        This function is called when processing a row to update all sessions whenever the time has changed.
        It writes out sessions if they are old, and then deletes the sessions that are closed.
        """
        to_del = []
        for i in range(0, len(self.sessions)):
            session = self.sessions[i]
            duration = self.current_time - session.time_last
            data = session.check_session(duration)
            if not isinstance(data, type(None)):
                self.output_file_fh.write(data)
                to_del.append(i)

        # delete sessions that ended, re-index sessions
        self.sessions = [v for i, v in enumerate(self.sessions) if i not in to_del]
        self.index = {v.ip: i for i, v in enumerate(self.sessions)}  # update the index


class Session(object):
    """
    The Session class keeps track of all relevant data for a session.
    """

    def __init__(self, inactivity_period, ip, time_first, duration=1, document_number=1):
        """
        The constructor for the Session class, which keeps track of a single session.
        See (datetime documentation)[https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior] for
        details of the time_format and date_format.

        :param inactivity_period: This is the int (in seconds) of the inactivity period.
        :param ip: The IP address that uniquely identifies a session
        :param time_first: This is the time a session starts
        :param duration: This is the duration of a session, defaults to 1.
        :param document_number: The number of documents requested in a session, defaults to 1.
        :param time_format: This is the format of the time in the files, defaults to '%H:%M:%S'.
        :param date_format: This is the format of the date in the files, defaults to '%Y-%m-%d'.
        """
        self.inactivity_period = inactivity_period
        self.ip = ip
        self.time_first = time_first
        self.time_last = time_first
        self.duration = duration
        self.document_number = document_number
        self.delimiter = ','

    def _format_datetime(self, s):
        """
        A faster version of datetimeObj.strftime()
        :param s: A datetime object
        :return: A string formatted with the date and the time
        """
        return '%d-%02d-%02d %02d:%02d:%02d' % (s.year, s.month, s.day, s.hour, s.minute, s.second)

    def __repr__(self):
        """
        Returns the string representation of a session based in __str__
        :return: The string representation of a session
        """
        return str(self)

    def __str__(self):
        """
        Returns the string representation of a session in a csv-safe manner.
        :return: The string representation of a session
        """
        data = '%s,%s,%s,%s,%d\n' % (self.ip,
                                     self._format_datetime(self.time_first), self._format_datetime(self.time_last),
                                     self.duration, self.document_number)
        return data

    def check_session(self, duration):
        """
        Checks a session to see if it is inactive based on the inactivity period.
        :param duration: the duration that the session has been inactive
        :return: Returns the string representation of the session if inactive, None if active
        """
        # session existed, update session
        if duration.total_seconds() > self.inactivity_period:  # Checks if session is still active
            # close this session
            return str(self)
        else:
            return None

    def update_session(self, current_time):
        """
        This function updates the current session with a new request.
        :param current_time: a datetime object of the current time to be stored as the last time the session was active.
        """
        self.document_number += 1  # Updates the number of documents retrieved, where one line is one document
        self.time_last = current_time  # Updates the time the session was last active
        tmp = self.time_last - self.time_first  # Updates the duration of the session
        self.duration = tmp.seconds + 1


class PublicEDGARLogFiles(object):
    """
    A class to retrieve the public EDGAR files. Takes the
    (EDGAR index file)[https://www.sec.gov/files/EDGAR_LogFileData_thru_Jun2017.html] processed into a csv as input.
    """

    def __init__(self, log_index, storage_dir):
        """
        The constructor of the PublicEDGARLogFiles class.
        :param log_index: Takes a csv version of the log file that contains all urls for the log files.
        :param storage_dir: The path to store all the log files.
        """
        self.log_index = log_index
        self.log_urls = []
        self.storage_dir = storage_dir
        self.downloaded_logs = []  # all the downloaded log files

        # Process log_index
        fh = open(self.log_index)
        for line in fh:
            self.log_urls.append('http://{}'.format(line.strip()))
        fh.close()

    def get_log(self, log_url):
        """
        Downloads and unzips the log file at log_url
        :param log_url: This is a url of the zipped log.
        :return:
        """
        f = urllib.request.urlopen(log_url)
        r = f.read()
        file_name = log_url.split('/')[-1]
        with open(os.path.join(self.storage_dir, file_name), "wb") as fh:
            fh.write(r)
        z = zipfile.ZipFile(io.BytesIO(r))
        z.extractall(self.storage_dir)


if __name__ == '__main__':
    log = sys.argv[1]
    inactivity = sys.argv[2]
    output = sys.argv[3]
    sessionizationObj = Sessionization(log_file=log,
                                       inactivity_period=inactivity,
                                       output_file=output)
    sessionizationObj.run()
