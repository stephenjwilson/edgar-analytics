# Table of Contents
1. [Introduction](README.md#introduction)
2. [Approach](README.md#approach)
3. [Requirements](README.md#requirements)
4. [Running the code](README.md#running-the-code)
5. [Testing](README.md#testing-the-code)
6. [Author](README.md#author)

# Introduction
This Repository was created to submit to the [Insight Data Engineering Coding Challenge](https://github.com/InsightDataScience/edgar-analytics).

# Approach
I approached this coding challenge using Python 3.6 and through an object-oriented perspective.
I created three classes to process and store the EDGAR data.

This class is called to process an EDGAR log file and "Sessionize" the log into sessions based on IP address. 
Two support classes are used to represent the sessions (Session) and the set of sessions (SessionSet).

I ignored the document being retrieved at each request, as duplicate document requests are counted twice.

The Session class:
 * Stores all important information about a session
 * Formats data for output, 
 * Updates the session as needed
 * Provides a function to assess if the session has ended

The SessionSet class creates, stores, and closes all the sessions. An important feature of this class is that when 
deleted, it will close all sessions that are open and write them to file. Currently, this occurs at the end of a log file.

Currently, these classes are relatively minimal, but they can be easily extended to:
 * Accept different file formats
 * Change the criteria to end a session
 * Alter the output format
 
 An easy way to speed up processing many log files would be to process separate log files in parallel.

# Requirements
Language:
* Python 3.6

Third-Party Libraries:
* None

Systems Tested: 
* Windows 10
* Ubuntu 

# Running the Code:
This repository's code can be run with:
```bash
./run.sh
```
# Testing the Code:
This repository can be tested by:
```bash
cd insight_testsuite
chmod +x ./run_tests.sh
./run_tests.sh
```

## Unittests
A few trivial python unittests were provided and added to the run_tests.sh script. 
These were not the focus of testing but to demonstrate an alternative testing approach.
They can be run separately by running:
```bash
python setup.py test
``` 

# Author
Created by Stephen J. Wilson