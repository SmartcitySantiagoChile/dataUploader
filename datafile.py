#!/usr/bin/python3
# Class that represents a general file.

import csv
from datetime import datetime
from elasticsearch.helpers import parallel_bulk

class DataFile:
	def __init__(self, datafile):
		self.datafile = datafile

	def load(self, client, index_name, doctype):
		with open(self.datafile, "r", encoding="latin-1") as f:
			reader = csv.DictReader(f, delimiter='|')
			for success, info in parallel_bulk(client, reader, index=index_name, doc_type=doctype):
    				if not success: print('Doc failed', info)

def nameToDate(filename):
	startDate = datetime.strptime(filename, '%Y-%m-%d').isoformat() + '.000Z' #Python doesn't support military Z.
	return startDate
