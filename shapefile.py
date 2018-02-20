#!/usr/bin/python3

import os, re
from subprocess import call
from itertools import groupby
from datafile import *

# Class that represents a shape file and its mapping.
class ShapeFile(DataFile):
	def __init__(self, datafile):
		DataFile.__init__(self, datafile)

	def load(self, client, index_name, doctype, threads):
		self.prepareFile()
		for success, info in parallel_bulk(client, self.read_routes(), thread_count=int(threads), index=index_name, doc_type=doctype):
    				if not success: print('Doc failed', info)

	def headerIsOK(self):
		#Read first line
		with open(self.datafile, 'r', encoding='latin-1') as f:
			header = f.readline().rstrip('\n')
		#If the header is already the one we want
		if header == 'route|segmentStart|longitude|latitude':
			return True
		else:
			return False

	def hasHeader(self):
		#Read first line
		with open(self.datafile, 'r', encoding='latin-1') as f:
			header = f.readline().rstrip('\n')
		#Check characters
		search = re.compile(r'[^a-zA-Z|#.]').search
		return not bool(search(header))

	def removeHeader(self):
		#Remove first line of the file
		call(["sed", "-i", '1d', self.datafile])

	def addHeader(self):
		#Put this on the first line
		call(["sed", "-i", '1i route|segmentStart|longitude|latitude', self.datafile])

	def prepareFile(self):
		#Header is correct
		if self.headerIsOK():
			pass
		#Has a header but it's not right
		elif self.hasHeader():
			self.removeHeader()
			self.addHeader()
		#Doesn't have a header
		else:
			self.addHeader()

	def read_routes(self):
		#Get filename and extension
		filename, file_extension = os.path.basename(self.datafile).split(".")
		with open(self.datafile, "r") as f:
			reader = csv.DictReader(f, delimiter='|')
        	#Group data using 'route' as key
			for route, points in groupby(reader, lambda p: p['route']):
				points = list(points)
				startDate = nameToDate(filename)	
				points = [
					{
					'segmentStart': p['segmentStart'],
					'longitude': p['longitude'],
					'latitude': p['latitude']
					} for p in points
       				]
				yield {
					"_source": {
						"route": route,
						"startDate": startDate,
						"points": points
            		}
        		}
