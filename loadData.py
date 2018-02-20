#!/usr/bin/python3
#This script will create an index on a existing elasticsearch instance and populate it using data from a given file. If an index with the same name already exists, it will use that index instead.

import argparse, elasticsearch
from datafile import *
from shapefile import *
from elasticsearch import Elasticsearch
	
if __name__ == "__main__":
	#Arguments and description
	parser = argparse.ArgumentParser(description='Add documents from a file to an elasticsearch index.')
	parser.add_argument('-host', default="127.0.0.1", help='elasticsearch host, default is "127.0.0.1"')
	parser.add_argument('-port', default=9200, help='port, default is 9200')
	parser.add_argument('-index', help='name of the index to create/use')
	parser.add_argument('-file', help='data file path, e.g. /usr/local/file')
	parser.add_argument('-doctype', help='type of the document, e.g. "doctype"')
	parser.add_argument('-mapping', help='mapping file path, e.g. /usr/local/mapping')
	args = parser.parse_args()
	
	#Get a client
	es = Elasticsearch(hosts=[{"host": args.host, "port": args.port}])
	#Read mapping
	mapping = open(args.mapping, 'r').read()
	#Create index with mapping, ignore if it exists already
	es.indices.create(index=args.index, ignore=400, body=mapping)

	#Give names to arguments
	index_name = args.index
	doctype = args.doctype
	datafile = args.file

	#Get filename and extension
	filename, file_extension = os.path.splitext(datafile)
	#Determine file type according to the extension
	if file_extension == '.shapes': #shape, shapes
		fileToLoad = ShapeFile(datafile)
	else:
		fileToLoad = DataFile(datafile)
	#Load file
	fileToLoad.load(es, index_name, doctype)

