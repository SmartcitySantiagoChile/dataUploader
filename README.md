# ESUploader
Python script to upload files to Elasticsearch

To run the uploader, first download the files in the repo https://github.com/SmartcitySantiagoChile/elasticsearchInstaller/ and follow the instructions provided in the Readme.

After elasticsearch and cerebro are running, go to the ESUploader folder and execute:

    chmod +x requirements
    ./requirements
    
To load a file, the usage is:

    python3 loadData -index index-name -file path/to/file

Arguments:

    -index: name of the index to create/use. If an index with the same name does not exist, the application will create it. Otherwise, it will use the existing index.
    -file: path to the file that contains the data to load.
    
Optional:

    -host: the IP where ES is running. Default:"127.0.0.1".
    -port: the port that application is listening on. Default: 9200.
    -chunk: number of docs to send to ES in one chunk. Default: 5000.
    -threads: number of threads to use. Default: 4.
    -timeout: timeout parameter of the ES client. Default:30 (seconds).

The default chunk size and number of threads are the ones that gave the best results when experimenting with different files, so using the defaults is recommended. Nevertheless, sometimes loading a big file can cause a timeout error; in this case, raising the timeout value should solve the issue (https://github.com/elastic/elasticsearch-py/issues/231).


About the mappings:
	
    The mappings must be in a folder called 'mappings' in the same directory where loadData.py is. The script uses the datafile extension to know what mapping to use so, if the mapping is called 'ext-template.json', the the file must have 'ext' as its extension. For the mappings, only JSON format is accepted.

      As the ES team is planning to deprecate doctypes, the mappings used to load the file should all use the default doctype that is 'doc'. If a property is a 'date', then expliciting the format is recommended since ES is not able to parse all date formats and it may cause an exception. E.g.: 

    {
        "mappings": {
            "doc": {
                "properties": {
                    "id": {"type": "long"},
					"dateTime": {"type": "date", "format" : "yyyy-MM-dd HH:mm:ss"},
                    "type": {"type": "text"},
                    "description": {"type": "text"}
                }
            }
        }
    }

Additionally, is a property is listed as boolean only 'true' and 'false' are accepted, using 0 and 1 will cause a 'MapperParsingException'.
  






