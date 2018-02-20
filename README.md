# ESUploader
Python script to upload files to Elasticsearch

To run the uploader, first download the files in the repo https://github.com/SmartcitySantiagoChile/elasticsearchInstaller/ and follow the instructions provided in the Readme file.

After elasticsearch and cerebro are running, go to the ESUploader folder and execute:

    chmod +x requirements
    ./requirements
    
To load a file, the usage is:

    python3 loadData -host "127.0.0.1" -port 9200 -index "index-name" -file 'path/to/file' -doctype '*' -mapping 'path/to/mapping'
    
The default host is 127.0.0.1 and the port is 9200, but the options -host and -port allow to use other hosts and ports. The doctype has to be the same type mentioned in the mapping and the mapping has to be in JSON format. E.g: 

    {
        "mappings": {
            "*": {
                "properties": {
                    "id": {"type": "long"},
                    "type": {"type": "text"},
                    "description": {"type": "text"}
                }
            }
        }
    }
    
In this case, the doctype is ```'*'```.
    
There is also the ```-threads``` option that allows to choose the number of threads to use when loading the data. Usage: ```-threads 8```. The default number is 4.
