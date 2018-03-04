# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from itertools import groupby

from rqworkers.dataUploader.uploader.datafile import DataFile, get_timestamp

import os
import csv
import io


class StopFile(DataFile):
    """ Class that represents a stop file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def make_docs(self):
        # Get filename and extension
        filename, file_extension = os.path.basename(self.datafile).split(".")
        with io.open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            # Group data using 'authRouteCode' as key
            for authUserOp, stops in groupby(reader, lambda r: (r['authRouteCode'], r['userRouteCode'], r['operator'])):
                stops = list(stops)
                path = self.get_path()
                timestamp = get_timestamp()
                date = self.name_to_date()
                stops = [
                    {
                        'order': int(p['order']),
                        'longitude': float(p['longitude']),
                        'latitude': float(p['latitude']),
                        'authStopCode': p['authStopCode'],
                        'userStopCode': p['userStopCode'],
                        'stopName': p['stopName']
                    } for p in stops
                ]
                yield {
                    "_source": {
                        "path": path,
                        "timestamp": timestamp,
                        "date": date,
                        "authRouteCode": authUserOp[0],
                        "userRouteCode": authUserOp[1],
                        "operator": int(authUserOp[2]),
                        "stops": stops
                    }
                }

    def get_header(self):
        return 'authRouteCode|userRouteCode|operator|order|authStopCode|userStopCode|stopName|latitude|longitude'
