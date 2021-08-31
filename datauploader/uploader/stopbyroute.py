import csv
import traceback
from collections import defaultdict
from itertools import groupby

from datauploader.uploader.datafile import DataFile, get_timestamp


class StopByRouteFile(DataFile):
    """ Class that represents a stop file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['authRouteCode', 'userRouteCode', 'operator', 'order', 'authStopCode', 'userStopCode',
                           'stopName', 'latitude', 'longitude']
        self.routes_by_stop = defaultdict(lambda: set())
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = '|'
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            for row in reader:
                self.routes_by_stop[row['authStopCode']].add(row['userRouteCode'])

        for authStopCode in self.routes_by_stop.keys():
            route_list = list(self.routes_by_stop[authStopCode])
            route_list.sort()
            self.routes_by_stop[authStopCode] = route_list

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)

            # Group data using 'authRouteCode' as key
            for authUserOp, stops in groupby(reader,
                                             lambda r: (r['authRouteCode'], r['userRouteCode'], r['operator'])):
                # skip if authority operator code is an hyphen
                if authUserOp[0] == str('-'):
                    continue
                try:
                    path = self.basename
                    timestamp = get_timestamp()
                    date = self.name_to_date()
                    stops = [
                        {
                            'order': int(p['order']),
                            'longitude': float(p['longitude']),
                            'latitude': float(p['latitude']),
                            'authStopCode': p['authStopCode'],
                            'userStopCode': p['userStopCode'],
                            'routes': self.routes_by_stop[p['authStopCode']],
                            'stopName': p['stopName'],
                        } for p in stops
                    ]
                    yield {
                        "_source": {
                            "path": path,
                            "timestamp": timestamp,
                            "startDate": date,
                            "authRouteCode": authUserOp[0],
                            "userRouteCode": authUserOp[1],
                            "operator": int(authUserOp[2]),
                            "stops": stops
                        }
                    }
                except ValueError:
                    traceback.print_exc()
