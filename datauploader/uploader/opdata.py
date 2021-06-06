import csv
import traceback
from itertools import groupby

from datauploader.uploader.datafile import DataFile


class OPDataFile(DataFile):
    """ Class that represents a opdata file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['ServicioSentido', 'UN', 'Servicio', 'Sentido', 'ServicioTS', 'TipoDia', 'PeriodoTS',
                           'HoraIni', 'HoraFin', 'Frecuencia', 'Capacidad', 'Distancia', 'Velocidad']

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            # Group data using 'route' as key
            for identifier, day_types in groupby(reader,
                                                 lambda day_type: (day_type['TipoDia'], day_type['ServicioTS'],
                                                                   day_type['UN'], day_type['Servicio'])):
                try:
                    day_types = list(day_types)
                    path = self.basename
                    day_types = [{
                        'timePeriod': int(p['PeriodoTS']),
                        'startPeriodTime': p['HoraIni'],
                        'endPeriodTime': p['HoraFin'],
                        'frequency': float(p['Frecuencia']),
                        'capacity': float(p['Capacidad']),
                        'distance': float(p['Distancia']),
                        'speed': float(p['Velocidad'])} for p in day_types]
                    yield {
                        "_source": {
                            "path": path,
                            "date": path.split(".")[0],
                            "opRouteCode": identifier[1],
                            "operator": int(identifier[2]),
                            "userRouteCode": identifier[3],
                            "dayType": day_types
                        }
                    }
                except ValueError:
                    traceback.print_exc()
