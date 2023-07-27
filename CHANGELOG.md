# CHANGELOG

## v2.0.33 2023-07-27

- Increase timeout to 5 minutes
- Fix opdata loader
- Fix filter to upload trip data

## v2.0.32 2023-04-04

- Remove unused attribute in trip index

## v2.0.31 2023-04-04

- Apply filter only for dates greater than 2022-09-31

## v2.0.30 2023-04-03

- Cast tripNumber field to int for odbyroute index
- Ignore trip document if speed can not be calculated
- tviaje value for old files will be transformed to seconds
- Add dependencies to `setup.py` module