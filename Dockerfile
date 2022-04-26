FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano rsync
RUN apt-get upgrade -y zlib1g subversion
RUN pip install nose netCDF4 pymongo xarray numpy geopy

WORKDIR /app
COPY . .
RUN chown -R 1000660000 /app
