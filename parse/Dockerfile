FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano
RUN apt-get upgrade -y zlib1g
RUN pip install nose netCDF4 pymongo xarray numpy geopy

WORKDIR /app
COPY . .
RUN chown -R 1000660000 /app
CMD ['python', 'choosefiles.py']
