FROM python:3.7

RUN apt-get update -y
RUN apt-get install nano
RUN pip install nose netCDF4 pymongo xarray numpy geopy

WORKDIR /app
COPY . .
RUN chown -R 1000660000 /app
CMD ['python', 'choosefiles.py']
