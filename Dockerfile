FROM python:3.10-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY serial_to_influx.py .

# command to run on container start
CMD [ "python", "./serial_to_influx.py" ]