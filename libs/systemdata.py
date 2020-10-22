import random, os, datetime, uuid


def ingest_time():
    return datetime.datetime.now()


def generate_uuid():
    id = uuid.uuid4().hex
    return id


def systemdata():
    time = ingest_time()
    id = generate_uuid()
    return time, id


# def prefixer(metricName):
