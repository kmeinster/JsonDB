import json
from datetime import datetime
from libs.minioclient import MinioClient
from libs.redisclient import RedisClient

mc = MinioClient
rc = RedisClient()


def process_json_message(message):
    parsed_json = json.loads(message)
    labels = parsed_json['metadata']
    # TODO: Change user_time in json template, doesn't make much sense to call it ['systemdata']['timestamp']
    user_time = parsed_json['systemdata']['timestamp']
    user_time_dt = datetime.strptime(user_time,
                                       "%H:%M:%S.%f - %b %d %Y")
    user_time_unix = datetime.timestamp(user_time_dt)
    metrics = parsed_json['data']['metrics']
    rc.ingest(labels=labels, timestamp=int(user_time_unix * 1000), metrics=metrics)
    return labels, user_time, int(user_time_unix), metrics


def ingest_minio(labels, user_time, user_time_unix, metrics):
    pass

