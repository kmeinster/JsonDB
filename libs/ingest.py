import uuid, json, io
from datetime import datetime
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou


class MinioClient:
    # The following should come from a configurator, env vars or configfile.
    def __init__(self):
        self.host = 'localhost:9000'
        self.access_key = 'minio'
        self.secret_key = 'minio123'
        self.secure = False
        self.mc = Minio(self.host,
                            access_key=self.access_key,
                            secret_key=self.secret_key,
                            secure=self.secure)

    def generate_object_path_and_name(self, metricname):
        now = datetime.now()  # current date and time
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        minute = now.strftime("%M")
        full_path = f"{year}/{month}/{day}/{hour}/{metricname}-{minute}-{uuid.uuid4().hex}.json"
        return full_path

    def get_and_create_bucket(self, metricname):
        try:
            self.mc.make_bucket(bucket_name=metricname)
            return metricname
        except BucketAlreadyExists as e:
            # print("Bucket already exists, dummy")
            return metricname
        except BucketAlreadyOwnedByYou as e:
            # print('Bucket already exists and is owned by you.')
            return metricname

    def ingest(self, message: str):
        starting_time = datetime.now()
        msg = json.loads(message)
        metadata = msg['metadata']
        # TODO: user_time should have a default of ingestion time if not provided by user
        user_time = msg['systemdata']['timestamp']
        metrics = msg['data']['metrics']
        for metric in metrics:
            metric['ingest_timestamp'] = datetime.now().strftime("%H:%M:%S.%f - %b %d %Y")
            metric['timestamp'] = user_time
            print(metric)
            # print(metadata)
            buffer = io.BytesIO(json.dumps(metric).encode('utf-8'))
            self.mc.put_object(bucket_name=self.get_and_create_bucket(metric['name']),
                               object_name=self.generate_object_path_and_name(metric['name']),
                               data=buffer,
                               content_type='application/json',
                               metadata=metadata,
                               length=int(buffer.getbuffer().nbytes))
        stopping_time = datetime.now()
        print(stopping_time - starting_time)