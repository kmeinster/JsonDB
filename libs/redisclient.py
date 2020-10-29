import redis, json
from datetime import datetime
from redistimeseries.client import Client as RTSClient


class RedisClient:
    def __init__(self):
        self.host = '192.168.64.103'
        self.port = 6379
        self.db = 0
        self.r = redis.Redis(host=self.host,
                             port=self.port,
                             db=self.db)

    def ingest(self, labels, timestamp, metrics):
        for m in metrics:
            print(m['name'])
        rts = RTSClient(conn=self.r)
        for metric in metrics:
            timeseries = metric['name']
            value = metric['value']
            try:
                print(f'We\'re not in any exception, yeeha: {timeseries} - {timestamp} - {value} - {labels}')
                rts.add(key=timeseries, timestamp=int(timestamp), labels=labels, value=value)
            except Exception as e:
                print(f'We\'re in an exception: {e}')
                rts.create(key=timeseries, labels=labels)
                rts.add(key=timeseries, timestamp=int(timestamp), labels=labels, value=value)
                pass
        return


1604009296
1604012182
1604012098042