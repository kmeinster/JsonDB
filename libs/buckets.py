# from datetime import datetime
# from minio import Minio
# from minio.error import ResponseError, BucketAlreadyExists
#
# minioClient = Minio('localhost:9000',
#                   access_key='minio',
#                   secret_key='minio123',
#                   secure=False)
#
# def create_folders():
#     now = datetime.now()  # current date and time
#     year = now.strftime("%Y")
#     month = now.strftime("%m")
#     day = now.strftime("%d")
#     hour = now.strftime("%H")
#     minute = now.strftime("%M")
#
#     minioClient.
#
#     # try:
#     #     minioClient.make_bucket(bucket_name=str('{}/{}/{}/{}/{}').format(year,
#     #                                                                      month,
#     #                                                                      day,
#     #                                                                      hour,
#     #                                                                      minute))
#     # except Exception as e:
#     #     print(e)
#
# create_bucket()