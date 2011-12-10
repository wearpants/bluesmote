from boto import connect_s3, s3
import os


bucket_name = os.environ.get('BLUESMOTE_LOGS_BUCKET')
conn = connect_s3()
bucket = conn.create_bucket(bucket_name)



