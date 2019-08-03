from __future__ import print_function # Python 2/3 compatibility
import json
import boto3
import logging
import os
import datetime
import calendar
import decimal
from boto3.dynamodb.conditions import Key, Attr
 
from base64 import b64decode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3 = boto3.resource('s3')
 
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
 
    # DynamoDBからcsvデータの作成
def setcsv(dt):
    dtj=json.loads(json.dumps(dt, cls=DecimalEncoder))
    user_id=dtj['user_id']
    lat_north_south=dtj['lat_north_south']
    latitude=dtj['latitude']
    lon_west_east=dtj['lon_west_east']
    longitude=dtj['longitude']
    timestamp=dtj['timestamp']
    csv=""
    csv=csv+str('\n')
    csv=csv+user_id+','
    csv=csv+lat_north_south+','
    csv=csv+latitude+','
    csv=csv+lon_west_east+','
    csv=csv+longitude+','
    csv=csv+str(timestamp)
    return csv
    
def lambda_handler(event, context):
    bucket = s3.Bucket('task3-location-csv')
    table = dynamodb.Table('serverless-location-api-dev')
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    stryesterday = yesterday.strftime("%Y%m%d")
    # GSIに設定したdateから前日のデータを取得
    response = table.query(
        IndexName='date-index',
        KeyConditionExpression=Key('date').eq(stryesterday)
    )
    # ヘッダー作成
    csv="user_id,lat_north_south,latitude,lon_west_east,longitude,timestamp"
    for dt in response['Items']:
        csv=csv+setcsv(dt)

    # 1MB以上の場合に繰り返し取得       
    while 'LastEvaluatedKey' in response:
        response = table.query(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            IndexName='date-index',
            KeyConditionExpression=Key('date').eq(stryesterday)
        )
        for dt in response['Items']:
            csv=csv+setcsv(dt)
 
    # logger.info(str(csv))
    now = datetime.datetime.now()
    strnow = now.strftime("%Y%m%d%H%M%S")
    # S3バケットへ出力
    ret = bucket.put_object( Body= str(csv), Key='location_' + strnow + '.csv', ContentType='text/csv' )
    
    return str(ret)