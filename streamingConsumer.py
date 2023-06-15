from kafka import KafkaConsumer
import sys
import json
import mysql.connector
import time
from datetime import datetime
bootstrap_servers = ['localhost:9092']
topicName = 'part_1'

consumer = KafkaConsumer (topicName,group_id = "group_1",bootstrap_servers = bootstrap_servers,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
mydb = mysql.connector.connect(
    host="localhost",
    user = "root",
    password="root",
    database = "orderdatabase"
)

cursor = mydb.cursor()

try:
    start_time = time.time()
    count = 0
    for message in consumer:
        count += 1
        if time.time() - start_time >= 10 or count>10:
            start_time = time.time()
            count=0
            consumer.commit()
            print("Committed to kafka")
            # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
        item = message.value
        columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in item.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in item.values())
        print(columns)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('orders', columns, values)
        # print(sql)
        cursor.execute(sql)
        mydb.commit()
except KeyboardInterrupt:
    consumer.commit()
    cursor.close()
    mydb.close()
    sys.exit()