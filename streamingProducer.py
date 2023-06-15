from kafka import KafkaProducer
import pandas as pd
from datetime import datetime

#Connection for kafka
servers = ['localhost:9092']
#Defining the topic name we are working on
topicName = 'part_1'

#Defining KafkaProducer using the above connections
producer = KafkaProducer(bootstrap_servers = servers,value_serializer=lambda v: v.encode('ascii'))


# #Importing the data
df = pd.read_csv('part1.csv')
for i in range(df.shape[0]):
    # print(df.iloc[i])
    acknowledgement = producer.send(topicName,df.iloc[i].to_json())
    metaData = acknowledgement.get()
    print("Topic = ", metaData.topic)
    print("Partition Id = ",metaData.partition,"\n\n")
    # break


#Closing the producer Connection
producer.flush()
producer.close()