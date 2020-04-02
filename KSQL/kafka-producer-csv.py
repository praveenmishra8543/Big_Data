import json,csv
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9094'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
csvFilepath="/home/praveen/Downloads/SalesJan2009.csv"

# data =pd.read_csv("/home/praveen/Downloads/SalesJan2009.csv")
# j_data = data.to_json()
# print(j_data)

# for i in range(0,100):
while True:
    with open(csvFilepath) as csvFile:
        datas = {}
        csvReader = csv.DictReader(csvFile)
        print(csvReader.reader)
        print(type(csvReader))
        for row in csvReader:
            id = row["id"]
            datas[id] = row
        for each in datas.values():
            #print(json.dumps(each))
            #print("going for second json")
            producer.send('test',value=each)
