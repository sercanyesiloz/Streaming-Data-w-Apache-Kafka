import time
import pandas as pd
from kafka import KafkaProducer
df = pd.read_csv('/home/train/datasets/iris.csv')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in df['Species'].unique():
    iris_df = df[df['Species'] == i]
    species = i
    print(species)
    message = [', '.join([iris_df.columns[k] + ': ' \
                          + str(iris_df.iloc[j,].values.tolist()[k]) \
                          for k in range(iris_df.shape[1])]) for j in range(iris_df.shape[0])]
    for key,value in enumerate(message):
        time.sleep(0.5)
        producer.send('topic1', value = value.encode(), key = str(key).encode())
        print('key: {}, value: {}'.format(key, value))

producer.flush()


