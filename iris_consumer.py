from kafka import KafkaConsumer

consumer = KafkaConsumer('topic1',
                         group_id = 'group1',
                         bootstrap_servers=['localhost:9092'],
                         consumer_timeout_ms = 10000)

versicolor = []
setosa = []
virginica = []
other = []

for message in consumer:
    topic = 'Topic: ' + message.topic
    key = 'Key: ' + message.key.decode()
    value = message.value.decode()
    part = 'Partition: ' + str(message.partition)
    text = ', '.join([topic, key, value, part])
    print(text)
    text_dict = {i.split(':')[0].strip(): i.split()[1] for i in
                 [text.split(',')[i] for i in range(len(text.split(',')))]}

    if text_dict['Species'] == 'Iris-versicolor':
        versicolor.append(text)
    elif text_dict['Species'] == 'Iris-setosa':
        setosa.append(text)
    elif text_dict['Species'] == 'Iris-virginica':
        virginica.append(text)
    else:
        other.append(text)

if len(versicolor) > 0:
    with open('versicolor.txt', 'w') as f:
        f.write('\n'.join(versicolor))

if len(setosa) > 0:
    with open('setosa.txt', 'w') as f:
        f.write('\n'.join(setosa))

if len(virginica) > 0:
    with open('virginica.txt', 'w') as f:
        f.write('\n'.join(virginica))

if len(other) > 0:
    with open('other.txt', 'w') as f:
        f.write('\n'.join(other))

