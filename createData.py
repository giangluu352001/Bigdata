import string    
import random
from datetime import datetime
from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
  S = 10
  id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = S))    
  now = str(datetime.now())
  distance = random.uniform(0.1, 5)
  latitude = random.uniform(40, 43)
  longitude = random.uniform(-72, -70)
  source = ''.join(random.choices(string.ascii_uppercase + string.digits, k = S))
  destination = ''.join(random.choices(string.ascii_uppercase + string.digits, k = S))
  msg = {
    "id": id,
    "datetime": now,
    "distance": distance,
    "longitude": longitude,
    "latitude": latitude,
    "source": source,
    "destination": destination
  }
  producer.send("uber", json.dumps(msg).encode('utf-8'))
  time.sleep(2)