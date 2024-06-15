from flask import Flask, request
from kafka import KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/')
def index():
    return 'Hello, World!', 200

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print("Webhook received:", data)  
    producer.send('shopify_orders', data)
    return '', 200

if __name__ == '__main__':
    app.run(port=5000)
