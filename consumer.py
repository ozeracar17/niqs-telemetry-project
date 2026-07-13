import json
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

print("NIQS Telemetry Consumer başlatılıyor...")

# Docker içindeki Kafka'ya sunucu üzerinden (172.17.0.1 köprüsüyle) bağlanıyoruz
consumer = KafkaConsumer(
    'niqs-ram-telemetry',
    bootstrap_servers=['172.17.0.1:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    bootstrap_timeout_ms=10000
)

# Docker içindeki Elasticsearch'e sunucu üzerinden bağlanıyoruz
es = Elasticsearch(["http://127.0.0.1:9200"])
print("Sunucu ve Docker köprüsü kuruldu. Veri bekleniyor...")

for message in consumer:
    payload = message.value
    print(f"Kafka'dan veri alındı: {payload}")
    if 'timestamp' in payload:
        raw_time = payload['timestamp']
        payload['timestamp'] = datetime.fromtimestamp(raw_time).isoformat()
    try:
        res = es.index(index='ram-metrics', document=payload)
        print(f"Veri Elasticsearch'e başarıyla kaydedildi. ID: {res['_id']}")
    except Exception as e:
        print(f"Kayıt hatası: {e}")
