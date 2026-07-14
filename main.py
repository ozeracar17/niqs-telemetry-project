import time
import json
import psutil
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

print("NIQS RAM Producer başlatılıyor (Gerçek Zaman Modu)...")

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_timeout_ms=10000
)

topic_name = 'niqs-ram-telemetry'

# Türkiye Saati (UTC+3) için offset tanımlıyoruz
tr_timezone = timezone(timedelta(hours=3))

try:
    while True:
        mem = psutil.virtual_memory()
        
        # Sunucu saati yerine gerçek UTC+3 zaman damgasını hesaplıyoruz
        current_tr_time = datetime.now(timezone.utc).astimezone(tr_timezone)
        real_timestamp = int(current_tr_time.timestamp())
        
        payload = {
            'timestamp': real_timestamp,
            'total_gb': round(mem.total / (1024**3), 2),
            'used_gb': round(mem.used / (1024**3), 2),
            'percent_used': mem.percent
        }
        producer.send(topic_name, payload)
        print(f"Kafka'ya Veri Gönderildi: {payload}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Durduruldu.")
