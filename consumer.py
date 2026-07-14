import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

print("NIQS Telemetry Consumer başlatılıyor...")

consumer = KafkaConsumer(
    'niqs-ram-telemetry',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname="ram_metrics_db",
    user="ozer_user",
    password="ozer_password",
    host="niqs_postgres"
)
cur = conn.cursor()

print("Kafka bağlantısı BAŞARILI! Veri yazılıyor...")

try:
    for message in consumer:
        data = message.value
        # Sayısal timestamp'i PostgreSQL'in anlayacağı tarih formatına çeviriyoruz
        dt_object = datetime.fromtimestamp(data['timestamp'])
        
        cur.execute(
            "INSERT INTO ram_metrics (timestamp, ram_usage_percent) VALUES (%s, %s)",
            (dt_object, data['percent_used'])
        )
        conn.commit()
        print(f"Yazıldı: {dt_object} -> %{data['percent_used']}")
except Exception as e:
    print(f"Hata: {e}")
finally:
    cur.close()
    conn.close()
