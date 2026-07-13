import json
import time
import psycopg2
from kafka import KafkaConsumer

print("NIQS Telemetry Consumer başlatılıyor...")

# Docker iç ağı üzerinden PostgreSQL bağlantısı
DB_PARAMS = {
    "host": "postgres", 
    "database": "ram_metrics_db",
    "user": "ozer_user",
    "password": "ozer_password",
    "port": "5432"
}

def init_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ram_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    ram_usage_percent REAL NOT NULL
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("PostgreSQL bağlantısı başarılı ve tablo hazır.")
            break
        except Exception as e:
            print("PostgreSQL'e bağlanılamadı, 3 saniye sonra tekrar denenecek...", e)
            time.sleep(3)

init_db()

# Docker iç ağındaki direkt isim üzerinden Kafka bağlantısı
consumer = KafkaConsumer(
    'ram-metrics-topic',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Sunucu ve PostgreSQL Docker köprüsü kuruldu. Veri bekleniyor...")

for message in consumer:
    try:
        data = message.value
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ram_metrics (timestamp, ram_usage_percent) VALUES (%s, %s)",
            (data.get('timestamp'), data.get('ram_usage_percent'))
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Veri PostgreSQL'e kaydedildi: {data}")
    except Exception as e:
        print(f"Veri işlenirken hata oluştu: {e}")
