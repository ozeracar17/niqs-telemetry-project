import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError

print("NIQS Telemetry Consumer başlatılıyor...")

# Docker iç ağında servisin adı 'postgres' olduğu için host'u postgres yapıyoruz
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

# Docker iç ağındaki direkt servis ismi ve iç port (29092) üzerinden kırılmaz bağlantı
consumer = None
while consumer is None:
    try:
        print("Docker iç ağı üzerinden Kafka'ya bağlanılıyor (kafka:29092)...")
        consumer = KafkaConsumer(
            'ram-metrics-topic',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            request_timeout_ms=5000
        )
        print("Kafka bağlantısı BAŞARILI!")
    except KafkaTimeoutError:
        print("Kafka zaman aşımı verdi, 5 saniye sonra tekrar denenecek...")
        time.sleep(5)
    except Exception as e:
        print(f"Bağlantı hatası: {e}")
        time.sleep(5)

print("Sunucu ve PostgreSQL köprüsü kuruldu. Canlı veri bekleniyor...")

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
        print(f"Veri PostgreSQL'e başarıyla kaydedildi: {data}")
    except Exception as e:
        print(f"Veri tabanına yazılırken hata oluştu: {e}")
