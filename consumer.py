import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError

print("NIQS Telemetry Consumer başlatılıyor...")

DB_PARAMS = {
    "host": "127.0.0.1", 
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

# Hem dış IP hem localhost'u sırayla deneyecek dayanıklı Kafka bağlantısı
consumer = None
while consumer is None:
    for broker in ['194.62.54.28:9092', '127.0.0.1:9092']:
        try:
            print(f"Kafka broker deneniyor: {broker}")
            consumer = KafkaConsumer(
                'ram-metrics-topic',
                bootstrap_servers=[broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=5000
            )
            print(f"Kafka bağlantısı BAŞARILI: {broker}")
            break
        except KafkaTimeoutError:
            print(f"Kafka broker zaman aşımı verdi: {broker}")
        except Exception as e:
            print(f"Bağlantı hatası: {e}")
    
    if consumer is None:
        print("Kafka'ya bağlanılamadı. 5 saniye sonra tekrar denenecek...")
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
