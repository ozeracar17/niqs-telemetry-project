# 🚀 NIQS Real-Time Telemetry & Monitoring Pipeline

Bu proje; sunucu sistemlerinin kaynak kullanımlarını (RAM) saniyesi saniyesine izleyen, kritik eşik aşımlarında proaktif uyarılar üreten ve kendi kendini temizleyen (Data Retention) uçtan uca bir telemetri mimarisidir. 

Özellikle yüksek erişilebilirlik (High Availability) gerektiren servislerde, olası performans kayıplarını sistem çökmeden önce tespit etmek amacıyla tasarlanmıştır.

## 🏗️ Sistem Mimarisi ve Veri Akışı (Data Pipeline)

Sistem, endüstri standartlarında bir mikroservis mimarisiyle kurgulanmış olup 4 temel aşamadan oluşur:

1. **Producer (Sensör):** Python tabanlı script, sunucunun anlık RAM kullanımını saniyede bir okur ve Kafka'ya iletir.
2. **Apache Kafka (Veri Otobanı):** Veritabanına doğrudan yük bindirmemek ve anlık veri yığılmalarını (spike) tolere etmek için mesaj kuyruğu (message broker) olarak konumlandırılmıştır.
3. **Consumer (Yazman):** Kafka'daki mesajları tüketerek kalıcı depolama için PostgreSQL veritabanına aktarır.
4. **Grafana (NOC Ekranı):** Veritabanındaki zaman serisi (time-series) verilerini saniyelik olarak görselleştirir ve proaktif alarmlar üretir.

## 🛠️ Kullanılan Teknolojiler
* **Altyapı:** Docker & Docker Compose
* **Message Broker:** Apache Kafka & Zookeeper
* **Veritabanı:** PostgreSQL
* **Görselleştirme & Alerting:** Grafana
* **Veri Üretimi & Tüketimi:** Python

## 🛡️ Otomatik Kaynak Yönetimi (Data Retention)
Sistemin kendi diskinin dolmasını ve kapasite sınırına ulaşmasını engellemek için proaktif bir temizlik mekanizması kurulmuştur. `consumer.py` servisi her yeni veri yazımında, **24 saatten eski olan verileri otomatik olarak silerek** sistemin sonsuz bir döngüde, sıfır bakım maliyetiyle çalışmasını garanti eder.

## 🚨 Proaktif Uyarı Sistemi (Alerting)
Sistem yöneticilerinin ekran başında olmadığı durumlar için Grafana Alerting entegrasyonu aktiftir. RAM kullanımı 3 dakika boyunca **%85 kritik eşiğini** aştığında, Telegram API üzerinden doğrudan operasyon ekibine anlık acil durum bildirimi (Notification) düşmektedir.

---

## 💻 Projeyi Yerelde Çalıştırma

Projeyi kendi bilgisayarınızda veya sunucunuzda izole bir şekilde ayağa kaldırmak için aşağıdaki adımları sırasıyla uygulayabilirsiniz:

### 1. Depoyu Klonlayın

```bash
git clone [https://github.com/ozeracar17/niqs-telemetry-project.git](https://github.com/ozeracar17/niqs-telemetry-project.git)
cd niqs-telemetry-project
```

### 2. Ağır Sistemleri (Altyapıyı) Başlatın

```bash
docker-compose up -d
```

### 3. Python Servislerini (Veri Akışını) Uyandırın

```bash
# Veri üreticisini başlat (Sensör)
docker start niqs_producer_live

# Veri tüketicisini başlat (Veritabanı Yazmanı)
docker start niqs_consumer_live
```

### 4. Grafana Dashboard Kurulumu

Tarayıcınızdan `http://localhost:3000` adresine gidin. Kullanıcı adı ve şifreyle (`admin` / `admin`) giriş yapın. PostgreSQL veritabanınızı "Data Source" olarak ekledikten sonra, RAM metriklerini zaman serisi (Time series) olarak canlı izlemek ve alarm kurmak için yeni bir panel açıp aşağıdaki SQL sorgusunu çalıştırın:

```sql
SELECT 
  timestamp AS "time", 
  ram_usage_percent AS "RAM Kullanımı (%)" 
FROM ram_metrics 
ORDER BY timestamp ASC;
```

### 5. Telegram Alarm Entegrasyonu (Opsiyonel)
Projeyi kendi yerelinizde kurduktan sonra Telegram bildirimlerini aktif etmek isterseniz:
1. `@BotFather` üzerinden yeni bir bot oluşturup API Token alın.
2. `@userinfobot` üzerinden Chat ID'nizi öğrenin.
3. Grafana arayüzünden `Alerting -> Contact Points` kısmına giderek Telegram entegrasyonunu tanımlayın.
