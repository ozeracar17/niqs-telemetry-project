SELECT 
  timestamp AS "time", 
  ram_usage_percent AS "RAM Kullanımı (%)" 
FROM ram_metrics 
ORDER BY timestamp ASC;
