# Folder data

Folder ini tidak di-commit ke GitHub (lihat `.gitignore`). Salin data sumber ke sini sebelum
menjalankan pipeline, dengan struktur yang sama seperti folder `UTS_BI` di Google Drive:

```
data/
├── Data_CitiBike/            12 file perjalanan JC-202401 ... JC-202412-citibike-tripdata.xlsx
├── Data_Weather.xlsx         NOAA Global Hourly stasiun KCDW (72409454743), 2024
├── Data_Holiday.xlsx         Kaggle Global Public Holidays and Bank Holidays 2020-2026
└── pipeline/                 dibuat otomatis: raw, clean, rejected, logs, backup, sql
```

| Data | Sumber |
|---|---|
| Riwayat perjalanan Citi Bike Jersey City 2024 | https://citibikenyc.com/system-data (file `JC-2024MM-citibike-tripdata`) |
| Cuaca per jam NOAA KCDW | https://www.ncei.noaa.gov/access/search/data-search/global-hourly |
| Hari libur | Kaggle: Global Public Holidays and Bank Holidays 2020-2026 |
| Tarif HudsonRide | Asumsi, mengacu tarif Citi Bike Januari 2024 (tersimpan di `dim_user_type`) |

