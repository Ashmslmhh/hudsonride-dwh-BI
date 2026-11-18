# HudsonRide Bike Share – Data Warehouse dan Dashboard BI

Proyek UAS Kecerdasan Bisnis (Bidang Manajemen Teknologi Informasi), Universitas Lambung Mangkurat.
Pipeline ETL memuat data perjalanan Citi Bike Jersey City 2024, cuaca per jam NOAA, dan kalender hari
libur ke data warehouse PostgreSQL berbentuk **galaxy schema** (6 dimensi, 3 fakta), lalu menyajikannya
melalui data mart per stakeholder.

```
Data sumber (data/) ──► raw zone ──► transform T1–T17 ──► staging (stg) ──► DWH (dw) ──► data mart (mart) ──► dashboard
                         _manifest     Python/pandas        CSV + \copy       1 transaksi     materialized view
                                                                            + rekonsiliasi
   Airflow DAG hudsonride_etl: prepare → extract → transform → load_dw → dq_checks → build_mart → backup_dw → finish
```

## Isi repository

| Folder/berkas | Isi |
|---|---|
| `notebooks/ETL_HudsonRide_DWH.ipynb` | Notebook ETL untuk Google Colab (sumber logika ETL) |
| `etl/pipeline.py` | Modul ETL yang **dihasilkan dari notebook** (`tools/make_pipeline.py`); logika identik |
| `etl/__main__.py` | CLI: `python -m etl run`, `python -m etl run --months 2024-12`, `python -m etl dr-drill` |
| `dags/hudsonride_etl.py` | DAG Airflow: jadwal bulanan, retry 3×, run gagal ditandai FAILED di `audit.etl_run` |
| `sql/01_schema.sql` … `03_dq_checks.sql` | Schema stg/dw/audit, load satu transaksi, 14 pemeriksaan kualitas data |
| `sql/04_rbac.sql` | Role `hr_admin`, `hr_etl`, `hr_analyst`, `hr_dashboard` dan hak aksesnya |
| `sql/05_datamart.sql` | Data mart: `trip_daily`, `station_hourly`, `weather_hourly`, `route_monthly`, `kpi_monthly`, `dq_latest` |
| `sql/hudsonride_dw_schema*.sql` | DDL schema dw (PostgreSQL) dan versi MySQL Workbench untuk menggambar ERD |
| `docker-compose.yml`, `docker/` | PostgreSQL 16, pgAdmin 4, Airflow 2.10 (LocalExecutor) |
| `data/` | Tempat data sumber (tidak di-commit), lihat `data/README.md` |

## Menjalankan dengan Docker (Windows 10/11)

Prasyarat: Docker Desktop (WSL 2) berjalan, Git, RAM ≥ 8 GB.

1. **Siapkan data.** Salin `Data_CitiBike/`, `Data_Weather.xlsx`, dan `Data_Holiday.xlsx` dari Google Drive
   ke folder `data/` (lihat `data/README.md`).
2. **Buat file `.env`.** Di PowerShell, dari folder repository:
   ```powershell
   Copy-Item .env.example .env
   notepad .env        # ganti semua password
   ```
3. **Nyalakan stack.** Build pertama memerlukan sekitar 5–10 menit:
   ```powershell
   docker compose up -d --build
   docker compose ps   # postgres & pgadmin "healthy/running", airflow-init "exited (0)"
   ```
4. **Jalankan ETL lewat Airflow.** Buka http://localhost:8080 (login sesuai `AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD`),
   aktifkan DAG `hudsonride_etl`, lalu klik **Trigger DAG**. Untuk incremental pilih **Trigger DAG w/ config**
   dan isi `run_months` = `2024-12`. Lihat progres di tab **Graph**; log tiap task ada di tab **Logs**.
5. **Lihat database di pgAdmin.** Buka http://localhost:5050, server **HudsonRide DWH** sudah terdaftar
   (password = `POSTGRES_PASSWORD`). Schema: `stg`, `dw`, `audit`, `mart`.

### Perintah lain

```powershell
# ETL langsung tanpa Airflow (di dalam container)
docker compose exec airflow-scheduler python -m etl run
docker compose exec airflow-scheduler python -m etl run --months 2024-12

# DR drill: restore backup terbaru ke database uji dan ukur RTO (butuh hak DBA)
docker compose exec -e PGUSER=hr_admin -e PGPASSWORD=<POSTGRES_PASSWORD> airflow-scheduler python -m etl dr-drill

# Uji hak akses: dashboard hanya boleh membaca schema mart
docker compose exec postgres psql -U hr_dashboard -d hudsonride_dw -c "SELECT * FROM mart.kpi_monthly LIMIT 3;"
docker compose exec postgres psql -U hr_dashboard -d hudsonride_dw -c "SELECT count(*) FROM dw.fact_trip;"   # ditolak

# Hentikan stack (data tetap tersimpan di volume)
docker compose down
# Hapus semua data dan mulai dari nol (role dibuat ulang dari .env)
docker compose down -v
```

Hasil pipeline tersimpan di `data/pipeline/`: `raw/` (salinan sumber + manifest), `clean/` (staging CSV),
`rejected/` (baris ditolak beserta aturannya), `logs/` (log per BATCH_ID), `backup/` (pg_dump, 7 terakhir).

## Hak akses (RBAC)

| Role | Pengguna | Hak |
|---|---|---|
| `hr_admin` | DBA | Superuser container: manajemen role, restore, DR drill |
| `hr_etl` | Airflow / pipeline | Pemilik schema `stg`, `dw`, `audit`, `mart` di `hudsonride_dw`; tanpa superuser, CREATEDB, CREATEROLE |
| `hr_analyst` | Data Analyst, Data Steward | SELECT pada `dw`, `audit`, `mart` |
| `hr_dashboard` | Aplikasi dashboard | SELECT hanya pada schema `mart` |

## Menjalankan di Google Colab

Upload `notebooks/ETL_HudsonRide_DWH.ipynb`, letakkan data di `MyDrive/UTS_BI`, lalu **Runtime → Run all**.
Notebook memasang PostgreSQL di runtime Colab dan menulis hasil ke `MyDrive/UTS_BI/pipeline`.

## Mengubah logika ETL

Notebook adalah sumber logika. Setelah mengubah notebook, bangun ulang modul:

```powershell
python tools/make_pipeline.py notebooks/ETL_HudsonRide_DWH.ipynb etl/pipeline.py
```

## Masalah umum

| Gejala | Penyelesaian |
|---|---|
| `port is already allocated` pada 5432 | Ada PostgreSQL lain di laptop. Isi `PG_PORT=5433` di `.env` |
| Airflow lambat / container berhenti | Tutup aplikasi berat; Docker Desktop → Settings → Resources, beri memori ≥ 6 GB |
| `Sumber trip tidak lengkap: 11 file` | Salah satu file `JC-2024MM` belum ada di `data/Data_CitiBike` |
| Password di `.env` diganti setelah stack pernah jalan | Role dibuat sekali saat volume baru; jalankan `docker compose down -v` lalu `up` lagi |
| `/bin/bash^M: bad interpreter` | Skrip tersimpan dengan akhir baris CRLF; `git config core.autocrlf false` lalu clone ulang |

## Sumber data dan lisensi

Data perjalanan © Lyft Bikes and Scooters (Citi Bike System Data), cuaca © NOAA NCEI, hari libur dari Kaggle.
Tarif dan estimasi pendapatan adalah **asumsi** untuk keperluan analisis.
