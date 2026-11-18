"""DAG Airflow: pipeline ETL bulanan HudsonRide Bike Share.

prepare -> extract -> transform -> load_dw -> dq_checks -> build_mart -> backup_dw -> finish

- Jadwal: bulanan tanggal 8 pukul 02.00 America/New_York (data perjalanan bulan sebelumnya
  dirilis pada minggu pertama). Cuaca dan hari libur diperbarui oleh DAG terpisah
  (hudsonride_weather_daily, hudsonride_holiday_yearly).
- Retry otomatis 3 kali per task; bila tetap gagal, run ditandai FAILED di audit.etl_run
  dan email notifikasi dikirim ke HR_ALERT_EMAIL.
- Parameter `run_months` (Trigger DAG w/ config): kosong = full load, "2024-12" = incremental.
- Semua task memakai BATCH_ID yang sama (dari logical date run), sehingga log, audit, file
  rejected, dan backup satu run dapat ditelusuri.
"""
import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from hudsonride_common import batch_id, default_args, months, send_alert

BATCH_ID = "B{{ logical_date.in_timezone('UTC').strftime('%Y%m%d_%H%M%S') }}"
RUN_MONTHS = "{{ params.run_months }}"


def _run_stage(stage, batch_id, run_months):
    from etl import pipeline as p          # impor di dalam task agar parsing DAG tetap ringan
    p.init(batch_id, months(run_months))
    result = getattr(p, stage)()
    if stage == 'finish' and result != 'SUCCESS':
        st = p.state()
        send_alert(f'PERINGATAN: run {batch_id} selesai dengan peringatan DQ', [
            f'<b>BATCH_ID</b>: {batch_id}',
            f'<b>Status</b>: {result}',
            f"<b>Baris</b>: extracted {st.get('extracted')}, rejected {st.get('rejected')}, loaded {st.get('loaded')}",
            f"Detail: SELECT * FROM audit.dq_result WHERE batch_id = '{batch_id}' AND NOT passed;",
        ])
    return result


with DAG(
    dag_id='hudsonride_etl',
    description='ETL HudsonRide: sumber -> raw zone -> staging -> DWH galaxy schema -> DQ -> data mart -> backup',
    schedule='0 2 8 * *',
    start_date=pendulum.datetime(2026, 11, 1, tz='America/New_York'),
    catchup=False,
    max_active_runs=1,
    default_args=default_args('B'),
    params={'run_months': Param('', type=['null', 'string'],
                                description='Kosong = full load; contoh incremental: 2024-12 atau 2024-11,2024-12')},
    tags=['hudsonride', 'etl', 'dwh'],
    doc_md=__doc__,
) as dag:
    stages = [
        ('prepare', 'Buat/periksa schema dan catat run di audit.etl_run'),
        ('extract', 'E1-E3: baca file sumber ke raw zone (+ _manifest.json)'),
        ('transform', 'T1-T17, validasi pra-load V1, tulis staging S1'),
        ('load', 'L1: 02_load_dw.sql dalam satu transaksi + rekonsiliasi'),
        ('dq', 'Q1: 03_dq_checks.sql; gagal bila ada CRITICAL'),
        ('mart', 'M1: 05_datamart.sql, materialized view per stakeholder'),
        ('backup', 'B1: pg_dump -Fc, retensi 7 file'),
        ('finish', 'Tandai run SUCCESS di audit.etl_run; email bila ada peringatan DQ'),
    ]
    task_ids = {'load': 'load_dw', 'dq': 'dq_checks', 'mart': 'build_mart', 'backup': 'backup_dw'}
    previous = None
    for stage, doc in stages:
        task = PythonOperator(
            task_id=task_ids.get(stage, stage),
            python_callable=_run_stage,
            op_kwargs={'stage': stage, 'batch_id': BATCH_ID, 'run_months': RUN_MONTHS},
            doc_md=doc,
        )
        if previous is not None:
            previous >> task
        previous = task
