"""DAG Airflow: pipeline ETL HudsonRide Bike Share.

prepare -> extract -> transform -> load_dw -> dq_checks -> build_mart -> backup_dw -> finish

- Jadwal: bulanan tanggal 8 pukul 02.00 America/New_York (data perjalanan bulan sebelumnya
  dirilis pada minggu pertama).
- Retry otomatis 3 kali per task; bila tetap gagal, run ditandai FAILED di audit.etl_run.
- Parameter `run_months` (Trigger DAG w/ config): kosong = full load, "2024-12" = incremental.
- Semua task memakai BATCH_ID yang sama (dari logical date run), sehingga log, audit, file
  rejected, dan backup satu run dapat ditelusuri.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

BATCH_ID = "B{{ logical_date.in_timezone('UTC').strftime('%Y%m%d_%H%M%S') }}"
RUN_MONTHS = "{{ params.run_months }}"


def _months(value):
    return [m.strip() for m in (value or '').split(',') if m.strip()]


def _run_stage(stage, batch_id, run_months):
    from etl import pipeline as p          # impor di dalam task agar parsing DAG tetap ringan
    p.init(batch_id, _months(run_months))
    getattr(p, stage)()


def _mark_failed(context):
    from etl import pipeline as p
    ti = context['task_instance']
    batch_id = 'B' + context['logical_date'].in_timezone('UTC').strftime('%Y%m%d_%H%M%S')
    p.init(batch_id, _months(context['params'].get('run_months')))
    p.fail(f"task {ti.task_id} gagal: {context.get('exception')}")


default_args = {
    'owner': 'hudsonride-data',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'on_failure_callback': _mark_failed,
}

with DAG(
    dag_id='hudsonride_etl',
    description='ETL HudsonRide: sumber -> raw zone -> staging -> DWH galaxy schema -> DQ -> data mart -> backup',
    schedule='0 2 8 * *',
    start_date=pendulum.datetime(2026, 11, 1, tz='America/New_York'),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={'run_months': Param('', type='string',
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
        ('finish', 'Tandai run SUCCESS di audit.etl_run'),
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
