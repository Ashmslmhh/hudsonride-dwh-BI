"""DAG Airflow: pembaruan sumber eksternal sesuai jadwal rilis masing-masing (rancangan awal).

- hudsonride_weather_daily  : cuaca NOAA KCDW, harian pukul 03.00 America/New_York
                              (observasi per jam hari kemarin sudah lengkap)
- hudsonride_holiday_yearly : kalender hari libur, tahunan 2 Januari pukul 01.30

Keduanya memeriksa file sumber dan memperbarui raw zone (E2/E3) serta mencatat run di audit.etl_run.
DAG bulanan hudsonride_etl kemudian memakai raw zone terbaru ini. Pool `hudsonride` (1 slot) menjamin
task ketiga DAG tidak berjalan bersamaan.
"""
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from hudsonride_common import default_args


def _refresh(source, batch_id):
    from etl import pipeline as p
    p.init(batch_id, [])
    return p.refresh_source(source)


SOURCES = [
    # dag_id, sumber, prefix BATCH_ID, cron, keterangan
    ('hudsonride_weather_daily', 'weather', 'W', '0 3 * * *', 'Cuaca NOAA KCDW harian (E2)'),
    ('hudsonride_holiday_yearly', 'holiday', 'H', '30 1 2 1 *', 'Kalender hari libur tahunan (E3)'),
]

for dag_id, source, prefix, cron, desc in SOURCES:
    with DAG(
        dag_id=dag_id,
        description=desc,
        schedule=cron,
        start_date=pendulum.datetime(2026, 11, 1, tz='America/New_York'),
        catchup=False,
        max_active_runs=1,
        default_args=default_args(prefix),
        tags=['hudsonride', 'extract', source],
        doc_md=__doc__,
    ) as dag:
        PythonOperator(
            task_id=f'extract_{source}',
            python_callable=_refresh,
            op_kwargs={'source': source,
                       'batch_id': prefix + "{{ logical_date.in_timezone('UTC').strftime('%Y%m%d_%H%M%S') }}"},
        )
    globals()[dag_id] = dag
