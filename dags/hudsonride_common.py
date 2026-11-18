"""Bagian bersama DAG HudsonRide: retry, notifikasi email, dan pemanggilan modul ETL.

Notifikasi email aktif bila HR_ALERT_EMAIL dan SMTP_* diisi di .env:
  - task gagal setelah 3 kali retry  -> email GAGAL (nama task, BATCH_ID, pesan error, link log)
  - run selesai dengan peringatan DQ  -> email PERINGATAN
"""
import os
from datetime import timedelta

ALERT_EMAILS = [e.strip() for e in os.environ.get('HR_ALERT_EMAIL', '').split(',') if e.strip()]
POOL = 'hudsonride'      # 1 slot: task semua DAG HudsonRide tidak berjalan bersamaan (raw zone & DWH aman)


def batch_id(prefix, logical_date):
    return prefix + logical_date.in_timezone('UTC').strftime('%Y%m%d_%H%M%S')


def months(value):
    # kolom kosong di form Trigger dapat terkirim sebagai None / "None"; keduanya berarti full load
    return [m.strip() for m in str(value or '').split(',') if m.strip() and m.strip().lower() not in ('none', 'null')]


def send_alert(subject, lines):
    """Kirim email ke HR_ALERT_EMAIL; kegagalan kirim hanya dicatat agar tidak menutupi error asli."""
    if not ALERT_EMAILS:
        return
    try:
        from airflow.utils.email import send_email
        body = '<br>'.join(lines)
        send_email(to=ALERT_EMAILS, subject=f'[HudsonRide ETL] {subject}',
                   html_content=f'<div style="font-family:Arial,sans-serif;font-size:14px">{body}</div>')
    except Exception as e:  # noqa: BLE001
        print(f'Email notifikasi gagal dikirim: {e}')


def on_failure(prefix):
    """Callback setelah retry habis: tandai run FAILED di audit.etl_run lalu kirim email."""
    def _callback(context):
        ti = context['task_instance']
        bid = batch_id(prefix, context['logical_date'])
        error = context.get('exception')
        try:
            from etl import pipeline as p
            p.init(bid, months(context['params'].get('run_months')))
            p.fail(f'task {ti.task_id} gagal: {error}')
        except Exception as e:  # noqa: BLE001
            print(f'audit FAILED tidak dapat ditulis: {e}')
        send_alert(f'GAGAL: {ti.dag_id}.{ti.task_id} ({bid})', [
            f'<b>DAG</b>: {ti.dag_id}',
            f'<b>Task</b>: {ti.task_id} (tetap gagal setelah retry otomatis)',
            f'<b>BATCH_ID</b>: {bid}',
            f'<b>Error</b>: {str(error)[:1500]}',
            f'<b>Log</b>: <a href="{ti.log_url}">{ti.log_url}</a>',
            'Data warehouse tetap pada kondisi sebelum run (load dijalankan dalam satu transaksi).',
            f'Riwayat: SELECT * FROM audit.etl_run WHERE batch_id = \'{bid}\';',
        ])
    return _callback


def default_args(prefix):
    return {
        'owner': 'hudsonride-data',
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'pool': POOL,
        'email_on_failure': False,      # email dikirim oleh callback di atas (isi lebih informatif)
        'email_on_retry': False,
        'on_failure_callback': on_failure(prefix),
    }
