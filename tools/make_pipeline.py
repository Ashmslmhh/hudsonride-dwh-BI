"""Bangun etl/pipeline.py dari sel kode notebook.

Pemakaian:  python tools/make_pipeline.py notebooks/ETL_HudsonRide_DWH.ipynb etl/pipeline.py

Sel 3-10 (fungsi database, SQL, extract, transform, validasi, load/DQ/backup) disalin apa adanya,
sehingga logika repo identik dengan notebook yang sudah teruji. Yang diganti hanya:
  - sel 1 (konfigurasi)  -> dibaca dari environment variable
  - sel 2 (lingkungan)   -> fungsi init() tanpa Google Drive
  - sel 11 (run_pipeline) -> tahap terpisah untuk Airflow + run_all() untuk CLI
"""
import re
import sys

NB_SRC, OUT = sys.argv[1], sys.argv[2]
if NB_SRC.endswith('.ipynb'):
    import json
    nb = json.load(open(NB_SRC, encoding='utf-8'))
    code = [''.join(c['source']) + '\n' for c in nb['cells'] if c['cell_type'] == 'code']
else:
    src = open(NB_SRC, encoding='utf-8').read()
    parts = re.split(r'^### (MD|CODE)\n', src, flags=re.M)
    cells = [(parts[i], parts[i + 1]) for i in range(1, len(parts), 2)]
    code = [c for t, c in cells if t == 'CODE']
assert code[0].startswith('import os') and code[10].startswith('def run_pipeline'), 'urutan sel berubah'

# ---------------------------------------------------------------- sel 1: konfigurasi
cfg = code[0]
REPL = [
    ("import os\n", "import os\nfrom pathlib import Path\n\n\ndef _env_bool(name, default):\n"
                    "    return os.environ.get(name, '1' if default else '0').strip().lower() in ('1', 'true', 'yes')\n\n\n"
                    "def _env_months(name):\n"
                    "    v = os.environ.get(name, '').strip()\n"
                    "    return [m.strip() for m in v.split(',') if m.strip()] or None\n\n"),
    ("# Lokasi data di Google Drive\nBASE_DIR      = os.environ.get('HR_BASE_DIR', '/content/drive/MyDrive/UTS_BI')",
     "# Lokasi data: folder data/ di repository (di container Airflow: /opt/airflow/data)\n"
     "BASE_DIR      = os.environ.get('HR_BASE_DIR', str(Path(__file__).resolve().parents[1] / 'data'))"),
    ("PIPELINE_DIR  = f'{BASE_DIR}/pipeline'", "PIPELINE_DIR  = os.environ.get('HR_PIPELINE_DIR', f'{BASE_DIR}/pipeline')"),
    ("RUN_MONTHS          = None    #", "RUN_MONTHS          = _env_months('HR_RUN_MONTHS')    #"),
    ("USE_RAW_CACHE       = True    #", "USE_RAW_CACHE       = _env_bool('HR_USE_RAW_CACHE', True)    #"),
    ("RUN_BACKUP          = True", "RUN_BACKUP          = _env_bool('HR_RUN_BACKUP', True)"),
    ("RUN_DR_DRILL        = False   #", "RUN_DR_DRILL        = False   # di repo dijalankan terpisah: python -m etl dr-drill;"),
    ("DB_MODE = os.environ.get('HR_DB_MODE', 'colab')", "DB_MODE = os.environ.get('HR_DB_MODE', 'external')"),
]
for old, new in REPL:
    assert cfg.count(old) == 1, old
    cfg = cfg.replace(old, new)

# ---------------------------------------------------------------- sel 2: lingkungan -> init()
env = '''import glob, io, json, logging, re, secrets, shutil, subprocess, sys, time
from contextlib import contextmanager
from datetime import datetime

import numpy as np
import pandas as pd

try:
    from IPython.display import display
except ImportError:
    display = print

IN_COLAB = False
DIRS = {k: f'{PIPELINE_DIR}/{k}' for k in ('raw', 'clean', 'rejected', 'logs', 'backup', 'sql')}
BATCH_ID = None
LOG_FILE = None
SQL_PATHS = {}

logger = logging.getLogger('hudsonride_etl')
logger.setLevel(logging.INFO)
logger.propagate = False
_fmt = logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', '%Y-%m-%d %H:%M:%S')


def n(x):
    """Format angka ribuan untuk log."""
    return f'{x:,}' if isinstance(x, (int, np.integer)) else str(x)


def init(batch_id=None, run_months=None):
    """Siapkan folder, BATCH_ID, logging, file SQL, dan koneksi database untuk satu proses."""
    global BATCH_ID, LOG_FILE, RUN_MONTHS
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)
    BATCH_ID = batch_id or datetime.now().strftime('B%Y%m%d_%H%M%S')
    if run_months is not None:
        RUN_MONTHS = run_months or None
    LOG_FILE = f"{DIRS['logs']}/etl_{BATCH_ID}.log"
    logger.handlers.clear()
    for h in (logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler(sys.stdout)):
        h.setFormatter(_fmt)
        logger.addHandler(h)
    write_sql_files()
    setup_database()
    return BATCH_ID
'''

# ---------------------------------------------------------------- sel 4: penulisan file SQL -> fungsi
sqlcell = code[3]
tail_old = '''SQL_PATHS = {}
for _name, _body in {'01_schema.sql': render_schema_sql(), '02_load_dw.sql': LOAD_SQL, '03_dq_checks.sql': DQ_SQL}.items():
    SQL_PATHS[_name] = f"{DIRS['sql']}/{_name}"
    Path(SQL_PATHS[_name]).write_text(_body.lstrip(), encoding='utf-8')
logger.info(f"File SQL ditulis ke {DIRS['sql']}: {', '.join(SQL_PATHS)}")
'''
assert sqlcell.count(tail_old) == 1
sqlcell = sqlcell.replace(tail_old, '''
REPO_SQL_DIR = Path(__file__).resolve().parents[1] / 'sql'


def write_sql_files():
    bodies = {'01_schema.sql': render_schema_sql(), '02_load_dw.sql': LOAD_SQL, '03_dq_checks.sql': DQ_SQL}
    for name, body in bodies.items():
        SQL_PATHS[name] = f"{DIRS['sql']}/{name}"
        Path(SQL_PATHS[name]).write_text(body.lstrip(), encoding='utf-8')
    SQL_PATHS['05_datamart.sql'] = str(REPO_SQL_DIR / '05_datamart.sql')
''')

# ---------------------------------------------------------------- tahap untuk Airflow / CLI
stages = r'''
# =====================================================================================
# Tahap pipeline. Notebook menjalankan semuanya dalam satu fungsi (run_pipeline);
# di repo setiap tahap menjadi task Airflow terpisah dengan BATCH_ID yang sama.
# Ringkasan antartahap disimpan di pipeline/logs/state_<BATCH_ID>.json.
# =====================================================================================

def audit_run_start(run_mode, months):
    # idempotent: aman bila task Airflow di-retry
    psql(f"INSERT INTO audit.etl_run (batch_id, status, run_mode, months) "
         f"VALUES ({lit(BATCH_ID)}, 'RUNNING', {lit(run_mode)}, {lit(months)}) "
         f"ON CONFLICT (batch_id) DO UPDATE SET status = 'RUNNING', finished_at = NULL, message = NULL")


def _state_path():
    return f"{DIRS['logs']}/state_{BATCH_ID}.json"


def state(**updates):
    data = {}
    if os.path.exists(_state_path()):
        data = json.loads(Path(_state_path()).read_text(encoding='utf-8'))
    if updates:
        data.update(updates)
        Path(_state_path()).write_text(json.dumps(data, indent=1), encoding='utf-8')
    return data


def prepare():
    run_mode = 'full' if RUN_MONTHS is None else 'incremental'
    psql(file=SQL_PATHS['01_schema.sql'])
    audit_run_start(run_mode, 'all' if RUN_MONTHS is None else ','.join(RUN_MONTHS))
    state(run_mode=run_mode, run_months=RUN_MONTHS, extracted=None, rejected=None, loaded=None, dq_all_passed=None)
    logger.info(f'RUN {BATCH_ID} dimulai | mode {run_mode} | bulan {RUN_MONTHS or "semua"}')


def extract():
    with step('E1 extract trip') as s:
        raw_trip, trip_files = extract_trips()
        s['rows_out'] = len(raw_trip)
        s['message'] = f"{len(trip_files)} file ({', '.join(trip_files)})"
    with step('E2 extract cuaca NOAA') as s:
        raw_weather, weather_files = extract_weather()
        s['rows_out'] = len(raw_weather)
        s['message'] = ', '.join(Path(f).name for f in weather_files)
    with step('E3 extract hari libur') as s:
        raw_holiday, holiday_files = extract_holiday()
        s['rows_out'] = len(raw_holiday)
        s['message'] = Path(holiday_files[0]).name
    state(extracted=len(raw_trip))


def _read_raw_zone():
    """Task transform berjalan di proses terpisah: data mentah dibaca ulang dari raw zone (cache extract)."""
    level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        return extract_trips()[0], extract_weather()[0], extract_holiday()[0]
    finally:
        logger.setLevel(level)


def transform():
    raw_trip, raw_weather, raw_holiday = _read_raw_zone()
    stats = {}
    with step('T1-T8 transform trip') as s:
        trip, rejected, st = transform_trips(raw_trip)
        stats.update(st)
        s['rows_in'], s['rows_out'], s['rows_rejected'] = len(raw_trip), len(trip), len(rejected)
        if len(rejected):
            s['message'] = ', '.join(f'{r}:{c:,}' for r, c in rejected['reject_reason'].value_counts().items())
    with step('T9 transform stasiun (SCD2)') as s:
        stations, st = build_stations(trip)
        stats.update(st)
        s['rows_out'] = len(stations)
        s['message'] = f"{st['stations']} stasiun, {st['station_versions']} versi"

    cal_start = trip['started_at'].min().normalize()
    cal_end = trip['ended_at'].max().normalize()
    trip_years = sorted(trip['started_at'].dt.year.unique().tolist())

    with step('T10-T15 transform cuaca NOAA') as s:
        weather, st = transform_weather(raw_weather, cal_start, cal_end)
        stats.update(st)
        s['rows_in'], s['rows_out'] = len(raw_weather), len(weather)
        s['message'] = f"{st['weather_unknown_hours']} jam Unknown"
    with step('T16-T17 transform hari libur dan kalender') as s:
        dates, st = transform_calendar(raw_holiday, cal_start, cal_end, trip_years)
        stats.update(st)
        s['rows_in'], s['rows_out'] = len(raw_holiday), len(dates)
        s['message'] = f"{st['holidays']} hari libur pada tahun data"
    with step('V1 validasi pra-load') as s:
        n_checks = validate_before_load(trip, stations, weather, dates, len(raw_trip), len(rejected))
        compare_baseline(stats)
        s['message'] = f'{n_checks} pemeriksaan lolos'
    with step('S1 tulis staging CSV') as s:
        s['rows_out'] = write_staging(trip, stations, weather, dates, rejected)
        s['message'] = DIRS['clean']
    state(rejected=len(rejected), staged=len(trip))


def load():
    with step('L1 load data warehouse') as s:
        load_dw()
        loaded = int(query(f'SELECT count(*) AS n FROM dw.fact_trip WHERE etl_batch_id = {lit(BATCH_ID)}').iloc[0, 0])
        s['rows_in'], s['rows_out'] = state().get('staged'), loaded
        counts = dw_counts()
        s['message'] = ' | '.join(f'{r.tabel}={r.baris:,}' for r in counts.itertuples() if r.tabel.startswith('dw.fact'))
    state(loaded=loaded)


def dq():
    with step('Q1 data quality checks') as s:
        res = run_dq()
        s['rows_out'] = len(res)
        s['message'] = f"{int(res['passed'].sum())}/{len(res)} lolos"
        critical = res[(~res['passed']) & (res['severity'] == 'CRITICAL')]
        if len(critical):
            raise ValueError(f"DQ CRITICAL gagal: {critical['check_name'].tolist()}")
    state(dq_all_passed=bool(res['passed'].all()))


def mart():
    with step('M1 data mart (view per stakeholder)') as s:
        psql(file=SQL_PATHS['05_datamart.sql'])
        objs = query("SELECT count(*) FILTER (WHERE c.relkind = 'v') AS v, count(*) FILTER (WHERE c.relkind = 'm') AS m "
                     "FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace WHERE s.nspname = 'mart'").iloc[0]
        s['rows_out'] = int(objs['v'] + objs['m'])
        s['message'] = f"{int(objs['m'])} materialized view + {int(objs['v'])} view di schema mart"


def backup():
    if not RUN_BACKUP:
        logger.info('Backup dilewati (HR_RUN_BACKUP = 0)')
        return
    with step('B1 backup database') as s:
        path, size_mb, secs, removed = backup_dw()
        s['message'] = f'{Path(path).name} ({size_mb:.1f} MB, {secs:.1f} s, {removed} backup lama dihapus)'


def finish():
    st = state()
    status = 'SUCCESS' if st.get('dq_all_passed') else 'SUCCESS_WITH_WARNINGS'
    audit_run_end(status, st.get('extracted'), st.get('rejected'), st.get('loaded'))
    logger.info(f"RUN {status} | extracted {n(st.get('extracted'))} | rejected {n(st.get('rejected'))} | "
                f"loaded {n(st.get('loaded'))}")
    return status


def fail(message):
    st = state()
    try:   # pastikan run tercatat walaupun gagal sebelum tahap prepare selesai
        run_mode = 'full' if RUN_MONTHS is None else 'incremental'
        psql(f"INSERT INTO audit.etl_run (batch_id, status, run_mode, months) VALUES ({lit(BATCH_ID)}, 'FAILED', "
             f"{lit(run_mode)}, {lit('all' if RUN_MONTHS is None else ','.join(RUN_MONTHS))}) ON CONFLICT (batch_id) DO NOTHING")
    except Exception as e:
        logger.warning(f'audit.etl_run tidak dapat ditulis: {e}')
    audit_run_end('FAILED', st.get('extracted'), st.get('rejected'), st.get('loaded'), str(message)[:4000])
    logger.error(f'RUN FAILED | {message} | log: {LOG_FILE}')


def _ensure_schema():
    t = query("SELECT coalesce(to_regclass('audit.etl_run')::text, '') AS t").iloc[0, 0]
    if not isinstance(t, str) or not t:
        psql(file=SQL_PATHS['01_schema.sql'])


def refresh_source(source):
    """Jadwal terpisah per sumber (rancangan awal): cuaca harian, hari libur tahunan.
    Memeriksa file sumber dan memperbarui raw zone; DAG bulanan memakai raw zone terbaru ini."""
    steps = {'weather': ('E2 extract cuaca NOAA', extract_weather),
             'holiday': ('E3 extract hari libur', extract_holiday)}
    name, fn = steps[source]
    _ensure_schema()
    audit_run_start(f'refresh_{source}', 'all')
    try:
        with step(name) as s:
            raw, files = fn()
            s['rows_out'] = len(raw)
            s['message'] = ', '.join(Path(f).name for f in files)
        audit_run_end('SUCCESS', len(raw), None, None, f'raw zone {source} diperbarui')
        logger.info(f'REFRESH {source} SUCCESS | {len(raw):,} baris')
        return len(raw)
    except Exception as e:
        audit_run_end('FAILED', None, None, None, f'{type(e).__name__}: {e}'[:4000])
        logger.error(f'REFRESH {source} FAILED | {type(e).__name__}: {e}')
        raise


def run_all(run_months=None, batch_id=None):
    """Seluruh tahap dalam satu proses (setara run_pipeline() di notebook)."""
    init(batch_id, run_months)
    prepare()
    try:
        for stage in (extract, transform, load, dq, mart, backup):
            stage()
        return finish()
    except Exception as e:
        fail(f'{type(e).__name__}: {e}')
        raise
'''

header = '''"""HudsonRide Bike Share - pipeline ETL ke data warehouse PostgreSQL.

Berkas ini DIHASILKAN dari notebook notebooks/ETL_HudsonRide_DWH.ipynb (sel 3-10 disalin apa adanya),
sehingga aturan T1-T17, load, DQ, dan backup identik dengan notebook. Jangan diedit manual:
ubah notebook lalu jalankan ulang tools/make_pipeline.py.

Pemakaian:  python -m etl run [--months 2024-12]    |    python -m etl dr-drill
"""
'''

body = [header, cfg.rstrip() + '\n\n', env, '\n', code[2], '\n', sqlcell, '\n']
for c in code[4:10]:
    body.append(c.rstrip() + '\n\n')
body.append(stages)
out = ''.join(body)
open(OUT, 'w', encoding='utf-8').write(out)
compile(out, OUT, 'exec')
print('ditulis', OUT, len(out.splitlines()), 'baris')
