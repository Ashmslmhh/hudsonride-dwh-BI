"""CLI pipeline HudsonRide.

  python -m etl run                      full load 12 bulan
  python -m etl run --months 2024-12     incremental per bulan
  python -m etl run --no-backup          tanpa pg_dump
  python -m etl dr-drill                 restore backup terbaru ke database uji (butuh hak CREATEDB, mis. hr_admin)
"""
import argparse
import os
import sys


def main(argv=None):
    ap = argparse.ArgumentParser(prog='python -m etl', description='Pipeline ETL HudsonRide Bike Share')
    sub = ap.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('run', help='jalankan seluruh pipeline')
    r.add_argument('--months', default='', help="bulan incremental, mis. 2024-12 atau 2024-11,2024-12")
    r.add_argument('--no-backup', action='store_true', help='lewati backup pg_dump')
    r.add_argument('--batch-id', default=None)
    sub.add_parser('dr-drill', help='uji pemulihan backup terbaru dan ukur RTO')
    args = ap.parse_args(argv)

    if getattr(args, 'no_backup', False):
        os.environ['HR_RUN_BACKUP'] = '0'
    from etl import pipeline as p   # impor setelah environment diatur

    if args.cmd == 'run':
        months = [m.strip() for m in args.months.split(',') if m.strip()]
        if args.no_backup:
            p.RUN_BACKUP = False
        status = p.run_all(run_months=months, batch_id=args.batch_id)
        return 0 if status.startswith('SUCCESS') else 1
    if args.cmd == 'dr-drill':
        p.init()
        rto = p.dr_drill()
        p.logger.info(f'RTO terukur: {rto:.1f} detik')
        return 0
    return 1


if __name__ == '__main__':
    sys.exit(main())
