"""
=============================================================
 Bat Swab Excel → SQLite Importer (Notebook Version)
=============================================================
 Usage in notebook:
   from batswab_importer_notebook import run_import
   run_import('data.xlsx', 'surveillance.db', sheet=None, dry_run=True)
=============================================================
"""

import sqlite3
import sys
import re
from datetime import datetime, date
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)


# ── Excel serial date fix ─────────────────────────────────────────────────────
def parse_date(value):
    """Convert Excel serial, datetime object, or date string to 'YYYY-MM-DD'."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    # Already a datetime/date
    if isinstance(value, (datetime, date)):
        return str(value)[:10]
    # Numeric Excel serial date (e.g. 45797)
    if isinstance(value, (int, float)):
        try:
            dt = pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(value))
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None
    # String — try common formats
    val = str(value).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return val[:10] if len(val) >= 10 else None


def parse_source_id(raw):
    """
    SourceId like '45797<21:00F501' is:
      Excel_date < capture_time + bag_id
    Returns cleaned string — stored as-is in samples.source_id.
    """
    return str(raw).strip() if raw else None


def clean(value):
    """Return None for empty/NaN, else stripped string."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def extract_time(source_id):
    """
    Try to pull capture time from SourceId like '45797<21:00F501'
    Pattern: digits < HH:MM letters
    """
    if not source_id:
        return None
    m = re.search(r'<(\d{1,2}:\d{2})', str(source_id))
    return m.group(1) if m else None


# ── Database helpers ──────────────────────────────────────────────────────────
def get_or_create_location(cur, province, district, village):
    """Return location_id, inserting if not present."""
    province = province or 'Unknown'
    district = district or 'Unknown'
    village  = village  or 'Unknown'
    site     = 'Unknown'

    cur.execute("""
        SELECT location_id FROM locations
        WHERE province=? AND district=? AND village=? AND site_name=?
    """, (province, district, village, site))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO locations (province, district, village, site_name)
        VALUES (?, ?, ?, ?)
    """, (province, district, village, site))
    return cur.lastrowid


def insert_sample(cur, source_id, collection_date, location_id, remark):
    """Insert into samples; skip if source_id already exists. Return sample_id."""
    cur.execute("SELECT sample_id FROM samples WHERE source_id=?", (source_id,))
    row = cur.fetchone()
    if row:
        return row[0], 'skipped'

    cur.execute("""
        INSERT INTO samples (source_id, sample_origin, collection_date, location_id, remark)
        VALUES (?, 'Bat', ?, ?, ?)
    """, (source_id, collection_date, location_id, remark))
    return cur.lastrowid, 'inserted'


def insert_host(cur, row_id, source_id, sample_id, bag_id, ring_id,
                capture_date, capture_time, method, location_id):
    """Insert into hosts; skip if source_id already exists."""
    cur.execute("SELECT host_id FROM hosts WHERE source_id=?", (source_id,))
    if cur.fetchone():
        return None, 'skipped'

    cur.execute("""
        INSERT INTO hosts
            (source_id, sample_id, host_type, bag_id, ring_no,
             capture_date, capture_time, trap_type, location_id)
        VALUES (?, ?, 'Bat', ?, ?, ?, ?, ?, ?)
    """, (source_id, sample_id, bag_id, ring_id,
          capture_date, capture_time, method, location_id))
    return cur.lastrowid, 'inserted'


def insert_tube(cur, sample_id, tube_type, tube_code, tissue_type=None):
    """Insert a sample tube; skip if tube_code already exists."""
    if not tube_code:
        return None, 'empty'

    cur.execute("SELECT tube_id FROM sample_tubes WHERE tube_code=?", (tube_code,))
    if cur.fetchone():
        return None, 'skipped'

    # Check UNIQUE(sample_id, tube_type) — same type already for this sample?
    cur.execute("""
        SELECT tube_id FROM sample_tubes
        WHERE sample_id=? AND tube_type=?
    """, (sample_id, tube_type))
    if cur.fetchone():
        return None, f'duplicate_type:{tube_type}'

    cur.execute("""
        INSERT INTO sample_tubes (sample_id, tube_type, tube_code, tissue_sample_type)
        VALUES (?, ?, ?, ?)
    """, (sample_id, tube_type, tube_code, tissue_type))
    return cur.lastrowid, 'inserted'


# ── Column mapping ────────────────────────────────────────────────────────────
# Maps Excel column names → (tube_type, tissue_type)
# Adjust these if your Excel headers differ
TUBE_COLUMNS = {
    'SalivaId':    ('saliva',    None),
    'AnalId':      ('anal',      None),
    'UrineId':     ('urine',     None),
    'EctoId':      ('ecto',      None),
    'BloodId':     ('blood',     None),
    'TissueId':    ('tissue',    None),  # tissue_type comes from TissueType col
    'IntestineId': ('intestine', None),
    'AdiposeId':   ('adipose',   None),
    'PlasmaId':    ('plasma',    None),
    'GuanoId':     ('anal',      None),  # guano → anal type, adjust if needed
}


# ── Main importer ─────────────────────────────────────────────────────────────
def run_import(excel_path, db_path, sheet=None, dry_run=False):
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Starting import...")
    print(f"  Excel : {excel_path}")
    print(f"  DB    : {db_path}\n")

    # Load Excel
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet or 0, dtype=str)
    except Exception as e:
        print(f"ERROR reading Excel: {e}")
        return False

    # Normalise column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]
    print(f"Columns found: {list(df.columns)}")
    print(f"Rows to process: {len(df)}\n")

    # Which tube columns are actually present?
    present_tube_cols = [c for c in TUBE_COLUMNS if c in df.columns]
    print(f"Tube columns detected: {present_tube_cols}\n")

    # Connect
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    cur = con.cursor()

    stats = {
        'samples_inserted': 0, 'samples_skipped': 0,
        'hosts_inserted':   0, 'hosts_skipped':   0,
        'tubes_inserted':   0, 'tubes_skipped':   0,
        'tubes_empty':      0, 'errors':           [],
    }

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number (header is row 1)

        try:
            # ── Collect raw values ──────────────────────────────────
            excel_id   = clean(row.get('Id'))
            source_id  = parse_source_id(row.get('SourceId'))
            date_raw   = clean(row.get('Date'))
            province   = clean(row.get('Province'))
            district   = clean(row.get('District'))
            village    = clean(row.get('Village'))
            method     = clean(row.get('Method'))
            time_val   = clean(row.get('Time'))
            bag_id     = clean(row.get('BagId'))
            ring_id    = clean(row.get('RingId'))
            remark     = clean(row.get('Remark'))
            tissue_type= clean(row.get('TissueType'))  # optional column

            # Parse date
            collection_date = parse_date(date_raw)

            # Try to get capture time from Time column, fall back to SourceId
            capture_time = time_val or extract_time(source_id)

            # Skip fully empty rows
            if not source_id and not excel_id:
                continue

            # Use excel_id as host source_id (field code like 'Batswab_1')
            host_source_id = excel_id or source_id

            if dry_run:
                print(f"Row {row_num}: host={host_source_id} | sample={source_id} | "
                      f"date={collection_date} | loc={province}/{district}/{village} | "
                      f"time={capture_time} | bag={bag_id} | ring={ring_id}")
                for col in present_tube_cols:
                    code = clean(row.get(col))
                    if code:
                        ttype, _ = TUBE_COLUMNS[col]
                        print(f"         tube: {ttype} → {code}")
                continue

            # ── 1. Location ─────────────────────────────────────────
            location_id = get_or_create_location(cur, province, district, village)

            # ── 2. Sample ───────────────────────────────────────────
            sample_id, s_status = insert_sample(
                cur, source_id, collection_date, location_id, remark
            )
            if s_status == 'inserted':
                stats['samples_inserted'] += 1
            else:
                stats['samples_skipped'] += 1

            # ── 3. Host ─────────────────────────────────────────────
            _, h_status = insert_host(
                cur, excel_id, host_source_id, sample_id,
                bag_id, ring_id, collection_date, capture_time,
                method, location_id
            )
            if h_status == 'inserted':
                stats['hosts_inserted'] += 1
            else:
                stats['hosts_skipped'] += 1

            # ── 4. Tubes ────────────────────────────────────────────
            for col in present_tube_cols:
                tube_code = clean(row.get(col))
                tube_type, default_tissue = TUBE_COLUMNS[col]
                tt = tissue_type if tube_type == 'tissue' else default_tissue

                _, t_status = insert_tube(cur, sample_id, tube_type, tube_code, tt)
                if t_status == 'inserted':
                    stats['tubes_inserted'] += 1
                elif t_status == 'empty':
                    stats['tubes_empty'] += 1
                else:
                    stats['tubes_skipped'] += 1

        except Exception as e:
            msg = f"Row {row_num} ({clean(row.get('Id','?'))}): {e}"
            stats['errors'].append(msg)
            print(f"  WARNING: {msg}")
            continue

    # ── Commit or rollback ────────────────────────────────────────────────────
    if not dry_run:
        con.commit()
        print("Changes committed to database.\n")
    con.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 50)
    print("IMPORT SUMMARY")
    print("=" * 50)
    print(f"  Samples   inserted : {stats['samples_inserted']}")
    print(f"  Samples   skipped  : {stats['samples_skipped']}  (already existed)")
    print(f"  Hosts     inserted : {stats['hosts_inserted']}")
    print(f"  Hosts     skipped  : {stats['hosts_skipped']}  (already existed)")
    print(f"  Tubes     inserted : {stats['tubes_inserted']}")
    print(f"  Tubes     skipped  : {stats['tubes_skipped']}  (duplicate)")
    print(f"  Tubes     empty    : {stats['tubes_empty']}   (blank in Excel)")
    if stats['errors']:
        print(f"\n  ERRORS ({len(stats['errors'])}):")
        for e in stats['errors']:
            print(f"    - {e}")
    else:
        print(f"\n  No errors.")
    print("=" * 50)
    
    return True


# ── Example usage for notebook ─────────────────────────────────────────────────
print("Bat Swab Importer loaded. Usage:")
print("run_import('data.xlsx', 'database.db', sheet=None, dry_run=True)")
