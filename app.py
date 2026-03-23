from flask import Flask, jsonify, send_file, request
import pandas as pd
from flask_cors import CORS
import os
import numpy as np
import warnings
import json
import threading
import time
from datetime import datetime, date, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB upload limit
warnings.filterwarnings('ignore')

@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large (max 50 MB)'}), 413

# ---- Weekly snapshot helpers ----
SNAPSHOT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'snapshots.json')
DATA_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data.xlsm')
_BASELINE_SUNDAY = date(2026, 3, 15)   # Week 0: Mar 15–21 2026
_MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

def _week_sunday(d):
    """Sunday that opens the Sun–Sat week containing d."""
    return d - timedelta(days=(d.weekday() + 1) % 7)

def _week_num(d):
    return (_week_sunday(d) - _BASELINE_SUNDAY).days // 7

def _week_label(d):
    sun = _week_sunday(d)
    sat = sun + timedelta(days=6)
    s_str = _MON[sun.month-1] + ' ' + str(sun.day)
    e_str = (_MON[sat.month-1] + ' ' if sat.month != sun.month else '') + str(sat.day)
    return s_str + '\u2013' + e_str

def _norm_status(s):
    l = str(s or '').lower().strip()
    if l == 'done': return 'Done'
    if l in ('in progress', 'in_progress'): return 'In Progress'
    return 'To Do'

def _is_compliance_test_key(k):
    """Return True when snapshot key belongs to a Compliance/Complience test project."""
    project = str(k or '').split('|', 1)[0].strip().lower()
    return 'test' in project and project.startswith('compli')

def _parse_snapshot_key(k):
    """Split a snapshot key into Project/Category/Document type/Field parts."""
    parts = str(k or '').split('|', 3)
    while len(parts) < 4:
        parts.append('')
    return {
        'project': parts[0].strip(),
        'category': parts[1].strip(),
        'doc_type': parts[2].strip(),
        'field': parts[3].strip(),
    }

def _load_snaps():
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE, 'r', encoding='utf-8') as fh:
                return json.load(fh)
        except Exception:
            pass
    return []

def _save_snaps(snaps):
    tmp = SNAPSHOT_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(snaps, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, SNAPSHOT_FILE)

def take_snapshot():
    """Capture the current Fields sheet state into snapshots.json."""
    try:
        if not os.path.exists(DATA_FILE):
            print('Snapshot skipped: data.xlsm not found at', DATA_FILE)
            return
        df = pd.read_excel(DATA_FILE, sheet_name='Fields')
        df = df.where(pd.notna(df), '')
        states = {}
        for _, row in df.iterrows():
            key = '|'.join([
                str(row.get('Project', '') or '').strip(),
                str(row.get('Category', '') or '').strip(),
                str(row.get('Document type', '') or '').strip(),
                str(row.get('Field', '') or '').strip(),
            ])
            states[key] = {
                'internal': _norm_status(row.get('Internal tool', '')),
                'gpt':      _norm_status(row.get('GPT', '')),
                'gemini':   _norm_status(row.get('Gemini', '')),
                'claude':   _norm_status(row.get('Claude', '')),
            }
        tz  = ZoneInfo('America/New_York') if ZoneInfo else None
        now = datetime.now(tz) if tz else datetime.utcnow()
        today = now.date()
        snap = {
            'week_num':    _week_num(today),
            'week_label':  _week_label(today),
            'taken_at':    now.isoformat(),
            'total_fields': len(states),
            'field_states': states,
        }
        snaps = _load_snaps()
        idx = next((i for i, s in enumerate(snaps) if s['week_num'] == snap['week_num']), None)
        if idx is not None: snaps[idx] = snap
        else: snaps.append(snap)
        snaps.sort(key=lambda s: s['week_num'])
        _save_snaps(snaps)
        print(f"Snapshot saved \u2013 W{snap['week_num']} ({snap['week_label']}), {snap['total_fields']} fields")
    except Exception as e:
        print(f'Snapshot error: {e}')

def take_start_snapshot():
    """Save a named 'Start' baseline snapshot (week_num=-1)."""
    try:
        if not os.path.exists(DATA_FILE):
            return
        df = pd.read_excel(DATA_FILE, sheet_name='Fields')
        df = df.where(pd.notna(df), '')
        states = {}
        for _, row in df.iterrows():
            key = '|'.join([
                str(row.get('Project', '') or '').strip(),
                str(row.get('Category', '') or '').strip(),
                str(row.get('Document type', '') or '').strip(),
                str(row.get('Field', '') or '').strip(),
            ])
            states[key] = {
                'internal': _norm_status(row.get('Internal tool', '')),
                'gpt':      _norm_status(row.get('GPT', '')),
                'gemini':   _norm_status(row.get('Gemini', '')),
                'claude':   _norm_status(row.get('Claude', '')),
            }
        tz  = ZoneInfo('America/New_York') if ZoneInfo else None
        now = datetime.now(tz) if tz else datetime.utcnow()
        snap = {
            'week_num':     -1,
            'is_start':     True,
            'week_label':   'Start',
            'taken_at':     now.isoformat(),
            'total_fields': len(states),
            'field_states': states,
        }
        snaps = _load_snaps()
        snaps = [s for s in snaps if not s.get('is_start')]  # remove previous start
        snaps.append(snap)
        snaps.sort(key=lambda s: s['week_num'])
        _save_snaps(snaps)
        print(f"Start snapshot saved \u2013 {snap['total_fields']} fields at {now.strftime('%H:%M ET')}")
    except Exception as e:
        print(f'Start snapshot error: {e}')

def _scheduler():
    """Daemon thread: take a snapshot every Friday at 21:00 ET.
    Also takes a catch-up snapshot on startup if this week's snapshot was missed."""
    # --- Catch-up: if server was down when Friday 9 PM passed, snapshot now ---
    try:
        tz  = ZoneInfo('America/New_York') if ZoneInfo else None
        now = datetime.now(tz) if tz else datetime.utcnow()
        today = now.date()
        current_week = _week_num(today)
        snaps = _load_snaps()
        existing_weeks = {s['week_num'] for s in snaps if not s.get('is_start')}
        # It's past Friday 9 PM this week (Fri after 21:00 or Sat/Sun) and no snapshot yet
        day_of_week = now.weekday()  # Mon=0 … Fri=4, Sat=5, Sun=6
        past_fri_cutoff = (day_of_week == 4 and now.hour >= 21) or day_of_week in (5, 6)
        if past_fri_cutoff and current_week not in existing_weeks:
            print('Catch-up snapshot: missed Friday auto-snapshot, taking now...')
            take_snapshot()
    except Exception as e:
        print(f'Catch-up snapshot error: {e}')

    while True:
        try:
            tz = ZoneInfo('America/New_York') if ZoneInfo else None
            now = datetime.now(tz) if tz else datetime.utcnow()
            da = (4 - now.weekday()) % 7          # days ahead to next Friday
            target = now.replace(hour=21, minute=0, second=0, microsecond=0)
            if da == 0 and now.hour >= 21: da = 7  # already past this Friday 9 PM
            target += timedelta(days=da)
            secs = max((target - now).total_seconds(), 1)
            print(f'Next auto-snapshot in {secs/3600:.1f} h (Fri 9 PM ET)')
            time.sleep(secs)
            take_snapshot()
        except Exception as e:
            print(f'Scheduler error: {e}')
            time.sleep(3600)

threading.Thread(target=_scheduler, daemon=True, name='snapshot-scheduler').start()

def convert_to_serializable(obj):
    """Convert any value to JSON-serializable format"""
    if pd.isna(obj) or obj is None:
        return ""
    if isinstance(obj, (np.integer, np.floating)):
        if np.isnan(obj) or np.isinf(obj):
            return ""
        return float(obj) if isinstance(obj, np.floating) else int(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    return str(obj)

@app.route('/api/data', methods=['GET'])
def get_data():
    """Read Excel file and return data as JSON"""
    try:
        # Always read fresh from disk
        # Try .xlsm first, then .xlsx
        data_xlsx = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data.xlsx')
        if os.path.exists(DATA_FILE):
            df = pd.read_excel(DATA_FILE, sheet_name='Fields')
        elif os.path.exists(data_xlsx):
            df = pd.read_excel(data_xlsx, sheet_name=0)
        else:
            return jsonify({'error': 'data.xlsm or data.xlsx not found'}), 404
        
        # Replace all NaN and inf values with empty string
        df = df.where(pd.notna(df), '')
        df = df.replace([np.inf, -np.inf], '')
        
        # Convert dataframe to dictionary
        data = df.to_dict('records')
        
        # Clean each row - convert all values to serializable format
        cleaned_data = []
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                try:
                    cleaned_row[key] = convert_to_serializable(value)
                except:
                    cleaned_row[key] = ""
            cleaned_data.append(cleaned_row)
        
        resp = jsonify({
            'success': True,
            'count': len(cleaned_data),
            'data': cleaned_data
        })
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/columns', methods=['GET'])
def get_columns():
    """Get column names from Excel"""
    try:
        data_xlsx = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data.xlsx')
        if os.path.exists(DATA_FILE):
            df = pd.read_excel(DATA_FILE, sheet_name='Fields')
        elif os.path.exists(data_xlsx):
            df = pd.read_excel(data_xlsx, sheet_name=0)
        else:
            return jsonify({'error': 'Excel file not found'}), 404
        
        columns = df.columns.tolist()
        return jsonify({'columns': columns})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/autotests', methods=['GET'])
def get_autotests():
    """Read Autotests sheet and return data as JSON"""
    try:
        if not os.path.exists(DATA_FILE):
            return jsonify({'error': 'data.xlsm not found'}), 404

        df = pd.read_excel(DATA_FILE, sheet_name='Autotests')

        # Drop unnamed columns
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        df = df.where(pd.notna(df), '')
        df = df.replace([np.inf, -np.inf], '')

        data = df.to_dict('records')
        cleaned_data = []
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                try:
                    cleaned_row[key] = convert_to_serializable(value)
                except:
                    cleaned_row[key] = ""
            cleaned_data.append(cleaned_row)

        return jsonify({
            'success': True,
            'count': len(cleaned_data),
            'data': cleaned_data
        })
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/snapshots', methods=['GET'])
def get_snapshots():
    """Return weekly dynamics derived from stored snapshots."""
    snaps = _load_snaps()
    if not snaps:
        return jsonify({'success': True, 'weeks': []})

    # Separate the 'Start' baseline from regular weekly snapshots
    start_snap = next((s for s in snaps if s.get('is_start')), None)
    week_snaps = [s for s in snaps if not s.get('is_start')]
    result = []

    # --- Start row: absolute Done / In-Progress counts per model ---
    if start_snap:
        st = start_snap.get('field_states', {})
        by_model = {}
        non_compliance_keys = [k for k in st.keys() if not _is_compliance_test_key(k)]
        for m in ('internal', 'gpt', 'gemini', 'claude'):
            by_model[m] = {
                # Start row: exclude Compliance Test from Done and In Progress
                'to_done':     sum(1 for k in non_compliance_keys if st.get(k, {}).get(m) == 'Done'),
                'to_progress': sum(1 for k in non_compliance_keys if st.get(k, {}).get(m) == 'In Progress'),
            }
        result.append({
            'week_num':     -1,
            'is_start':     True,
            'week_label':   'Start',
            'taken_at':     start_snap['taken_at'],
            # Start row: exclude Compliance Test from Total
            'total_fields': len(non_compliance_keys),
            'new_fields':   0,
            'by_model':     by_model,
        })

    # --- Weekly rows: delta vs previous snapshot (Start → W0 → W1 …) ---
    prev_snap = start_snap  # W0 delta vs Start; if no Start, delta vs empty
    for snap in week_snaps:
        curr_st   = snap.get('field_states', {})
        prev_st   = prev_snap.get('field_states', {}) if prev_snap else {}
        curr_keys = set(curr_st.keys())
        new_fields = len(curr_keys - set(prev_st.keys()))
        removed_fields = len(set(prev_st.keys()) - curr_keys) if prev_snap else 0
        is_w0 = snap.get('week_num') == 0
        by_model = {}
        for m in ('internal', 'gpt', 'gemini', 'claude'):
            to_done = to_prog = 0
            for k in curr_keys:
                cs = curr_st[k].get(m, 'To Do')
                ps = prev_st.get(k, {}).get(m, 'To Do') if prev_st else 'To Do'
                if cs == 'Done' and ps != 'Done':
                    # W0 row: exclude Compliance Test from Done delta
                    if (not is_w0) or (not _is_compliance_test_key(k)):
                        to_done += 1
                if cs == 'In Progress' and ps not in ('Done', 'In Progress'):   to_prog += 1
            by_model[m] = {'to_done': to_done, 'to_progress': to_prog}
        total_fields_out = snap['total_fields']
        if is_w0:
            # W0 row: exclude Compliance Test from Total
            total_fields_out = sum(1 for k in curr_keys if not _is_compliance_test_key(k))
        result.append({
            'week_num':     snap['week_num'],
            'is_start':     False,
            'week_label':   snap['week_label'],
            'taken_at':     snap['taken_at'],
            'total_fields': total_fields_out,
            'new_fields':   new_fields,
            'removed_fields': removed_fields,
            'by_model':     by_model,
        })
        prev_snap = snap

    return jsonify({'success': True, 'weeks': result})

@app.route('/api/history', methods=['GET'])
def get_history():
    """Return detailed weekly field changes (added, removed, status transitions)."""
    snaps = _load_snaps()
    if not snaps:
        return jsonify({'success': True, 'weeks': [], 'changes': []})

    start_snap = next((s for s in snaps if s.get('is_start')), None)
    week_snaps = sorted([s for s in snaps if not s.get('is_start')], key=lambda s: s.get('week_num', 0))

    models = ('internal', 'gpt', 'gemini', 'claude')
    changes = []
    weeks = []
    prev_snap = start_snap

    for snap in week_snaps:
        curr_st = snap.get('field_states', {})
        prev_st = prev_snap.get('field_states', {}) if prev_snap else {}

        curr_keys = set(curr_st.keys())
        prev_keys = set(prev_st.keys())
        added_keys = sorted(curr_keys - prev_keys)
        removed_keys = sorted(prev_keys - curr_keys)

        status_changes_count = 0

        # Added fields: one row per field (no per-model split).
        for k in added_keys:
            parts = _parse_snapshot_key(k)
            changes.append({
                'week_num': snap.get('week_num'),
                'week_label': snap.get('week_label'),
                'taken_at': snap.get('taken_at'),
                'change_type': 'added',
                'model': None,
                'from_status': 'Not Present',
                'to_status': 'Added',
                **parts,
            })

        # Removed fields: one row per field (no per-model split).
        for k in removed_keys:
            parts = _parse_snapshot_key(k)
            changes.append({
                'week_num': snap.get('week_num'),
                'week_label': snap.get('week_label'),
                'taken_at': snap.get('taken_at'),
                'change_type': 'removed',
                'model': None,
                'from_status': 'Present',
                'to_status': 'Removed',
                **parts,
            })

        # Existing fields: capture status transitions per model.
        for k in sorted(curr_keys.intersection(prev_keys)):
            parts = _parse_snapshot_key(k)
            curr_model_states = curr_st.get(k, {})
            prev_model_states = prev_st.get(k, {})
            for m in models:
                prev_status = _norm_status(prev_model_states.get(m, 'To Do'))
                curr_status = _norm_status(curr_model_states.get(m, 'To Do'))
                if prev_status != curr_status:
                    status_changes_count += 1
                    changes.append({
                        'week_num': snap.get('week_num'),
                        'week_label': snap.get('week_label'),
                        'taken_at': snap.get('taken_at'),
                        'change_type': 'status_changed',
                        'model': m,
                        'from_status': prev_status,
                        'to_status': curr_status,
                        **parts,
                    })

        weeks.append({
            'week_num': snap.get('week_num'),
            'week_label': snap.get('week_label'),
            'taken_at': snap.get('taken_at'),
            'added_fields': len(added_keys),
            'removed_fields': len(removed_keys),
            'status_changes': status_changes_count,
            'total_changes': len(added_keys) + len(removed_keys) + status_changes_count,
        })
        prev_snap = snap

    return jsonify({'success': True, 'weeks': weeks, 'changes': changes})

_PROTECTED_ACTIONS = {'upload', 'download'}
_ACTION_PW = '1x2c3v'

@app.route('/api/auth', methods=['POST'])
def check_auth():
    """Verify password for protected actions. Returns a short-lived token."""
    import hmac, hashlib, time as _time
    data = request.get_json(force=True, silent=True) or {}
    action = data.get('action', '')
    password = data.get('password', '')
    if action not in _PROTECTED_ACTIONS:
        return jsonify({'success': False, 'error': 'Unknown action'}), 400
    if not hmac.compare_digest(password, _ACTION_PW):
        return jsonify({'success': False, 'error': 'Incorrect password'}), 403
    # Simple time-based token: action|timestamp (good for 5 min)
    ts = str(int(_time.time()))
    raw = f'{action}|{ts}|{_ACTION_PW}'
    token = hmac.new(b'dashboard-secret', raw.encode(), hashlib.sha256).hexdigest()[:16]
    return jsonify({'success': True, 'token': token, 'ts': ts, 'action': action})

def _verify_token(action, token, ts):
    """Returns True if token is valid and < 5 minutes old."""
    import hmac, hashlib, time as _time
    try:
        if abs(int(_time.time()) - int(ts)) > 300:
            return False
        raw = f'{action}|{ts}|{_ACTION_PW}'
        expected = hmac.new(b'dashboard-secret', raw.encode(), hashlib.sha256).hexdigest()[:16]
        return hmac.compare_digest(token, expected)
    except Exception:
        return False

@app.route('/api/download-excel', methods=['GET'])
def download_excel():
    """Serve the current data.xlsm for download (requires auth token)."""
    token = request.args.get('token', '')
    ts    = request.args.get('ts', '')
    if not _verify_token('download', token, ts):
        return jsonify({'error': 'Unauthorized'}), 403
    if not os.path.exists(DATA_FILE):
        return jsonify({'error': 'data.xlsm not found on server'}), 404
    return send_file(DATA_FILE, as_attachment=True, download_name='data.xlsm',
                     mimetype='application/vnd.ms-excel.sheet.macroEnabled.12')

@app.route('/api/upload-excel', methods=['POST'])
def upload_excel():
    """Accept an uploaded Excel file and replace data.xlsm on the server (requires auth token)."""
    try:
        token = request.form.get('token', '')
        ts    = request.form.get('ts', '')
        if not _verify_token('upload', token, ts):
            return jsonify({'error': 'Unauthorized'}), 403
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        f = request.files['file']
        if not f.filename:
            return jsonify({'error': 'Empty filename'}), 400
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in ('.xlsm', '.xlsx'):
            return jsonify({'error': 'Only .xlsm or .xlsx files are accepted'}), 400
        tmp_path = DATA_FILE + '.tmp'
        f.save(tmp_path)
        if os.path.exists(DATA_FILE):
            try:
                os.chmod(DATA_FILE, 0o644)
            except OSError:
                pass
        os.replace(tmp_path, DATA_FILE)
        return jsonify({'success': True, 'filename': f.filename})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/snapshot/take', methods=['POST'])
def manual_snapshot():
    """Manually trigger a snapshot for the current week."""
    try:
        if not os.path.exists(DATA_FILE):
            return jsonify({'success': False, 'error': f'data.xlsm not found at {DATA_FILE}'}), 500
        take_snapshot()
        snaps = _load_snaps()
        return jsonify({'success': True, 'total_snapshots': len(snaps)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/snapshot/take-start', methods=['POST'])
def manual_start_snapshot():
    """Save the 'Start' baseline snapshot."""
    try:
        if not os.path.exists(DATA_FILE):
            return jsonify({'success': False, 'error': f'data.xlsm not found at {DATA_FILE}'}), 500
        take_start_snapshot()
        snaps = _load_snaps()
        return jsonify({'success': True, 'total_snapshots': len(snaps)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/')
def index():
    """Serve the frontend"""
    index_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html')
    response = send_file(index_file)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

if __name__ == '__main__':
    print("Starting Flask server...")
    print("Visit http://localhost:5000 or http://10.230.0.21:5000 in your browser")
    app.run(debug=False, port=5000, host='0.0.0.0')
