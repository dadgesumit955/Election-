import json
import os
import secrets
import sqlite3
import threading
import urllib.request
import urllib.error
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, unquote

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "election.db")
PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "0.0.0.0")

COLLEGE_NAME = "College of Engineering"
DEPARTMENTS = ["AI & ML", "Computer Engineering", "Civil Engineering", "Electronics & Telecommunication"]
ADMIN_USERS = {"admin": "admin123"}
STATUSES = {"draft", "open", "paused", "closed", "published"}

SCHEMA_STATEMENTS = [
    "CREATE TABLE IF NOT EXISTS students (id TEXT PRIMARY KEY, name TEXT NOT NULL, department TEXT NOT NULL, year TEXT DEFAULT 'SE', pin TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS elections (id TEXT PRIMARY KEY, title TEXT NOT NULL, type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', created_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS election_departments (election_id TEXT NOT NULL REFERENCES elections(id) ON DELETE CASCADE, department TEXT NOT NULL, PRIMARY KEY (election_id, department))",
    "CREATE TABLE IF NOT EXISTS positions (id TEXT PRIMARY KEY, election_id TEXT NOT NULL REFERENCES elections(id) ON DELETE CASCADE, title TEXT NOT NULL, description TEXT)",
    "CREATE TABLE IF NOT EXISTS candidates (id TEXT PRIMARY KEY, position_id TEXT NOT NULL REFERENCES positions(id) ON DELETE CASCADE, name TEXT NOT NULL, department TEXT, year TEXT, platform TEXT, symbol TEXT, photo TEXT)",
    "CREATE TABLE IF NOT EXISTS voter_status (election_id TEXT NOT NULL, student_id TEXT NOT NULL, receipt_no TEXT NOT NULL, voted_at TEXT NOT NULL, PRIMARY KEY (election_id, student_id))",
    "CREATE TABLE IF NOT EXISTS ballots (id INTEGER PRIMARY KEY AUTOINCREMENT, ballot_id TEXT NOT NULL, election_id TEXT NOT NULL, position_id TEXT NOT NULL, candidate_id TEXT NOT NULL, voted_at TEXT NOT NULL, UNIQUE (ballot_id, election_id, position_id))",
    "CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, action TEXT NOT NULL)",
]

tokens = {}
uploads = {}
write_lock = threading.Lock()


def read_config_file(name):
    p = os.path.join(BASE_DIR, name)
    if os.path.isfile(p):
        with open(p, "r", encoding="utf-8") as f:
            v = f.read().strip()
        return v or None
    return None


TURSO_URL = os.environ.get("TURSO_URL") or os.environ.get("TURSO_DATABASE_URL") or read_config_file("turso_url.txt")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN") or os.environ.get("TURSO_AUTH_TOKEN") or read_config_file("turso_token.txt")
TURSO_ENABLED = bool(TURSO_URL and TURSO_TOKEN)


class TursoHTTPError(Exception):
    pass


class RowConflictError(Exception):
    pass


class TursoDB:
    def __init__(self, raw_url, token):
        if raw_url.startswith("libsql://"):
            raw_url = "https://" + raw_url[len("libsql://"):]
        elif raw_url.startswith("http://"):
            raw_url = "https://" + raw_url[len("http://"):]
        self.base = raw_url.rstrip("/")
        self.token = token
        self.endpoint = self.base + "/"

    def _request(self, statements):
        body = json.dumps({"statements": statements}).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.token},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise TursoHTTPError("HTTP %d: %s" % (e.code, e.read().decode("utf-8", "replace")[:300]))
        except OSError as e:
            raise TursoHTTPError(str(e))
        results = payload if isinstance(payload, list) else [payload]
        out = []
        for entry in results:
            if not isinstance(entry, dict):
                raise TursoHTTPError(str(entry))
            if "error" in entry:
                err = entry["error"]
                if isinstance(err, dict):
                    message = err.get("message", "")
                    code = err.get("code")
                else:
                    message = str(err)
                    code = None
                up = (message or "").upper()
                if code in ("2067", "1555") or "UNIQUE" in up or "must be unique" in up:
                    raise RowConflictError(message)
                raise TursoHTTPError(message or str(err))
            out.append(entry.get("results") or entry)
        return out

    def statement(self, sql, args=()):
        s = sql.strip().upper()
        if s.startswith(("BEGIN", "COMMIT", "ROLLBACK", "PRAGMA")):
            return {"cols": [], "rows": [], "rows_affected": 0}
        res = self._request([{"q": sql, "params": list(args or ())}])[0]
        return {
            "cols": res.get("columns") or res.get("cols") or [],
            "rows": res.get("rows") or [],
            "rows_affected": res.get("rows_affected", 0),
        }

    def statements(self, sql_args_list):
        stmts = [{"q": sql, "params": list(args or ())} for sql, args in sql_args_list]
        ress = self._request(stmts)
        return [{**r, "cols": r.get("columns") or r.get("cols") or []} for r in ress]


class TursoResult:
    def __init__(self, stmt):
        cols = stmt.get("cols") or []
        self._rows = [dict(zip(cols, row)) for row in (stmt.get("rows") or [])]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class TursoConn:
    def __init__(self, db):
        self._db = db

    def execute(self, sql, args=None):
        return TursoResult(self._db.statement(sql, args or ()))

    def executemany(self, sql, seq):
        self._db.statements([(sql, a) for a in seq])
        return None

    def cursor(self):
        return self

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def get_conn():
    if TURSO_ENABLED:
        return TursoConn(turso_db)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


turso_db = TursoDB(TURSO_URL, TURSO_TOKEN) if TURSO_ENABLED else None


def now():
    return datetime.now().isoformat(timespec="seconds")


def make_id(prefix):
    return prefix + secrets.token_hex(4)


def make_pin():
    return str(secrets.randbelow(900000) + 100000)


def init_db():
    conn = get_conn()
    if TURSO_ENABLED:
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
    else:
        conn.execute("PRAGMA journal_mode=WAL")
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(stmt)
    count = conn.execute("SELECT COUNT(*) AS c FROM elections").fetchone()["c"]
    if count == 0:
        seed(conn)
        audit(conn, "System initialized with demo data")
        conn.commit()
    conn.close()


def heal_demo_election():
    conn = get_conn()
    rows = conn.execute("SELECT id, title, status FROM elections").fetchall()
    if len(rows) == 1 and rows[0]["title"] == "General Secretary & College President Election 2026":
        eid = rows[0]["id"]
        ballots = conn.execute("SELECT COUNT(*) AS c FROM ballots WHERE election_id=?", (eid,)).fetchone()["c"]
        if ballots == 0 and rows[0]["status"] != "open":
            conn.execute("DELETE FROM voter_status WHERE election_id=?", (eid,))
            conn.execute("UPDATE elections SET status='open' WHERE id=?", (eid,))
            conn.commit()
            print("  [self-heal] Demo election reopened for voting (no ballots were cast).")
    conn.close()


def audit(conn, action):
    conn.execute("INSERT INTO audit_log (at, action) VALUES (?, ?)", (now(), action))


def seed(conn):
    students = [
        ("AIML_01", "Suraj Jadhav", "AI & ML", "SE", "482913"),
        ("AIML_62", "Aditya Pawar", "AI & ML", "SE", "773102"),
        ("CIVIL_03", "Aditya Patil", "Civil Engineering", "SE", "990481"),
        ("CSE_07", "Priya Sharma", "Computer Engineering", "TE", "216907"),
        ("CSE_12", "Rohan Verma", "Computer Engineering", "SE", "548320"),
        ("ETC_04", "Sneha Kulkarni", "Electronics & Telecommunication", "BE", "631845"),
        ("CIVIL_09", "Varun Deshmukh", "Civil Engineering", "BE", "118276"),
        ("ETC_15", "Kavya Nair", "Electronics & Telecommunication", "TE", "905614"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO students (id, name, department, year, pin) VALUES (?, ?, ?, ?, ?)", students
    )

    eid = make_id("e_")
    conn.execute(
        "INSERT INTO elections (id, title, type, status, created_at) VALUES (?, ?, ?, ?, ?)",
        (eid, "General Secretary & College President Election 2026", "college-wide", "open", now()),
    )
    for d in DEPARTMENTS:
        conn.execute("INSERT INTO election_departments (election_id, department) VALUES (?, ?)", (eid, d))

    positions = [
        ("General Secretary", "Manages council coordination, communications and documentation across all departments.", [
            ("Suraj Jadhav", "AI & ML", "SE", "Digital documentation, transparent records, student newsletter", "\u2699\ufe0f"),
            ("Sneha Kulkarni", "Electronics & Telecommunication", "BE", "Streamlined communication, event coordination, council accountability", "\U0001f6f0\ufe0f"),
            ("Aditya Patil", "Civil Engineering", "SE", "Organized record systems, health & safety bulletins, campus outreach", "\U0001f3d7\ufe0f"),
        ]),
        ("College President", "Represents all engineering students and heads the student council.", [
            ("Priya Sharma", "Computer Engineering", "TE", "Tech literacy programs, career placement, campus innovation hub", "\U0001f4bb"),
            ("Rohan Verma", "Computer Engineering", "SE", "Scholarship expansion, student welfare, inclusive governance", "\U0001f3af"),
            ("Kavya Nair", "Electronics & Telecommunication", "TE", "Mental health programs, campus-wide Wi-Fi, sustainability drive", "\U0001f310"),
        ]),
        ("Sports & Cultural Coordinator", "Organizes inter-department sports, cultural festivals and student events.", [
            ("Varun Deshmukh", "Civil Engineering", "BE", "Annual sports meet, inter-department tournaments, fitness initiatives", "\U0001f3c6"),
            ("Aditya Pawar", "AI & ML", "SE", "Cultural fest, arts funding, student creative spaces", "\U0001f3ad"),
        ]),
    ]
    for title, desc, cands in positions:
        pid = make_id("p_")
        conn.execute(
            "INSERT INTO positions (id, election_id, title, description) VALUES (?, ?, ?, ?)",
            (pid, eid, title, desc),
        )
        for name, dept, year, platform, symbol in cands:
            conn.execute(
                "INSERT INTO candidates (id, position_id, name, department, year, platform, symbol, photo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (make_id("c_"), pid, name, dept, year, platform, symbol, None),
            )


def student_eligible(conn, student, election_id, etype, depts):
    if etype == "college-wide":
        return True
    row = conn.execute(
        "SELECT 1 FROM election_departments WHERE election_id=? AND department=?",
        (election_id, student["department"]),
    ).fetchone()
    return row is not None


def compute_results(election_id):
    conn = get_conn()
    election = conn.execute("SELECT * FROM elections WHERE id=?", (election_id,)).fetchone()
    if not election:
        conn.close()
        return None
    positions = conn.execute("SELECT * FROM positions WHERE election_id=? ORDER BY rowid", (election_id,)).fetchall()
    total_students = conn.execute("SELECT COUNT(*) AS c FROM students").fetchone()["c"]
    eligible = total_students
    if election["type"] != "college-wide":
        depts = [
            r["department"]
            for r in conn.execute("SELECT department FROM election_departments WHERE election_id=?", (election_id,))
        ]
        eligible = conn.execute(
            "SELECT COUNT(*) AS c FROM students WHERE department IN (%s)" % ",".join("?" * len(depts)),
            depts,
        ).fetchone()["c"]
    votes = conn.execute(
        "SELECT COUNT(DISTINCT ballot_id) AS c FROM ballots WHERE election_id=?", (election_id,)
    ).fetchone()["c"]
    turnout = round((votes / eligible) * 100) if eligible > 0 else 0

    out_positions = []
    for p in positions:
        cand_rows = conn.execute("SELECT * FROM candidates WHERE position_id=? ORDER BY rowid", (p["id"],)).fetchall()
        tally = {}
        for c in cand_rows:
            tally[c["id"]] = conn.execute(
                "SELECT COUNT(*) AS c FROM ballots WHERE election_id=? AND position_id=? AND candidate_id=?",
                (election_id, p["id"], c["id"]),
            ).fetchone()["c"]
        tally["NOTA"] = conn.execute(
            "SELECT COUNT(*) AS c FROM ballots WHERE election_id=? AND position_id=? AND candidate_id='NOTA'",
            (election_id, p["id"]),
        ).fetchone()["c"]
        position_total = sum(tally.values())
        max_votes = max(tally.values())
        rows = []
        for c in cand_rows:
            v = tally[c["id"]]
            rows.append({
                "id": c["id"],
                "name": c["name"],
                "department": c["department"],
                "photo": c["photo"],
                "symbol": c["symbol"],
                "votes": v,
                "pct": round((v / position_total) * 100) if position_total > 0 else 0,
                "winner": v == max_votes and v > 0,
            })
        nota_v = tally["NOTA"]
        rows.append({
            "id": "NOTA",
            "name": "NOTA",
            "department": "",
            "photo": None,
            "symbol": None,
            "votes": nota_v,
            "pct": round((nota_v / position_total) * 100) if position_total > 0 else 0,
            "winner": nota_v == max_votes and nota_v > 0,
        })
        out_positions.append({
            "id": p["id"],
            "title": p["title"],
            "description": p["description"],
            "total": position_total,
            "rows": rows,
        })
    conn.close()
    return {
        "election": {
            "id": election["id"],
            "title": election["title"],
            "status": election["status"],
            "type": election["type"],
        },
        "stats": {"votes": votes, "eligible": eligible, "turnout": turnout},
        "positions": out_positions,
    }


def build_election_detail(conn, election):
    depts = [
        r["department"]
        for r in conn.execute(
            "SELECT department FROM election_departments WHERE election_id=? ORDER BY rowid",
            (election["id"],),
        )
    ]
    positions = []
    for p in conn.execute("SELECT * FROM positions WHERE election_id=? ORDER BY rowid", (election["id"],)):
        candidates = [
            dict(c)
            for c in conn.execute("SELECT * FROM candidates WHERE position_id=? ORDER BY rowid", (p["id"],))
        ]
        positions.append({
            "id": p["id"],
            "title": p["title"],
            "description": p["description"],
            "candidates": candidates,
        })
    ballots = conn.execute(
        "SELECT COUNT(DISTINCT ballot_id) AS c FROM ballots WHERE election_id=?", (election["id"],)
    ).fetchone()["c"]
    eligible = compute_eligible_count(conn, election)
    return {
        "id": election["id"],
        "title": election["title"],
        "type": election["type"],
        "status": election["status"],
        "created_at": election["created_at"],
        "departments": depts,
        "positions": positions,
        "ballots": ballots,
        "eligible_count": eligible,
    }


def compute_eligible_count(conn, election):
    if election["type"] == "college-wide":
        return conn.execute("SELECT COUNT(*) AS c FROM students").fetchone()["c"]
    depts = [
        r["department"]
        for r in conn.execute(
            "SELECT department FROM election_departments WHERE election_id=?", (election["id"],)
        )
    ]
    if not depts:
        return 0
    return conn.execute(
        "SELECT COUNT(*) AS c FROM students WHERE department IN (%s)" % ",".join("?" * len(depts)),
        depts,
    ).fetchone()["c"]


def validate_election_payload(data):
    if not data.get("title") or not str(data["title"]).strip():
        return "Election title is required"
    if data.get("type") not in ("college-wide", "department"):
        return "Invalid election type"
    depts = data.get("departments") or []
    if not isinstance(depts, list) or len(depts) == 0:
        return "Select at least one department"
    positions = data.get("positions") or []
    if not isinstance(positions, list) or len(positions) == 0:
        return "Add at least one position"
    for p in positions:
        if not p.get("title") or not str(p["title"]).strip():
            return "Position title required"
    return None


def replace_election_payload(conn, election_id, data):
    cur = conn.cursor()
    cur.execute("DELETE FROM election_departments WHERE election_id=?", (election_id,))
    for d in data.get("departments", []):
        cur.execute("INSERT INTO election_departments (election_id, department) VALUES (?, ?)", (election_id, d))
    existing_pos_ids = {
        r["id"]
        for r in cur.execute("SELECT id FROM positions WHERE election_id=?", (election_id,)).fetchall()
    }
    new_pos_ids = set()
    for p in data.get("positions", []):
        pid = p.get("id")
        is_existing_pos = pid and pid in existing_pos_ids
        if is_existing_pos:
            cur.execute("UPDATE positions SET title=?, description=? WHERE id=?", (p["title"], p.get("description", ""), pid))
        else:
            pid = make_id("p_")
            cur.execute(
                "INSERT INTO positions (id, election_id, title, description) VALUES (?, ?, ?, ?)",
                (pid, election_id, p["title"], p.get("description", "")),
            )
        new_pos_ids.add(pid)
        existing_cand_ids = {
            r["id"]
            for r in cur.execute("SELECT id FROM candidates WHERE position_id=?", (pid,)).fetchall()
        }
        new_cand_ids = set()
        for c in p.get("candidates", []):
            if not c.get("name") or not str(c["name"]).strip():
                continue
            cid = c.get("id")
            is_existing_cand = cid and cid in existing_cand_ids
            payload = (c["name"], c.get("department", ""), c.get("year", ""), c.get("platform", ""), c.get("symbol", ""), c.get("photo"))
            if is_existing_cand:
                cur.execute(
                    "UPDATE candidates SET name=?, department=?, year=?, platform=?, symbol=?, photo=? WHERE id=?",
                    (*payload, cid),
                )
                new_cand_ids.add(cid)
            else:
                cid = make_id("c_")
                cur.execute(
                    "INSERT INTO candidates (id, position_id, name, department, year, platform, symbol, photo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (cid, pid, *payload),
                )
                new_cand_ids.add(cid)
        removed = existing_cand_ids - new_cand_ids
        if removed:
            if new_cand_ids:
                q = ",".join("?" * len(new_cand_ids))
                cur.execute(
                    "DELETE FROM candidates WHERE position_id=? AND id NOT IN (%s)" % q,
                    [pid] + list(new_cand_ids),
                )
            else:
                cur.execute("DELETE FROM candidates WHERE position_id=?", (pid,))
    for oid in existing_pos_ids - new_pos_ids:
        cur.execute("DELETE FROM positions WHERE id=?", (oid,))


def make_token(role, student_id=None):
    tok = secrets.token_hex(16)
    tokens[tok] = {"role": role, "student_id": student_id}
    return tok


def authorize(handler, role):
    hdr = handler.headers.get("Authorization") or ""
    token = hdr.replace("Bearer ", "") if hdr.startswith("Bearer ") else ""
    info = tokens.get(token)
    if not info or info["role"] != role:
        return None
    return info


class Handler(BaseHTTPRequestHandler):
    server_version = "SAE/1.0"

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Cache-Control", "no-store")

    def _json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _text(self, code, text, ctype="text/plain; charset=utf-8"):
        data = text.encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _file(self, path):
        if not os.path.isfile(path):
            self._json(404, {"ok": False, "error": "Not found"})
            return
        ctype = "application/octet-stream"
        if path.endswith(".html"):
            ctype = "text/html; charset=utf-8"
        elif path.endswith(".js"):
            ctype = "application/javascript; charset=utf-8"
        elif path.endswith(".css"):
            ctype = "text/css; charset=utf-8"
        with open(path, "rb") as f:
            data = f.read()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json_body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            return json.loads(body) if body else {}
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        parts = [unquote(p) for p in path.split("/") if p]

        if path == "/" or path == "/index.html":
            self._file(os.path.join(BASE_DIR, "index.html"))
            return
        if path in ("/app.js", "/style.css"):
            self._file(os.path.join(BASE_DIR, path.lstrip("/")))
            return

        if parts[:2] == ["api", "results"] and len(parts) == 3:
            res = compute_results(parts[2])
            if not res:
                self._json(404, {"ok": False, "error": "Election not found"})
                return
            self._json(200, {"ok": True, **res})
            return

        if parts[:2] == ["api", "my"] and parts[2] == "elections":
            info = authorize(self, "student")
            if not info:
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            conn = get_conn()
            student = conn.execute("SELECT * FROM students WHERE id=?", (info["student_id"],)).fetchone()
            if not student:
                conn.close()
                self._json(401, {"ok": False, "error": "Student not found"})
                return
            elections = conn.execute("SELECT * FROM elections ORDER BY rowid").fetchall()
            available, others = [], []
            for e in elections:
                voted = conn.execute(
                    "SELECT 1 FROM voter_status WHERE election_id=? AND student_id=?", (e["id"], student["id"])
                ).fetchone() is not None
                detail = build_election_detail(conn, e)
                eligible = student_eligible(conn, dict(student), e["id"], e["type"], None)
                if e["status"] == "open" and eligible and not voted:
                    available.append({**detail, "voted": voted})
                else:
                    others.append({
                        "id": e["id"], "title": e["title"], "type": e["type"],
                        "status": e["status"], "voted": voted, "eligible": eligible,
                    })
            conn.close()
            self._json(200, {"ok": True, "available": available, "others": others})
            return

        if parts[:2] == ["api", "admin"] and len(parts) >= 3:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            conn = get_conn()
            if parts[2] == "elections" and len(parts) == 3:
                rows = conn.execute("SELECT * FROM elections ORDER BY rowid").fetchall()
                out = []
                for e in rows:
                    detail = build_election_detail(conn, e)
                    out.append({
                        "id": e["id"], "title": e["title"], "type": e["type"],
                        "status": e["status"], "created_at": e["created_at"],
                        "positions_count": len(detail["positions"]),
                        "ballots": detail["ballots"],
                        "eligible_count": detail["eligible_count"],
                    })
                conn.close()
                self._json(200, {"ok": True, "elections": out})
                return
            if parts[2] == "elections" and len(parts) == 4:
                row = conn.execute("SELECT * FROM elections WHERE id=?", (parts[3],)).fetchone()
                if not row:
                    conn.close()
                    self._json(404, {"ok": False, "error": "Election not found"})
                    return
                detail = build_election_detail(conn, row)
                conn.close()
                self._json(200, {"ok": True, **detail})
                return
            if parts[2] == "students" and len(parts) == 3:
                rows = [dict(r) for r in conn.execute("SELECT * FROM students ORDER BY id").fetchall()]
                conn.close()
                self._json(200, {"ok": True, "students": rows})
                return
            if parts[2] == "audit" and len(parts) == 3:
                rows = [dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 300").fetchall()]
                conn.close()
                self._json(200, {"ok": True, "log": rows})
                return
            if parts[2] == "export" and len(parts) == 4:
                res = compute_results(parts[3])
                conn.close()
                if not res:
                    self._json(404, {"ok": False, "error": "Election not found"})
                    return
                csv = "Position,Candidate,Votes,Percentage\n"
                for pos in res["positions"]:
                    for row in pos["rows"]:
                        csv += f'{pos["title"]},{row["name"]},{row["votes"]},{row["pct"]}%\n'
                self._text(200, csv, "text/csv; charset=utf-8")
                return
            conn.close()
            self._json(404, {"ok": False, "error": "Unknown endpoint"})
            return

        self._json(404, {"ok": False, "error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        parts = [unquote(p) for p in path.split("/") if p]
        data = self._json_body()

        if parts == ["api", "login"]:
            sid = str(data.get("student_id", "")).strip()
            pin = str(data.get("pin", "")).strip()
            conn = get_conn()
            student = conn.execute("SELECT * FROM students WHERE id=?", (sid,)).fetchone()
            conn.close()
            if not student or student["pin"] != pin or (not sid and not pin):
                self._json(401, {"ok": False, "error": "Invalid Student ID or PIN"})
                return
            tok = make_token("student", student["id"])
            self._json(200, {"ok": True, "token": tok, "student": {"id": student["id"], "name": student["name"], "department": student["department"], "year": student["year"]}})
            return

        if parts == ["api", "admin", "login"]:
            u = str(data.get("username", "")).strip()
            p = str(data.get("password", "")).strip()
            if ADMIN_USERS.get(u) != p:
                self._json(401, {"ok": False, "error": "Invalid admin credentials"})
                return
            tok = make_token("admin")
            conn = get_conn()
            audit(conn, f'Admin "{u}" logged in')
            conn.commit()
            conn.close()
            self._json(200, {"ok": True, "token": tok})
            return

        if parts == ["api", "vote"]:
            info = authorize(self, "student")
            if not info:
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            election_id = str(data.get("election_id", ""))
            selections = data.get("selections") or {}
            with write_lock:
                conn = get_conn()
                try:
                    election = conn.execute("SELECT * FROM elections WHERE id=?", (election_id,)).fetchone()
                    if not election:
                        self._json(404, {"ok": False, "error": "Election not found"})
                        return
                    if election["status"] != "open":
                        self._json(409, {"ok": False, "error": "Voting is not open"})
                        return
                    student = conn.execute("SELECT * FROM students WHERE id=?", (info["student_id"],)).fetchone()
                    if not student:
                        self._json(401, {"ok": False, "error": "Student not found"})
                        return
                    if election["type"] != "college-wide":
                        okd = conn.execute(
                            "SELECT 1 FROM election_departments WHERE election_id=? AND department=?",
                            (election_id, student["department"]),
                        ).fetchone()
                        if not okd:
                            self._json(403, {"ok": False, "error": "You are not eligible for this election"})
                            return
                    positions = conn.execute("SELECT id FROM positions WHERE election_id=?", (election_id,)).fetchall()
                    pos_ids = {r["id"] for r in positions}
                    if set(selections.keys()) != pos_ids:
                        self._json(400, {"ok": False, "error": "Every position must have exactly one selection"})
                        return
                    for pid, cid in selections.items():
                        if cid != "NOTA":
                            okc = conn.execute(
                                "SELECT 1 FROM candidates WHERE id=? AND position_id=?", (cid, pid)
                            ).fetchone()
                            if not okc:
                                self._json(400, {"ok": False, "error": "Invalid candidate selection"})
                                return
                    receipt = "SAE-" + secrets.token_hex(4).upper()
                    ts = now()
                    try:
                        conn.execute(
                            "INSERT INTO voter_status (election_id, student_id, receipt_no, voted_at) VALUES (?, ?, ?, ?)",
                            (election_id, info["student_id"], receipt, ts),
                        )
                    except (RowConflictError, sqlite3.IntegrityError):
                        self._json(409, {"ok": False, "error": "This student has already voted in this election"})
                        return
                    ballot_id = "b" + secrets.token_hex(5)
                    conn.executemany(
                        "INSERT INTO ballots (ballot_id, election_id, position_id, candidate_id, voted_at) VALUES (?, ?, ?, ?, ?)",
                        [(ballot_id, election_id, pid, cid, ts) for pid, cid in selections.items()],
                    )
                    self._json(200, {"ok": True, "receipt": receipt})
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": "Server error: " + str(ex)})
                finally:
                    conn.close()
            return

        if parts[:3] == ["api", "admin", "elections"] and len(parts) == 3 and path.count("/") == 3:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            validator = validate_election_payload(data)
            if validator:
                self._json(400, {"ok": False, "error": validator})
                return
            with write_lock:
                conn = get_conn()
                try:
                    eid = make_id("e_")
                    conn.execute(
                        "INSERT INTO elections (id, title, type, status, created_at) VALUES (?, ?, ?, ?, ?)",
                        (eid, data["title"].strip(), data["type"], "draft", now()),
                    )
                    replace_election_payload(conn, eid, data)
                    audit(conn, f'Created election "{data["title"].strip()}"')
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True, "id": eid})
            return

        if parts[:3] == ["api", "admin", "elections"] and len(parts) == 5 and parts[4] == "status":
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            status = data.get("status")
            if status not in STATUSES:
                self._json(400, {"ok": False, "error": "Invalid status"})
                return
            with write_lock:
                conn = get_conn()
                try:
                    row = conn.execute("SELECT * FROM elections WHERE id=?", (parts[3],)).fetchone()
                    if not row:
                        self._json(404, {"ok": False, "error": "Election not found"})
                        return
                    conn.execute("UPDATE elections SET status=? WHERE id=?", (status, parts[3]))
                    audit(conn, f'Set election "{row["title"]}" status to {status}')
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True})
            return

        if parts[:3] == ["api", "admin", "students"] and len(parts) == 4 and parts[3] == "import":
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            if not data.get("confirm"):
                records = data.get("records") or []
                seen = set()
                valid, duplicates, invalid = [], [], []
                for r in records:
                    sid = str(r.get("id", "")).strip()
                    name = str(r.get("name", "")).strip()
                    dept = str(r.get("department", "")).strip()
                    year = str(r.get("year", "")).strip() or "SE"
                    if not sid or not name or not dept:
                        invalid.append({"row": r, "reason": "Missing ID, name or department"})
                        continue
                    if sid in seen:
                        duplicates.append(sid)
                        continue
                    seen.add(sid)
                    with write_lock:
                        conn = get_conn()
                        try:
                            dup = conn.execute("SELECT 1 FROM students WHERE id=?", (sid,)).fetchone()
                        finally:
                            conn.close()
                    if dup:
                        duplicates.append(sid)
                        continue
                    valid.append({"id": sid, "name": name, "department": dept, "year": year, "pin": make_pin()})
                token = secrets.token_hex(8)
                uploads[token] = {"records": valid, "at": now()}
                self._json(200, {"ok": True, "upload_token": token, "valid": valid, "duplicates": duplicates, "invalid": invalid})
                return
            token = str(data.get("upload_token", ""))
            staged = uploads.pop(token, None)
            if not staged:
                self._json(400, {"ok": False, "error": "Upload session expired. Please try again."})
                return
            with write_lock:
                conn = get_conn()
                try:
                    inserted = 0
                    for r in staged["records"]:
                        try:
                            conn.execute(
                                "INSERT INTO students (id, name, department, year, pin) VALUES (?, ?, ?, ?, ?)",
                                (r["id"], r["name"], r["department"], r["year"], r["pin"]),
                            )
                            inserted += 1
                        except (RowConflictError, sqlite3.IntegrityError):
                            pass
                    audit(conn, f"Imported {inserted} students")
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True, "inserted": inserted})
            return

        self._json(404, {"ok": False, "error": "Unknown endpoint"})

    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path
        parts = [unquote(p) for p in path.split("/") if p]
        data = self._json_body()

        if parts[:3] == ["api", "admin", "elections"] and len(parts) == 4:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            eid = parts[3]
            with write_lock:
                conn = get_conn()
                try:
                    row = conn.execute("SELECT * FROM elections WHERE id=?", (eid,)).fetchone()
                    if not row:
                        self._json(404, {"ok": False, "error": "Election not found"})
                        return
                    has_ballots = conn.execute(
                        "SELECT COUNT(*) AS c FROM ballots WHERE election_id=?", (eid,)
                    ).fetchone()["c"] > 0
                    title = str(data.get("title", row["title"])).strip() or row["title"]
                    if has_ballots:
                        detail = build_election_detail(conn, row)
                        payload_positions = data.get("positions")
                        payload_depts = data.get("departments")
                        payload_type = data.get("type")
                        same = True
                        if payload_positions is not None and (
                            len(payload_positions) != len(detail["positions"])
                            or not all(
                                p.get("title") == dp["title"]
                                and sorted(x.get("id") for x in p.get("candidates", [])) == sorted(x["id"] for x in dp["candidates"])
                                for p, dp in zip(payload_positions, detail["positions"])
                            )
                        ):
                            same = False
                        if payload_depts is not None and set(payload_depts) != set(detail["departments"]):
                            same = False
                        if payload_type is not None and payload_type != detail["type"]:
                            same = False
                        if not same:
                            self._json(400, {"ok": False, "error": "Election structure is locked once ballots exist. Only the title can be changed."})
                            return
                        conn.execute("UPDATE elections SET title=? WHERE id=?", (title, eid))
                        audit(conn, f'Updated title of election "{title}"')
                        conn.commit()
                        self._json(200, {"ok": True})
                        return
                    validator = validate_election_payload(data)
                    if validator:
                        self._json(400, {"ok": False, "error": validator})
                        return
                    conn.execute("UPDATE elections SET type=?, title=? WHERE id=?", (data["type"], title, eid))
                    replace_election_payload(conn, eid, data)
                    audit(conn, f'Updated election "{title}"')
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True})
            return

        self._json(404, {"ok": False, "error": "Unknown endpoint"})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path
        parts = [unquote(p) for p in path.split("/") if p]

        if parts[:3] == ["api", "admin", "elections"] and len(parts) == 4:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            eid = parts[3]
            with write_lock:
                conn = get_conn()
                try:
                    row = conn.execute("SELECT * FROM elections WHERE id=?", (eid,)).fetchone()
                    if not row:
                        self._json(404, {"ok": False, "error": "Election not found"})
                        return
                    pids = [r["id"] for r in conn.execute("SELECT id FROM positions WHERE election_id=?", (eid,)).fetchall()]
                    if pids:
                        q = ",".join("?" * len(pids))
                        conn.execute(f"DELETE FROM candidates WHERE position_id IN ({q})", pids)
                        conn.execute(f"DELETE FROM ballots WHERE position_id IN ({q})", pids)
                    conn.execute("DELETE FROM positions WHERE election_id=?", (eid,))
                    conn.execute("DELETE FROM ballots WHERE election_id=?", (eid,))
                    conn.execute("DELETE FROM voter_status WHERE election_id=?", (eid,))
                    conn.execute("DELETE FROM election_departments WHERE election_id=?", (eid,))
                    conn.execute("DELETE FROM elections WHERE id=?", (eid,))
                    audit(conn, f'Deleted election "{row["title"]}"')
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True})
            return

        if parts[:3] == ["api", "admin", "students"] and len(parts) == 4:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            sid = parts[3]
            with write_lock:
                conn = get_conn()
                try:
                    conn.execute("DELETE FROM voter_status WHERE student_id=?", (sid,))
                    conn.execute("DELETE FROM students WHERE id=?", (sid,))
                    audit(conn, f'Removed student "{sid}"')
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True})
            return

        if parts[:3] == ["api", "admin", "audit"] and len(parts) == 3:
            if not authorize(self, "admin"):
                self._json(401, {"ok": False, "error": "Not authorized"})
                return
            with write_lock:
                conn = get_conn()
                try:
                    conn.execute("DELETE FROM audit_log")
                    conn.commit()
                except Exception as ex:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    self._json(500, {"ok": False, "error": str(ex)})
                    return
                finally:
                    conn.close()
            self._json(200, {"ok": True})
            return

        self._json(404, {"ok": False, "error": "Unknown endpoint"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    if TURSO_ENABLED:
        print("  Database : Turso (%s)" % turso_db.endpoint)
    else:
        missing = []
        if not TURSO_URL:
            missing.append("turso_url.txt")
        if not TURSO_TOKEN:
            missing.append("turso_token.txt")
        print("  Database : %s (LOCAL SQLite - Turso NOT configured; add %s)" % (DB_PATH, ", ".join(missing)))
    init_db()
    heal_demo_election()

    class NoReuseServer(ThreadingHTTPServer):
        allow_reuse_address = False

    try:
        server = NoReuseServer((HOST, PORT), Handler)
    except OSError:
        print("  Port %d is already in use - the server appears to already be running." % PORT)
        print("  Just open http://localhost:%d in your browser." % PORT)
        raise SystemExit(0)
    print("=" * 56)
    print("  Student Association Election System")
    print(f"  URL      : http://{HOST}:{PORT}")
    print("  Admin    : admin / admin123")
    print("  Demo voters (ID / PIN):")
    print("    AIML_01 / 482913   | CSE_07 / 216907")
    print("    CSE_12 / 548320    | ETC_15 / 905614")
    print("  Press Ctrl+C to stop.")
    print("=" * 56)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()