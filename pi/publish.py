#!/usr/bin/env python3
"""SensorPush -> Weather Underground -> GitHub publisher (hardened).

Every cycle (driven by sensorpush-publish.timer): read the SensorPush over BLE,
upload to Weather Underground, atomically append the reading to the monthly CSV +
derive recent.json / latest.json, then commit & push into the wunderground-killi
GitHub repo.

This file wraps the PROVEN-correct, UNCHANGED sensorpush.read_once() with
resilience: bounded BLE retry + adapter reset, a clock-integrity guard, atomic
crash-safe writes, git self-heal (stale-lock sweep + crash-safe union-by-epoch
reconcile on non-fast-forward), full failure isolation, an overall in-code cycle
deadline, a heartbeat/status file, and a self-healing watchdog mode
(`publish.py --watchdog`).

The calibration/formulas in sensorpush.py are proven correct and are NOT touched
here -- all robustness lives in this wrapper.
"""
import os, sys, json, csv, io, time, glob, random, signal, subprocess
import tempfile, fcntl, re, traceback
import urllib.parse, urllib.request, urllib.error
import asyncio

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from sensorpush import read_once, ADDR          # reuse existing BLE reader + device addr

# bleak exception types -- classify BLE failures as retryable, not bugs.
try:
    from bleak.exc import BleakError, BleakDeviceNotFoundError
except ImportError:                              # pragma: no cover - depends on bleak version
    try:
        from bleak.exc import BleakError
    except ImportError:
        class BleakError(Exception):
            pass
    class BleakDeviceNotFoundError(BleakError):
        pass


# ---------------------------------------------------------------------------
# Structured logging: one line per event, journald severity via <N> prefix.
# Defined FIRST so module-scope helpers can log safely at import time.
# ---------------------------------------------------------------------------
def log(level, msg):
    """level>=6 -> INFO (<6>), else ERR (<3>). journald maps the prefix to PRIORITY."""
    prefix = "<6>" if level >= 6 else "<3>"
    print(f"{prefix}{msg}", flush=True)


def _int_env(name, default):
    """Parse an integer env var DEFENSIVELY. A blank or malformed value (a user
    can edit .env) must NEVER raise -- fall back to the default and log. This is
    used lazily (never at module import) so a bad .env can't crash the process
    before main()'s guards can catch it."""
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    try:
        return int(v)
    except (TypeError, ValueError):
        log(3, f"invalid {name}={v!r}; using default {default}")
        return default


# ---------------------------------------------------------------------------
# Canonical paths + tunables (install.sh establishes these paths on the Pi).
# ---------------------------------------------------------------------------
PUBLISH_VERSION = "2.1.0"
REPO      = os.path.join(HERE, "wunderground-killi")
DATA      = os.path.join(REPO, "data")
STATE_DIR = os.path.join(HERE, "state")
STATUS_PATH = os.path.join(STATE_DIR, "status.json")
# The advisory lock and the reconcile spool live OUTSIDE the git working copy so
# they are never git-add'd / pushed to the public repo (there is no .gitignore).
LOCK_PATH   = os.path.join(STATE_DIR, ".publish.lock")
SPOOL_PATH  = os.path.join(STATE_DIR, "reconcile-spool.json")

RECENT_N  = 2016                                 # ~7 days at 5-min cadence
COLS = ["epoch", "ts", "temperature_c", "temperature_f", "humidity_pct",
        "pressure_station_mbar", "pressure_sealevel_mbar", "dew_point_c",
        "heat_index_c", "wu_status"]
FLOAT_COLS = {"temperature_c", "temperature_f", "humidity_pct",
              "pressure_station_mbar", "pressure_sealevel_mbar",
              "dew_point_c", "heat_index_c"}

# Clock integrity: 2026-01-01T00:00:00Z. A reading dated before this is a
# pre-NTP / fake-hwclock artefact and must never enter the archive.
FLOOR_EPOCH = 1767225600

# --- Overall cycle time budget (monotonic) --------------------------------
# The whole cycle (BLE + WU + git) is bounded IN CODE so the graceful skip / a
# persisted reading is guaranteed BEFORE systemd's TimeoutStartSec fires. The
# service TimeoutStartSec (360s) is a strictly-larger backstop.
#   MAX_CYCLE_SEC .......... hard in-code wall-clock cap for one cycle.
#   BLE_RESERVE_SEC ........ time held back after BLE for WU + git.
#   WU_RESERVE_SEC ......... time held back after WU for git.
# write_files() (the data-critical step) runs right after WU, i.e. no later than
# MAX_CYCLE_SEC - WU_RESERVE_SEC, well under TimeoutStartSec -- so a good reading
# can never be lost to a systemd kill.
MAX_CYCLE_SEC   = 300
BLE_RESERVE_SEC = 100
WU_RESERVE_SEC  = 30

# BLE retry budget. BLE_TRIES=4 so the escalation ladder can reach the tier-3
# reset (bluetoothd restart) when earlier attempts fail fast (the wedged-daemon
# signature). Each attempt is bounded by asyncio.wait_for AND capped to the time
# remaining before the BLE sub-deadline, so BLE can never overrun the budget.
BLE_TRIES = 4
BLE_ATTEMPT_TIMEOUT = 65                          # scan 20 + connect 30 + GATT + margin
BLE_MIN_ATTEMPT = 25                              # never start an attempt with less time left
BLE_BACKOFF = [3, 8, 20]                          # seconds between attempts (+ jitter)
BLE_JITTER = 3.0

# Weather Underground.
WU_URL    = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"
WU_TRIES  = 4
WU_TIMEOUT = 15

# git subprocess timeouts.
GIT_LOCAL_TIMEOUT = 20
GIT_NET_TIMEOUT   = 45
GIT_NET_TRIES     = 3

DISK_FLOOR = 64 * 1024 * 1024                     # 64 MB free minimum to write
GIT_LOCKS = ["index.lock", "HEAD.lock", "config.lock", "packed-refs.lock",
             "ORIG_HEAD.lock", "logs/HEAD.lock", "shallow.lock"]

WATCHDOG_BOOT_GRACE_SEC = 300
WATCHDOG_STALE_DEFAULT  = 1200                    # 20 min (~4 missed cycles)

# Overall cycle deadline (CLOCK_MONOTONIC). Set once per invocation in cycle();
# consulted by the BLE and git layers so no phase overruns MAX_CYCLE_SEC. A fresh
# oneshot process per cycle means this is process-local, never shared state.
_CYCLE_DEADLINE = None


def _time_left():
    if _CYCLE_DEADLINE is None:
        return float("inf")
    return _CYCLE_DEADLINE - time.monotonic()


# ---------------------------------------------------------------------------
# Environment.
# ---------------------------------------------------------------------------
def load_env(path=None):
    path = path or os.path.join(HERE, ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


# ---------------------------------------------------------------------------
# Atomic, crash-safe file primitives (R5).
# ---------------------------------------------------------------------------
def _fsync_dir(dirpath):
    try:
        dfd = os.open(dirpath, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError as e:                          # some filesystems reject dir-fsync
        log(3, f"dir-fsync skipped {dirpath}: {e}")


def atomic_write_bytes(path, data):
    """mkstemp (same dir) -> write -> flush+fsync -> fchmod 644 -> os.replace -> fsync dir.

    Never truncates in place and never appends; a power cut can only leave the
    prior committed file or an orphan .tmp-* (swept at startup), never a torn one.
    """
    dirpath = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dirpath, prefix=".tmp-" + os.path.basename(path) + "-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            os.fchmod(f.fileno(), 0o644)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    _fsync_dir(dirpath)


def sweep_orphans():
    """Remove .tmp-* leftovers from an interrupted write so they never accumulate
    or get git-add'd."""
    for base in (DATA, REPO, STATE_DIR):
        if not os.path.isdir(base):
            continue
        for p in glob.glob(os.path.join(base, ".tmp-*")):
            try:
                os.unlink(p)
            except OSError:
                pass


def free_bytes(path):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


# ---------------------------------------------------------------------------
# CSV helpers (read-validate-rewrite; the monthly file is the source of truth).
# ---------------------------------------------------------------------------
class CsvReadError(Exception):
    """A CSV could not be read in full (I/O or parse error). Callers MUST treat a
    partial read as fatal -- never let it drive a truncating rewrite or a
    destructive reconcile (that would silently drop the unread tail)."""


def _isint(s):
    try:
        int(str(s))
        return True
    except (TypeError, ValueError):
        return False


def _read_csv_rows(path):
    """Return valid row dicts. Drops torn rows (wrong field count) and header
    lines; the header is regenerated on write so a header-less file self-heals.

    Raises CsvReadError on an I/O or parse error MID-FILE, rather than silently
    returning the partial rows read so far -- a truncated read must never be
    mistaken for 'the file has N rows' and drive a truncating rewrite."""
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, newline="") as f:
            for rec in csv.reader(f):
                if not rec:
                    continue
                if rec == COLS:                    # skip any (possibly duplicated) header
                    continue
                if len(rec) != len(COLS):          # torn / short row from a prior crash
                    continue
                rows.append(dict(zip(COLS, rec)))
    except (OSError, csv.Error) as e:
        raise CsvReadError(f"{path}: {type(e).__name__}: {e}") from e
    return rows


def _serialize_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLS)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, "") for k in COLS})
    return buf.getvalue().encode()


def _typed_row(r):
    """Cast a CSV string row to the typed dict shape recent.json/latest.json readers expect."""
    out = {}
    for k in COLS:
        v = r.get(k, "")
        if k == "epoch":
            out[k] = int(v) if _isint(v) else 0
        elif k in FLOAT_COLS:
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = None
        else:
            out[k] = v
    return out


def _prev_month(tm):
    y, m = tm.tm_year, tm.tm_mon
    if m == 1:
        return f"{y - 1:04d}-12"
    return f"{y:04d}-{m - 1:02d}"


def write_files(d):
    """Atomically append `d` to its UTC-month CSV (read-validate-rewrite), then
    DERIVE recent.json + latest.json from the CSV truth so they can never drift.

    A CsvReadError reading the month CSV propagates BEFORE any write, so the
    on-disk CSV is left intact (the reading is retried next cycle) rather than
    truncated. recent.json is best-effort (regenerated next cycle if its inputs
    can't be read); it never blocks the source-of-truth CSV write."""
    os.makedirs(DATA, exist_ok=True)
    month = time.strftime("%Y-%m", time.gmtime(d["epoch"]))
    csv_path = os.path.join(DATA, f"{month}.csv")

    rows = _read_csv_rows(csv_path)                # may raise CsvReadError -> abort, CSV intact
    by_epoch = {int(r["epoch"]): r for r in rows if _isint(r.get("epoch"))}
    by_epoch[int(d["epoch"])] = {k: d.get(k) for k in COLS}   # new reading wins on collision
    merged = [by_epoch[e] for e in sorted(by_epoch)]
    atomic_write_bytes(csv_path, _serialize_csv(merged))       # reading now durably persisted

    # latest.json = the just-written reading (guaranteed newest by the monotonic guard).
    atomic_write_bytes(os.path.join(REPO, "latest.json"),
                       json.dumps(d, indent=2).encode())

    # recent.json: tail(RECENT_N) of prev+current month, derived from CSV (best-effort).
    try:
        tm = time.gmtime(d["epoch"])
        prev_path = os.path.join(DATA, _prev_month(tm) + ".csv")
        combined = _read_csv_rows(prev_path) + _read_csv_rows(csv_path)
        win = {int(r["epoch"]): r for r in combined if _isint(r.get("epoch"))}
        ordered = [win[e] for e in sorted(win)][-RECENT_N:]
        atomic_write_bytes(os.path.join(DATA, "recent.json"),
                           json.dumps([_typed_row(r) for r in ordered]).encode())
    except CsvReadError as e:
        log(3, f"recent.json deferred (csv read error): {e}")


def _rebuild_derived_all():
    """Regenerate recent.json + latest.json from ALL month CSVs (used post-reconcile).
    Best-effort: a CsvReadError just defers the derived files to the next cycle;
    the CSVs themselves are the source of truth."""
    try:
        rows = []
        for f in sorted(glob.glob(os.path.join(DATA, "*.csv"))):
            rows += _read_csv_rows(f)
    except CsvReadError as e:
        log(3, f"derived rebuild deferred (csv read error): {e}")
        return
    by = {int(r["epoch"]): r for r in rows if _isint(r.get("epoch"))}
    if not by:
        return
    ordered = [by[e] for e in sorted(by)]
    atomic_write_bytes(os.path.join(DATA, "recent.json"),
                       json.dumps([_typed_row(r) for r in ordered[-RECENT_N:]]).encode())
    atomic_write_bytes(os.path.join(REPO, "latest.json"),
                       json.dumps(_typed_row(ordered[-1]), indent=2).encode())


# ---------------------------------------------------------------------------
# Heartbeat / status file (R8). Lives OUTSIDE the git working copy.
# ---------------------------------------------------------------------------
def read_status():
    try:
        with open(STATUS_PATH) as f:
            s = json.load(f)
        if isinstance(s, dict):
            return s
    except (OSError, ValueError):
        pass
    return {}


def _status_int(status, key):
    """Read an integer field from status.json defensively -- an old-schema or
    hand-edited non-numeric value must never raise (this runs on the watchdog
    self-heal path, which must never crash)."""
    v = status.get(key, 0)
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def write_status(**kw):
    os.makedirs(STATE_DIR, exist_ok=True)
    st = read_status()
    st.update(kw)
    st["schema_version"] = 1
    st["pid"] = os.getpid()
    st["publish_version"] = PUBLISH_VERSION
    atomic_write_bytes(STATUS_PATH, json.dumps(st, indent=2).encode())


# ---------------------------------------------------------------------------
# Clock integrity (R4 / R-BOOT-1).
# ---------------------------------------------------------------------------
def clock_synced():
    if os.path.exists("/run/systemd/timesync/synchronized"):
        return True
    rc, out, _ = _sh(["timedatectl", "show", "-p", "NTPSynchronized", "--value"], timeout=5)
    return rc == 0 and out.strip() == "yes"


# ---------------------------------------------------------------------------
# Bounded shell-out helper (never hangs).
# ---------------------------------------------------------------------------
def _sh(args, timeout=15):
    try:
        r = subprocess.run(args, timeout=timeout, capture_output=True, text=True)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    except (FileNotFoundError, OSError) as e:
        return 127, "", str(e)


# ---------------------------------------------------------------------------
# BLE read with bounded retry + tiered controller reset (R2 / R3 / R-BLE-*).
# ---------------------------------------------------------------------------
def _reset_adapter(tier):
    """Escalating controller reset BETWEEN failed attempts. Synchronous -- runs
    between fresh asyncio.run() attempts, never inside the coroutine. Every
    subprocess is bounded so even recovery can't hang.

    tier 0 -> power-cycle the controller; tier 1 -> link reset + purge stale GATT
    cache; tier >=2 -> restart bluetoothd (clears a powered-but-wedged daemon),
    then re-power. BLE_TRIES=4 guarantees tier 2 is reachable."""
    log(6, f"ble reset tier={tier}")
    _sh(["rfkill", "unblock", "bluetooth"])       # unblock by TYPE (index unstable across boots)
    if tier <= 0:                                 # tier-1: power cycle the controller
        _sh(["bluetoothctl", "power", "off"])
        time.sleep(2)
        _sh(["bluetoothctl", "power", "on"])
    elif tier == 1:                               # tier-2: link reset + purge stale GATT cache
        _sh(["hciconfig", "hci0", "down"])
        time.sleep(1)
        _sh(["hciconfig", "hci0", "up"])
        _sh(["bluetoothctl", "remove", ADDR])
    else:                                         # tier-3: restart bluetoothd, then re-power
        _sh(["systemctl", "restart", "bluetooth"], timeout=30)
        time.sleep(3)
        _sh(["rfkill", "unblock", "bluetooth"])
        _sh(["bluetoothctl", "power", "on"])
        _sh(["bluetoothctl", "remove", ADDR])
    time.sleep(3)                                 # let the adapter settle before re-scan


def read_sensor(deadline):
    """Bounded retry driver around the UNCHANGED read_once(). Fresh event loop
    per attempt (a reset invalidates BlueZ's D-Bus object paths), reset+backoff
    between attempts. ANY read failure is treated as retryable so it engages the
    reset ladder (R3). `deadline` is a CLOCK_MONOTONIC bound: no attempt is
    started -- and no attempt's wait_for exceeds -- the time remaining, so BLE can
    never overrun the cycle budget. Raises RuntimeError once attempts/time are
    exhausted."""
    last_err = None
    for attempt in range(BLE_TRIES):
        remaining = deadline - time.monotonic()
        if remaining < BLE_MIN_ATTEMPT:
            log(3, f"ble: out of time (remaining {remaining:.0f}s) after {attempt} attempts")
            break
        to = int(min(BLE_ATTEMPT_TIMEOUT, remaining))
        try:
            return asyncio.run(asyncio.wait_for(read_once(), timeout=to))
        except Exception as e:                     # any failure is retryable (R3/R7)
            last_err = f"{type(e).__name__}: {e}"
            log(3, f"ble attempt {attempt + 1}/{BLE_TRIES} failed: {last_err}")
        if attempt < BLE_TRIES - 1:
            # Only reset + back off if another meaningful attempt can still fit.
            if (deadline - time.monotonic()) < (BLE_MIN_ATTEMPT + 5):
                log(3, "ble: no time for another attempt; giving up this cycle")
                break
            _reset_adapter(attempt)
            backoff = BLE_BACKOFF[min(attempt, len(BLE_BACKOFF) - 1)] + random.uniform(0, BLE_JITTER)
            time.sleep(max(0.0, min(backoff, deadline - time.monotonic() - BLE_MIN_ATTEMPT)))
    raise RuntimeError(last_err or "ble read failed (deadline)")


# ---------------------------------------------------------------------------
# Weather Underground upload (bounded; isolated so a WU failure never blocks
# the data write). Formulas preserved from the original.
# ---------------------------------------------------------------------------
def upload_wu(d, deadline=None):
    wu_id, wu_key = os.environ.get("WU_ID"), os.environ.get("WU_KEY")
    if not wu_id or not wu_key:
        return "skipped (no creds)"
    params = {
        "ID": wu_id, "PASSWORD": wu_key, "action": "updateraw",
        "dateutc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "tempf": round(d["temperature_f"], 2),
        "humidity": round(d["humidity_pct"], 2),
        "baromin": round(d["pressure_sealevel_mbar"] / 33.8639, 3),  # mbar -> inHg (sea level)
        "dewptf": round(d["dew_point_c"] * 9 / 5 + 32, 2),
        "softwaretype": "home-pi-sensorpush",
    }
    url = WU_URL + "?" + urllib.parse.urlencode(params)
    last = ""
    for i in range(WU_TRIES):
        # Respect the cycle budget: never let WU push the cycle past its deadline
        # (write_files must still run). A skip here is isolated and harmless.
        if deadline is not None and (deadline - time.monotonic()) < 3:
            return last or "skipped: deadline"
        to = WU_TIMEOUT
        if deadline is not None:
            to = int(max(3, min(WU_TIMEOUT, deadline - time.monotonic())))
        try:
            with urllib.request.urlopen(url, timeout=to) as r:
                body = r.read().decode("utf-8", "replace").strip()
            if "success" in body.lower():
                return "success" if i == 0 else f"success (try {i + 1})"
            last = f"error: {body[:60]}"
        except urllib.error.HTTPError as e:
            last = f"error: HTTP {e.code}"
        except Exception as e:
            last = f"error: {type(e).__name__}: {e}"
        if i < WU_TRIES - 1:
            time.sleep(2)
    return last


# ---------------------------------------------------------------------------
# git: bounded runner, stale-lock sweep, crash-safe union-by-epoch self-heal (R6).
# ---------------------------------------------------------------------------
def _git_env():
    env = dict(os.environ)
    key = os.environ.get("DEPLOY_KEY", os.path.expanduser("~/.ssh/deploy_key"))
    kh = os.environ.get("KNOWN_HOSTS", os.path.expanduser("~/.ssh/known_hosts"))
    env["GIT_SSH_COMMAND"] = (
        f"ssh -i {key} -o IdentitiesOnly=yes -o BatchMode=yes "
        f"-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile={kh} "
        f"-o ConnectTimeout=15 -o ServerAliveInterval=5 -o ServerAliveCountMax=3"
    )
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def _git(args, timeout=GIT_LOCAL_TIMEOUT, env=None):
    """Run one git command in its own process group; SIGKILL the whole group on
    timeout so the ssh grandchild dies too (subprocess timeout alone leaks it)."""
    env = env or _git_env()
    p = subprocess.Popen(["git", "-C", REPO, *args], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, start_new_session=True, env=env)
    try:
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass
        try:
            p.communicate(timeout=5)
        except Exception:
            pass
        return 124, "", "git-timeout"


def _git_net(args, timeout=GIT_NET_TIMEOUT, tries=GIT_NET_TRIES):
    """Retry network git ops (fetch/push/ls-remote) with exponential backoff+jitter.
    Each attempt's timeout is clamped to the time left in the cycle budget so a
    network op can never overrun MAX_CYCLE_SEC."""
    env = _git_env()
    delay = 2.0
    last = (1, "", "no-attempt")
    for i in range(tries):
        left = _time_left()
        if left <= 2:
            return (124, "", "deadline")
        rc, out, err = _git(args, timeout=int(min(timeout, left)), env=env)
        if rc == 0:
            return rc, out, err
        last = (rc, out, err)
        if i < tries - 1:
            if _time_left() <= 2:
                break
            time.sleep(min(delay + random.uniform(0, delay * 0.25), max(0.0, _time_left() - 1)))
            delay *= 2
    return last


def sweep_git_locks():
    """Remove power-cut orphaned lock files. Safe because the single Pi is the
    SINGLE writer (per ground truth) and the oneshot is serialized -- any lock is
    necessarily orphaned. Guarded by age>30s as belt-and-braces vs a concurrent human git."""
    gd = os.path.join(REPO, ".git")
    pats = [os.path.join(gd, x) for x in GIT_LOCKS]
    pats += glob.glob(os.path.join(gd, "refs", "**", "*.lock"), recursive=True)
    pats += glob.glob(os.path.join(gd, "refs", "remotes", "origin", "*.lock"))
    for p in pats:
        try:
            if os.path.exists(p) and (time.time() - os.path.getmtime(p) > 30):
                os.remove(p)
                log(6, f"removed stale git lock {p}")
        except OSError:
            pass


def _ensure_index():
    """Return (rc, out, err) of `git status --porcelain`, rebuilding the index
    from HEAD if it is unreadable/corrupt/truncated. Any status failure whose
    message names the index (corrupt, bad, unreadable, truncated / 'smaller than
    expected', short read, damaged, ...) triggers the rebuild -- so no single
    git message string can leave the git leg permanently wedged. Safe: the single
    Pi is the SOLE writer, so an unreadable index is always ours to rebuild."""
    rc, out, err = _git(["status", "--porcelain"], timeout=GIT_LOCAL_TIMEOUT)
    if rc == 0:
        return rc, out, err
    blob = (err or "") + (out or "")
    if re.search(r"index", blob, re.I) and re.search(
            r"corrupt|bad index|unable to read|cannot read|smaller than expected|"
            r"short read|too small|damaged|malformed|invalid",
            blob, re.I):
        log(3, f"unreadable git index detected -> rebuilding from HEAD ({blob.strip()[:120]})")
        try:
            os.remove(os.path.join(REPO, ".git", "index"))
        except OSError:
            pass
        _git(["reset", "-q"], timeout=GIT_LOCAL_TIMEOUT)
        return _git(["status", "--porcelain"], timeout=GIT_LOCAL_TIMEOUT)
    return rc, out, err


def _ensure_branch():
    """Guarantee HEAD is on `main`; recreate it at the current commit if detached
    or on the wrong branch (preserves any pending committed readings)."""
    rc, out, _ = _git(["symbolic-ref", "--short", "-q", "HEAD"], timeout=10)
    if rc == 0 and out.strip() == "main":
        return
    _git(["checkout", "-B", "main"], timeout=GIT_LOCAL_TIMEOUT)


_REJECT_RE = re.compile(r"non-fast-forward|fetch first|Updates were rejected|! \[rejected\]", re.I)


def _read_remote_csv(name):
    rc, out, _ = _git(["show", f"origin/main:data/{name}"], timeout=GIT_LOCAL_TIMEOUT)
    if rc != 0:
        return []
    rows = []
    for rec in csv.reader(io.StringIO(out)):
        if not rec or rec == COLS or len(rec) != len(COLS):
            continue
        rows.append(dict(zip(COLS, rec)))
    return rows


def _bad_status(s):
    s = (s or "").lower()
    return (not s) or s.startswith("error") or "skip" in s


def _prefer_row(remote_row, local_row):
    """Deterministic collision winner: the LOCAL row (the Pi authored that epoch),
    unless the local row's wu_status is error/skipped and the remote's is good."""
    if _bad_status(local_row.get("wu_status")) and not _bad_status(remote_row.get("wu_status")):
        return remote_row
    return local_row


def _union_rows(local, remote):
    by = {}
    for r in remote:
        if _isint(r.get("epoch")):
            by[int(r["epoch"])] = r
    for r in local:
        if not _isint(r.get("epoch")):
            continue
        e = int(r["epoch"])
        by[e] = _prefer_row(by[e], r) if e in by else r
    return [by[e] for e in sorted(by)]


def _union_csv_names():
    names = set()
    for f in glob.glob(os.path.join(DATA, "*.csv")):
        names.add(os.path.basename(f))
    rc, out, _ = _git(["ls-tree", "-r", "--name-only", "origin/main", "--", "data/"],
                      timeout=GIT_LOCAL_TIMEOUT)
    if rc == 0:
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("data/") and line.endswith(".csv"):
                names.add(os.path.basename(line))
    return sorted(names)


# --- reconcile durability spool (R6 crash-safety) --------------------------
# The union is captured DURABLY OUTSIDE the git tree BEFORE the destructive
# `reset --hard`, so a power cut / SD error in the reset->rewrite window can be
# recovered on the next cycle instead of silently losing every unpushed reading.
def _spool_write(unioned):
    os.makedirs(STATE_DIR, exist_ok=True)
    payload = {"schema": 1, "created": int(time.time()), "csvs": unioned}
    atomic_write_bytes(SPOOL_PATH, json.dumps(payload).encode())


def _spool_load():
    try:
        with open(SPOOL_PATH) as f:
            p = json.load(f)
        if isinstance(p, dict) and p.get("schema") == 1 and isinstance(p.get("csvs"), dict):
            return p["csvs"]
    except (OSError, ValueError):
        pass
    return None


def _spool_clear():
    try:
        os.remove(SPOOL_PATH)
    except OSError:
        pass


def _write_union_to_worktree(csvs):
    """Atomically write the union CSV sets to the working tree + rebuild derived."""
    os.makedirs(DATA, exist_ok=True)
    for name, rows in csvs.items():
        atomic_write_bytes(os.path.join(DATA, name), _serialize_csv(rows))
    _rebuild_derived_all()


def _merge_spool_into_worktree(csvs):
    """Recovery: UNION the spooled readings with whatever is CURRENTLY in the
    working tree (which may contain readings added after the spool was written)
    so no reading is ever dropped, then write. May raise CsvReadError."""
    os.makedirs(DATA, exist_ok=True)
    for name, rows in csvs.items():
        cur = _read_csv_rows(os.path.join(DATA, name))
        atomic_write_bytes(os.path.join(DATA, name),
                           _serialize_csv(_union_rows(cur, rows)))
    _rebuild_derived_all()


def _recover_spool():
    """If a prior reconcile was interrupted after its `reset --hard` but before its
    union was committed, restore the spooled readings into the working tree so the
    normal add/commit/push below re-persists them. Returns True when it is SAFE to
    clear the spool after this cycle commits (no spool present, or the spool was
    merged in successfully); False if the spool is present but could not be merged
    (retry next cycle -- do NOT clear)."""
    csvs = _spool_load()
    if csvs is None:
        if os.path.exists(SPOOL_PATH):
            # An unreadable spool (only reachable via FS corruption of an fsync'd
            # atomic file) can never be applied by anything automatic; drop it so
            # the git leg is not wedged forever.
            log(3, "reconcile spool unreadable -> discarding")
            _spool_clear()
        return True
    log(6, "recovering interrupted reconcile from spool")
    try:
        _merge_spool_into_worktree(csvs)
        return True
    except CsvReadError as e:
        log(3, f"spool recovery deferred (csv read error): {e}")
        return False


def _reconcile_once():
    """One fetch -> union-by-epoch -> reset --hard origin/main -> rewrite -> commit
    -> push cycle, made CRASH-SAFE: the union is spooled durably OUTSIDE the repo
    before the destructive reset, and cleared only once the union is committed
    (durable in a git object + the working tree). A partial read of a working-tree
    CSV aborts BEFORE the reset. Returns 'pushed' | 'offline' | 'retry' | 'git-error: ...'."""
    rc, _, err = _git_net(["fetch", "--no-tags", "origin", "main"])
    if rc != 0:
        return "offline"

    # Build the union from the working tree + remote. A partial/short read is
    # FATAL here: proceeding would feed a truncated set into the destructive reset
    # and silently drop readings, so abort (retry next cycle) with the tree intact.
    unioned = {}
    total = 0
    try:
        for name in _union_csv_names():
            rows = _union_rows(_read_csv_rows(os.path.join(DATA, name)), _read_remote_csv(name))
            unioned[name] = rows
            total += len(rows)
    except CsvReadError as e:
        return f"git-error: csv read aborted ({e})"

    # Persist the union DURABLY (fsync'd, outside the repo) BEFORE the reset.
    _spool_write(unioned)

    rc, _, err = _git(["reset", "--hard", "origin/main"], timeout=30)
    if rc != 0:
        # spool retained -> readings recovered next cycle
        return f"git-error: reset {(err or '').strip()[:120]}"

    # Re-apply the union on top of the clean remote base (atomic, crash-safe).
    try:
        _write_union_to_worktree(unioned)
    except OSError as e:
        # working tree is now the bare remote; the spool still holds the union, so
        # the next cycle's _recover_spool() restores it -- nothing is lost.
        return f"git-error: apply {type(e).__name__}: {e}"

    _git(["add", "-A"], timeout=GIT_LOCAL_TIMEOUT)
    rc, st, _ = _git(["status", "--porcelain"], timeout=GIT_LOCAL_TIMEOUT)
    if not (st or "").strip():
        _spool_clear()                             # union already identical to remote
        return "pushed"
    rc, _, err = _git(["commit", "-q", "--no-verify", "-m",
                       f"reconcile: union-by-epoch ({total} rows)"], timeout=GIT_LOCAL_TIMEOUT)
    if rc != 0:
        # commit failed -> HEAD is still bare remote; keep the spool so the union
        # is recovered + retried next cycle rather than reporting a false success.
        return f"git-error: commit {(err or '').strip()[:120]}"
    _spool_clear()                                 # union now durable in a git object + worktree
    rc, out, err = _git_net(["push", "origin", "HEAD:refs/heads/main"])
    if rc == 0:
        return "pushed"
    if _REJECT_RE.search((err or "") + (out or "")):
        return "retry"                             # remote advanced during reconcile
    return f"git-error: push {((err or '') + (out or '')).strip()[:120]}"


def _push_with_heal():
    rc, out, err = _git_net(["push", "origin", "HEAD:refs/heads/main"])
    if rc == 0:
        return "pushed"
    blob = (err or "") + (out or "")
    if not _REJECT_RE.search(blob):
        return f"git-error: push {blob.strip()[:120]}"
    # Non-fast-forward: self-heal via union-by-epoch, bounded to 3 attempts and the
    # cycle deadline.
    for _ in range(3):
        if _time_left() <= 3:
            return "push-deferred: deadline"
        r = _reconcile_once()
        if r == "pushed":
            return "pushed (reconciled)"
        if r == "offline":
            return "push-deferred: offline"
        if r != "retry":
            return r
    return "push-deferred: reconcile-exhausted"


def git_sync(d):
    sweep_git_locks()
    if free_bytes(REPO) < DISK_FLOOR:
        return "git-skip: low-disk"
    rc, out, err = _ensure_index()
    if rc != 0:
        return f"git-error: status {((err or '') + (out or '')).strip()[:120]}"
    _ensure_branch()
    # Restore an interrupted reconcile's readings before staging (R6 crash-safety).
    safe_to_clear = _recover_spool()
    rc, _, err = _git(["add", "-A"], timeout=GIT_LOCAL_TIMEOUT)
    if rc != 0:
        return f"git-error: add {(err or '').strip()[:120]}"
    rc, st, _ = _git(["status", "--porcelain"], timeout=GIT_LOCAL_TIMEOUT)
    if not st.strip():
        if safe_to_clear:
            _spool_clear()                         # working tree == HEAD; recovered rows committed
        return "no-change"
    msg = f"reading {d['ts']} ({d['temperature_c']}C {d['humidity_pct']}%)"
    rc, _, err = _git(["commit", "-q", "--no-verify", "-m", msg], timeout=GIT_LOCAL_TIMEOUT)
    if rc != 0:
        return f"git-error: commit {(err or '').strip()[:120]}"
    if safe_to_clear:
        _spool_clear()                             # reading(s) now durable in a git object
    return _push_with_heal()


# ---------------------------------------------------------------------------
# Single-writer advisory lock (belt-and-braces vs the serialized oneshot).
# ---------------------------------------------------------------------------
def acquire_lock():
    os.makedirs(STATE_DIR, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        os.close(fd)
        return None
    return fd


# ---------------------------------------------------------------------------
# One publish cycle (R7: never propagates; always records status; exit 0 on any
# handled/again-next-cycle condition).
# ---------------------------------------------------------------------------
def cycle():
    global _CYCLE_DEADLINE
    _CYCLE_DEADLINE = time.monotonic() + MAX_CYCLE_SEC
    load_env()
    sweep_orphans()

    lock_fd = acquire_lock()
    if lock_fd is None:
        log(6, "overlapping run detected; exiting cleanly")
        return 0

    status = read_status()
    consec = _status_int(status, "consecutive_failures")
    last_success = _status_int(status, "last_success_epoch")
    attempt_epoch = int(time.time())
    synced = clock_synced()
    saved = {"done": False}

    def save(**kw):
        saved["done"] = True
        write_status(last_attempt_epoch=attempt_epoch, clock_synced=synced,
                     consecutive_failures=consec, **kw)

    try:
        # R4 / R-BOOT-1: authoritative clock guard BEFORE any read/WU/write.
        if not synced or attempt_epoch < FLOOR_EPOCH:
            consec += 1
            save(last_error="skip: clock-unsynced")
            log(3, f"skip: clock-unsynced synced={synced} epoch={attempt_epoch}")
            return 0

        # Disk-space guard (avoid churning the SD card under a full disk).
        guard_dir = DATA if os.path.isdir(DATA) else HERE
        if free_bytes(guard_dir) < DISK_FLOOR:
            consec += 1
            save(last_error="skip: low-disk")
            log(3, "skip: low-disk")
            return 0

        # BLE read (R2/R3) -- bounded retry + deadline; a give-up is a graceful skip
        # (it short-circuits before WU/git, so it never risks a good reading).
        try:
            d = read_sensor(_CYCLE_DEADLINE - BLE_RESERVE_SEC)
        except Exception as e:
            consec += 1
            save(last_error=f"ble_giveup: {e}")
            log(3, f"ble_giveup after {BLE_TRIES} attempts: {e}")
            return 0

        d["epoch"] = int(time.time())
        # Re-check the floor + monotonic archive floor with the reading's epoch.
        if d["epoch"] < FLOOR_EPOCH:
            consec += 1
            save(last_error="skip: clock-unsynced")
            log(3, f"skip: clock-unsynced (post-read) epoch={d['epoch']}")
            return 0
        if d["epoch"] < last_success:
            consec += 1
            save(last_error="skip: clock-regressed")
            log(3, f"skip: clock-regressed epoch={d['epoch']} < last_success={last_success}")
            return 0

        # WU (isolated + deadline-bounded -- a failure/timeout here NEVER blocks the
        # write/commit, and can never push the cycle past its deadline).
        try:
            d["wu_status"] = upload_wu(d, _CYCLE_DEADLINE - WU_RESERVE_SEC)
        except Exception as e:
            d["wu_status"] = f"error: {type(e).__name__}"

        # Atomic write (isolated). Runs at latest ~MAX_CYCLE_SEC - WU_RESERVE_SEC,
        # i.e. well under TimeoutStartSec, so a good reading is always persisted.
        try:
            write_files(d)
        except Exception as e:
            consec += 1
            save(last_error=f"write-fail: {type(e).__name__}: {e}",
                 last_wu_status=d.get("wu_status"))
            log(3, f"write-fail: {type(e).__name__}: {e}")
            return 0

        # git (isolated -- a failure never loses the reading; it ships next cycle).
        try:
            git_status = git_sync(d)
        except Exception as e:
            git_status = f"git-error: {type(e).__name__}: {e}"

        consec = 0
        save(last_success_epoch=d["epoch"], last_error="",
             last_wu_status=d["wu_status"], last_git_status=git_status,
             last_reading=d)
        log(6, f"ok ts={d['ts']} temp={d['temperature_c']} hum={d['humidity_pct']} "
               f"press={d['pressure_sealevel_mbar']} wu={d['wu_status']} git={git_status}")
        return 0
    finally:
        if not saved["done"]:
            try:
                write_status(last_attempt_epoch=attempt_epoch, clock_synced=synced,
                             consecutive_failures=consec + 1,
                             last_error="interrupted (SIGTERM/timeout)")
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Watchdog (R8): separate timer runs `publish.py --watchdog`. Self-heals a stale
# heartbeat / wedged adapter WITHOUT a human. Never crashes; never reboots.
# ---------------------------------------------------------------------------
def _bt_powered():
    rc, out, _ = _sh(["bluetoothctl", "show"], timeout=15)
    if rc == 0 and "Powered: yes" in out:
        return True
    rc, out, _ = _sh(["hciconfig", "hci0"], timeout=15)
    return rc == 0 and "UP RUNNING" in out


def _restart_bluetoothd():
    _sh(["systemctl", "restart", "bluetooth"], timeout=30)
    time.sleep(3)
    _sh(["rfkill", "unblock", "bluetooth"])
    _sh(["bluetoothctl", "power", "on"], timeout=15)


def watchdog():
    load_env()
    stale_sec = _int_env("WATCHDOG_STALE_SEC", WATCHDOG_STALE_DEFAULT)

    # Boot-grace: staleness math is meaningless right after boot.
    try:
        up = time.clock_gettime(time.CLOCK_BOOTTIME)
    except (AttributeError, OSError):
        up = float(WATCHDOG_BOOT_GRACE_SEC + 1)
    if up < WATCHDOG_BOOT_GRACE_SEC:
        log(6, f"watchdog: boot-grace (up={up:.0f}s)")
        return 0

    # A publish run in progress owns the pipeline -- don't interfere.
    rc, _, _ = _sh(["systemctl", "is-active", "--quiet", "sensorpush-publish.service"], timeout=20)
    if rc == 0:
        log(6, "watchdog: publish active; nothing to do")
        return 0

    # Directly correct a down adapter even before the heartbeat goes stale.
    bt_restarted = False
    if not _bt_powered():
        log(3, "watchdog: adapter down -> bringing up")
        _sh(["rfkill", "unblock", "bluetooth"])
        _sh(["bluetoothctl", "power", "on"], timeout=15)
        if not _bt_powered():
            _restart_bluetoothd()
            bt_restarted = True

    if not clock_synced():
        log(6, "watchdog: clock unsynced; staleness check skipped")
        return 0

    status = read_status()
    last_success = _status_int(status, "last_success_epoch")
    if last_success == 0:
        log(6, "watchdog: no successful cycle recorded yet")
        return 0
    age = int(time.time()) - last_success
    if age <= stale_sec:
        log(6, f"watchdog: heartbeat fresh ({age}s)")
        return 0

    # STALE. A stale heartbeat with a POWERED adapter is the signature of a
    # wedged bluetoothd (D-Bus responsive but every LE connect fails), which a
    # mere power-cycle can't clear -- so restart bluetoothd here even though it
    # reports powered, unless we already restarted it above this run.
    log(3, f"watchdog: STALE ({age}s > {stale_sec}s) -> corrective reset + publish")
    if not bt_restarted:
        _restart_bluetoothd()
    _sh(["systemctl", "reset-failed", "sensorpush-publish.service"], timeout=20)
    _sh(["systemctl", "start", "sensorpush-publish.service"], timeout=20)
    return 0


# ---------------------------------------------------------------------------
# Entry point (R2/R7): SIGTERM -> clean exit so `finally` writes status.
# ---------------------------------------------------------------------------
def _on_sigterm(*_):
    log(3, "received SIGTERM; exiting for cleanup")
    sys.exit(143)


def main():
    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        if "--watchdog" in sys.argv:
            return watchdog()
        return cycle()
    except SystemExit:
        raise
    except Exception as e:
        # Genuinely unexpected internal state -- record, log, exit non-zero. This
        # guard covers BOTH the cycle and watchdog paths so main() never lets an
        # exception propagate (R7).
        try:
            write_status(last_attempt_epoch=int(time.time()),
                         last_error=f"unexpected: {type(e).__name__}: {e}")
        except Exception:
            pass
        log(3, f"unexpected: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
