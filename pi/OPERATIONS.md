# SensorPush → Weather Underground → GitHub — Operations & Recovery

The garden weather pipeline runs on a **single Raspberry Pi** (the sole writer).
Every ~5 minutes it reads the SensorPush HTP.xw over Bluetooth, uploads to Weather
Underground, and commits the reading to this repo (monthly CSV + `data/recent.json`
+ `latest.json`). The gh-pages chart reads `data/*.csv`; the Android widget reads
`latest.json`.

This document describes the hardened, self-recovering design: what runs, how it
recovers from a power cut with **zero human action**, how to install it, how to
run the doctor, and the one manual procedure that must NOT be automated (a wipe).

---

## Canonical on-Pi layout (established by `install.sh`)

```
/home/pi/sensorpush/
  publish.py sensorpush.py ...   # scripts (copied from the repo's pi/ dir)
  venv/                          # python venv with bleak
  .env                           # WU_ID, WU_KEY, DEPLOY_KEY, KNOWN_HOSTS, WATCHDOG_STALE_SEC
  state/status.json              # heartbeat (OUTSIDE the git tree — never committed)
  state/.publish.lock            # single-writer advisory lock (OUTSIDE the git tree)
  state/reconcile-spool.json     # crash-safe union snapshot during a reconcile (transient)
  wunderground-killi/            # git working copy (remote origin = GitHub over deploy key)
    data/YYYY-MM.csv             # monthly archive (source of truth)
    data/recent.json             # last 2016 readings (~7d) — DERIVED from the CSVs
    latest.json                  # newest reading — DERIVED
```

The publish service runs as **root** (deterministic `rfkill`/BLE). One consequence:
git objects in the pi-owned `.git` are written root-owned. Keep **one writer**
(the service). If a human must commit on the Pi, use `sudo`.

---

## Units

| Unit | Type | Purpose |
|------|------|---------|
| `sensorpush-bt.service` | oneshot, `RemainAfterExit=yes` | On boot: `rfkill unblock` → power controller on → **wait until ready** (bounded 60s). |
| `sensorpush-publish.service` | oneshot | One publish cycle. `publish.py` self-bounds to `MAX_CYCLE_SEC=300s`; `TimeoutStartSec=360` is a strictly-larger backstop that kills a genuinely stuck run so the next tick is clean. |
| `sensorpush-publish.timer` | timer | `OnBootSec=90s` (monotonic first-fire) + `OnCalendar=*:0/5` + `Persistent=true`. |
| `sensorpush-watchdog.service` | oneshot | Health check + corrective action (`--watchdog`). `TimeoutStartSec=300` covers the worst-case wedge-heal path. |
| `sensorpush-watchdog.timer` | timer | `OnBootSec=3min`, `OnUnitActiveSec=10min`. |

All are **enabled** at install, so they come back after any reboot.

Ordering: the publish service is `After=network-online.target time-sync.target
bluetooth.target sensorpush-bt.service` and `Wants=` the soft ones. Every
dependency is **soft** (`Wants=`, never `Requires=`) — a down WiFi or a wedged
adapter *degrades* a cycle (it skips and retries) but never *fails* the unit.

---

## Power-loss / reboot auto-recovery (the headline)

After a power cut, with **zero human action**:

1. systemd starts `bluetooth.service` (+ `hciuart` for the onboard UART radio);
   `/etc/bluetooth/main.conf` `[Policy] AutoEnable=true` powers the controller.
2. `sensorpush-bt.service` runs `rfkill unblock bluetooth` (clearing any persisted
   soft-block that survives power loss), powers the controller on, and **waits**
   until it reports `Powered: yes` / `UP RUNNING`.
3. `sensorpush-publish.timer` fires at **`OnBootSec=90s`** (monotonic clock — the
   only trustworthy first-fire on this RTC-less Pi), then every 5 minutes.
4. Each cycle first checks the **clock is NTP-synchronized** and `epoch >=
   2026-01-01`. Until sync lands it logs `skip: clock-unsynced` and retries — so a
   stale `fake-hwclock` time can never poison the archive or the WU history. The
   moment sync lands, publishing resumes.
5. The watchdog independently brings a wedged adapter up and, if the last success
   is stale (> 20 min), forces a publish cycle — recovering without a human.

**Correctness beats the 2-minute SLA:** if NTP is slow the first cycles correctly
skip until synced. That is intended.

---

## Resilience mechanisms (per requirement)

- **Never hang (R2):** `publish.py` enforces an **overall in-code cycle deadline**
  (`MAX_CYCLE_SEC=300s`, CLOCK_MONOTONIC): the BLE phase is capped to 200s, WU ends
  by 270s, and `write_files()` (the data-critical step) runs right after WU — so a
  good reading is **always persisted before** `TimeoutStartSec=360` could fire. git
  network ops are clamped to the remaining budget. `asyncio.wait_for` bounds every
  BLE attempt (and is itself capped to the time left); 15s WU HTTP timeout; every
  git subprocess is bounded and killed as a process group (`start_new_session` +
  `killpg`) so the ssh grandchild dies too. The only thing systemd's kill can ever
  interrupt is a `git push` (already committed locally → ships next cycle).
- **BLE resilience (R3):** up to **4** attempts with backoff+jitter; between failures
  a tiered controller reset — tier-1 `bluetoothctl power off/on`, tier-2 `hciconfig
  hci0 down/up` + purge the device's stale GATT cache, tier-3 `systemctl restart
  bluetooth` (reachable because `BLE_TRIES=4`; clears a *powered-but-wedged*
  bluetoothd that a power-cycle can't). **Any** read exception (timeout, `BleakError`,
  `struct.error`, `OSError`, …) is retryable, so every failure mode engages the reset
  ladder. Each attempt uses a fresh event loop (a reset invalidates BlueZ D-Bus
  paths). After the last attempt / when time runs out it skips the cycle
  gracefully — never crashes.
- **Clock integrity (R4):** in-code NTP-sync + floor guard is authoritative; plus a
  monotonic `last_success_epoch` floor rejects a clock that ran backwards.
- **Atomic writes (R5):** every file is written temp → `fsync` → `os.replace` →
  dir-`fsync`. The CSV is a read-validate-**rewrite** (torn rows dropped, header
  healed), never a bare append. `recent.json`/`latest.json` are derived from the
  CSV truth so they can never disagree.
- **Git self-heal (R6):** stale `.git/*.lock` sweep every cycle (safe: single
  writer); index rebuild on **any** unreadable-index error (corrupt / bad / *smaller
  than expected* / short-read / damaged); the reading is **committed locally first**,
  so a network failure never loses it. On a non-fast-forward push the pipeline does a
  **crash-safe union-by-epoch reconcile**: fetch → union remote+local by epoch →
  **spool the union durably to `state/reconcile-spool.json` (fsync'd, OUTSIDE the
  repo)** → `reset --hard origin/main` → rewrite → commit (rc-checked) → push. The
  spool is cleared only *after* the union is committed (durable in a git object). If
  power is cut in the `reset`→`commit` window, the next cycle's `_recover_spool()`
  re-merges the spooled readings into the working tree and commits them — so a
  divergence + power-yank **never loses an unpushed reading**. A partial/short read of
  any CSV **aborts the reconcile before the reset** (never feeds a truncated union
  into a destructive rewrite). A deliberate wipe is out of scope (see below).
- **Failure isolation (R7):** WU, write, and git failures are independent; `main()`
  wraps **both** the cycle and the watchdog and never propagates an unhandled
  exception, exiting 0 on any again-next-cycle condition. Integer env vars
  (`WATCHDOG_STALE_SEC`) and `status.json` fields are parsed defensively — a blank or
  malformed value falls back to its default instead of crashing at import.
- **Observability + watchdog (R8):** one structured `<6>`/`<3>` line per cycle to
  journald (persistent, capped at 200M) + `state/status.json` heartbeat
  (last-success epoch, consecutive-failure count, last error). The watchdog timer
  self-heals a stale heartbeat / wedged adapter — and when the heartbeat is stale
  **with the adapter still reporting powered** (the wedged-bluetoothd signature) it
  restarts `bluetooth.service` before forcing a publish cycle, not just a
  power-cycle.

---

## Install

On the Pi (as root), from the repo checkout:

```bash
sudo ./wunderground-killi/pi/install.sh
```

Idempotent — safe to re-run over a fresh reflash or the existing ad-hoc setup. It
provisions the venv + `bleak`, copies scripts, writes a `.env` template, wires git
(SSH remote, system `safe.directory`, committer identity, no hooks, `known_hosts`,
deploy-key perms, `main` tracking `origin/main`), sets BlueZ `AutoEnable=true`,
enables `systemd-time-wait-sync` + `hciuart`, and enables the **matching**
network-online provider (`NetworkManager-wait-online` / `systemd-networkd-wait-online`
/ `dhcpcd` — whichever this image actually uses, so `After=network-online.target` is
a real wait rather than a passively-satisfied no-op; if none is recognized publish
still degrades safely), makes journald persistent+capped, sets `noatime`, and
installs + `enable --now`s all units.

Then fill in `WU_ID` / `WU_KEY` in `/home/pi/sensorpush/.env` and place the deploy
private key at `DEPLOY_KEY` (default `/home/pi/.ssh/deploy_key`).

## Doctor / self-test

```bash
sudo ./wunderground-killi/pi/selftest.sh
```

Checks clock sync, BT adapter power + rfkill, `bleak` import, **one real sensor
read**, git remote/auth (`ls-remote`), committer identity, no stale locks, branch,
WU creds, free disk, and unit enablement. Prints PASS/FAIL per check; exits
non-zero if anything FAILs.

---

## Manual maintenance

### Deliberate remote wipe / truncation — do NOT let auto-reconcile fight it

Union-by-epoch reconcile is scoped to *divergence by addition*. A wipe done on
GitHub while the Pi keeps publishing would be **resurrected** by a naive union. So
a wipe MUST be done on the Pi with the service stopped:

```bash
sudo systemctl stop sensorpush-publish.timer
# ... perform the remote wipe on GitHub ...
sudo -u pi git -C /home/pi/sensorpush/wunderground-killi fetch origin
sudo -u pi git -C /home/pi/sensorpush/wunderground-killi reset --hard origin/main
sudo systemctl start sensorpush-publish.timer
```

Now `local == remote`, so there is no divergence for reconcile to fight.

### Disk / history

One commit per 5 min grows history unbounded (~105k commits/year) and the monthly
CSVs grow the card. The watchdog + selftest monitor free space. `git gc` is safe to
run in a maintenance window; **do not** rewrite/squash history on `main` — the
raw.githubusercontent chart links and the widget depend on it.

### Useful commands

```bash
journalctl -u sensorpush-publish -f              # live publish log
journalctl -u sensorpush-publish -o json | tail  # structured fields
cat /home/pi/sensorpush/state/status.json        # heartbeat
systemctl list-timers 'sensorpush*'              # next fire times
sudo systemctl start sensorpush-publish.service   # force one cycle now
```
