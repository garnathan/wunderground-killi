#!/usr/bin/env bash
# SensorPush publisher — doctor / self-test. Verifies every dependency needed for
# unattended operation and prints a PASS/FAIL report. Run at deploy time (as root
# so the checks see the same environment the root-run publish service does).
#
#   sudo /home/pi/sensorpush/wunderground-killi/pi/selftest.sh
#
# Exits non-zero if ANY check FAILs (WARN does not fail the run).
set -uo pipefail

PIUSER="${PIUSER:-pi}"
BASE="/home/${PIUSER}/sensorpush"
REPO="${BASE}/wunderground-killi"
VENV="${BASE}/venv"
ENV_FILE="${BASE}/.env"
FLOOR_EPOCH=1767225600
GIT_REMOTE_EXPECTED="git@github.com:garnathan/wunderground-killi.git"

PASS=0; FAIL=0; WARN=0
ok(){   printf '  PASS  %s\n' "$*"; PASS=$((PASS+1)); }
bad(){  printf '  FAIL  %s\n' "$*"; FAIL=$((FAIL+1)); }
warn(){ printf '  WARN  %s\n' "$*"; WARN=$((WARN+1)); }

# load .env for WU + deploy key
if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck source=/dev/null
  . "${ENV_FILE}"
  set +a
fi
DEPLOY_KEY="${DEPLOY_KEY:-/home/${PIUSER}/.ssh/deploy_key}"
KNOWN_HOSTS="${KNOWN_HOSTS:-/home/${PIUSER}/.ssh/known_hosts}"

echo "== SensorPush selftest =="

# 1. Clock synchronized + sane.
if [ -e /run/systemd/timesync/synchronized ] \
   || [ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null)" = "yes" ]; then
  ok "clock NTP-synchronized"
else
  bad "clock NOT synchronized (publish will correctly SKIP until it is)"
fi
NOW="$(date +%s)"
if [ "${NOW}" -ge "${FLOOR_EPOCH}" ]; then ok "epoch ${NOW} >= floor ${FLOOR_EPOCH}"
else bad "epoch ${NOW} below floor ${FLOOR_EPOCH} (stale/pre-NTP clock)"; fi

# 2. Bluetooth adapter present + powered.
if bluetoothctl show 2>/dev/null | grep -q "Powered: yes" \
   || hciconfig hci0 2>/dev/null | grep -q "UP RUNNING"; then
  ok "BT controller present + powered"
else
  bad "BT controller not powered (run: rfkill unblock bluetooth; bluetoothctl power on)"
fi
if rfkill list bluetooth 2>/dev/null | grep -q "Soft blocked: yes"; then
  bad "BT is rfkill soft-blocked"
else
  ok "BT not rfkill soft-blocked"
fi

# 3. bleak import in the venv.
if [ -x "${VENV}/bin/python" ] && "${VENV}/bin/python" -c "import bleak" 2>/dev/null; then
  ok "bleak imports in venv"
else
  bad "bleak not importable in ${VENV}"
fi

# 4. Sensor reachable (one real read).
if [ -x "${VENV}/bin/python" ]; then
  if timeout 90 "${VENV}/bin/python" "${BASE}/sensorpush.py" --json >/tmp/sp_read.$$ 2>/tmp/sp_err.$$; then
    ok "sensor read OK: $(head -c120 /tmp/sp_read.$$)"
  else
    bad "sensor read FAILED: $(head -c120 /tmp/sp_err.$$)"
  fi
  rm -f /tmp/sp_read.$$ /tmp/sp_err.$$
else
  bad "venv python missing; cannot test sensor read"
fi

# 5. git remote URL.
URL="$(git -C "${REPO}" remote get-url origin 2>/dev/null || true)"
if [ "${URL}" = "${GIT_REMOTE_EXPECTED}" ]; then ok "git remote = ${URL}"
else bad "git remote is '${URL}', expected '${GIT_REMOTE_EXPECTED}'"; fi

# 6. git status runs without dubious-ownership (safe.directory effective for this user).
if git -C "${REPO}" status --porcelain >/dev/null 2>/tmp/gs_err.$$; then
  ok "git status OK (safe.directory effective)"
else
  bad "git status failed: $(head -c120 /tmp/gs_err.$$)"
fi
rm -f /tmp/gs_err.$$

# 7. committer identity + hooksPath.
if [ -n "$(git config --get user.name || true)" ] && [ -n "$(git config --get user.email || true)" ]; then
  ok "git committer identity set ($(git config --get user.name))"
else
  bad "git committer identity missing (commits will fail)"
fi
if [ "$(git config --get core.hooksPath || true)" = "/dev/null" ]; then ok "git hooks disabled"
else warn "core.hooksPath not /dev/null (a repo hook could hang the oneshot)"; fi

# 8. no stale locks.
if find "${REPO}/.git" -maxdepth 3 -name '*.lock' -type f 2>/dev/null | grep -q .; then
  bad "stale .git/*.lock present: $(find "${REPO}/.git" -maxdepth 3 -name '*.lock' -type f | tr '\n' ' ')"
else
  ok "no stale .git locks"
fi

# 9. branch main tracking origin/main.
BR="$(git -C "${REPO}" symbolic-ref --short -q HEAD || echo '(detached)')"
UP="$(git -C "${REPO}" rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)"
if [ "${BR}" = "main" ] && [ "${UP}" = "origin/main" ]; then ok "on main tracking origin/main"
else warn "branch=${BR} upstream=${UP:-none} (reconcile still pushes HEAD:refs/heads/main)"; fi

# 10. git auth + reachability (bounded).
export GIT_SSH_COMMAND="ssh -i ${DEPLOY_KEY} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=${KNOWN_HOSTS} -o ConnectTimeout=15"
export GIT_TERMINAL_PROMPT=0
if timeout 15 git -C "${REPO}" ls-remote origin main 2>/tmp/lr_err.$$ | grep -Eq '^[0-9a-f]{40}\s+refs/heads/main'; then
  ok "git ls-remote authenticated + reachable"
else
  bad "git ls-remote FAILED: $(head -c120 /tmp/lr_err.$$)"
fi
rm -f /tmp/lr_err.$$

# 11. WU creds present.
if [ -n "${WU_ID:-}" ] && [ -n "${WU_KEY:-}" ]; then ok "WU creds present"
else warn "WU_ID/WU_KEY blank in ${ENV_FILE} (WU upload will be skipped)"; fi

# 12. free disk space (>= 200 MB on the repo mount).
AVAIL_KB="$(df --output=avail "${REPO}" 2>/dev/null | tail -1 | tr -d ' ')"
if [ -n "${AVAIL_KB}" ] && [ "${AVAIL_KB}" -ge 204800 ]; then ok "disk free ${AVAIL_KB}KB (>=200MB)"
else bad "low disk: ${AVAIL_KB:-?}KB free on repo mount"; fi

# 13. units enabled.
for u in sensorpush-bt.service sensorpush-publish.timer sensorpush-watchdog.timer \
         systemd-time-wait-sync.service; do
  if systemctl is-enabled "${u}" >/dev/null 2>&1; then ok "${u} enabled"
  else warn "${u} not enabled"; fi
done

echo "== summary: ${PASS} pass, ${FAIL} fail, ${WARN} warn =="
[ "${FAIL}" -eq 0 ]
