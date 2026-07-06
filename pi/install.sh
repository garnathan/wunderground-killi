#!/usr/bin/env bash
# SensorPush publisher — idempotent provisioner.
#
# ONE command provisions everything needed for zero-human power-loss recovery:
#   * canonical on-Pi layout + venv + deps (bleak)
#   * scripts copied to the canonical base dir
#   * .env template (WU + deploy-key)
#   * git wiring: SSH remote, system safe.directory, committer identity, no hooks,
#     known_hosts, deploy-key perms, main tracking origin/main, stale-lock sweep
#   * BlueZ [Policy] AutoEnable=true + persistent rfkill unblock
#   * time-sync gate + network-online provider + hciuart enabled
#   * persistent, capped journald
#   * noatime rootfs (SD-card longevity)
#   * all systemd units installed + enabled --now
#
# Idempotent: safe to re-run over a fresh reflash OR today's ad-hoc setup. It
# ESTABLISHES the canonical state rather than assuming the current one. Run as root.
set -euo pipefail

# ---- canonical layout (this installer defines it; nothing is guessed) --------
PIUSER="${PIUSER:-pi}"
BASE="/home/${PIUSER}/sensorpush"
REPO="${BASE}/wunderground-killi"
DATA="${REPO}/data"
VENV="${BASE}/venv"
STATE="${BASE}/state"
ENV_FILE="${BASE}/.env"
SSH_DIR="/home/${PIUSER}/.ssh"
KNOWN_HOSTS="${SSH_DIR}/known_hosts"
DEPLOY_KEY_DEFAULT="${SSH_DIR}/deploy_key"
GIT_REMOTE_URL="git@github.com:garnathan/wunderground-killi.git"
SYSTEMD_DIR="/etc/systemd/system"

# Source dir = the pi/ directory this script lives in (inside the repo checkout).
SRC="$(cd "$(dirname "$0")" && pwd)"

log(){ printf '[install] %s\n' "$*"; }
die(){ printf '[install] FATAL: %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "must run as root (sudo ./install.sh)"
command -v git >/dev/null 2>&1 || die "git not installed"
command -v python3 >/dev/null 2>&1 || die "python3 not installed"

# ---- 1. directories ----------------------------------------------------------
log "creating layout under ${BASE}"
install -d -o "${PIUSER}" -g "${PIUSER}" "${BASE}" "${STATE}"
install -d -o "${PIUSER}" -g "${PIUSER}" -m 700 "${SSH_DIR}"

# ---- 2. repo working copy ----------------------------------------------------
if [ ! -d "${REPO}/.git" ]; then
  log "cloning repo into ${REPO}"
  sudo -u "${PIUSER}" git clone "${GIT_REMOTE_URL}" "${REPO}" \
    || die "clone failed (check network + deploy key ${DEPLOY_KEY_DEFAULT})"
fi
install -d -o "${PIUSER}" -g "${PIUSER}" "${DATA}"

# ---- 3. scripts to the canonical base dir ------------------------------------
log "installing scripts into ${BASE}"
for f in publish.py sensorpush.py discover.py gatt_read.py decode_adv.py; do
  [ -f "${SRC}/${f}" ] && install -o "${PIUSER}" -g "${PIUSER}" -m 755 "${SRC}/${f}" "${BASE}/${f}"
done
[ -f "${BASE}/publish.py" ]   || die "publish.py missing after copy"
[ -f "${BASE}/sensorpush.py" ] || die "sensorpush.py missing after copy"

# ---- 4. venv + deps ----------------------------------------------------------
if [ ! -x "${VENV}/bin/python" ]; then
  log "creating venv"
  sudo -u "${PIUSER}" python3 -m venv "${VENV}"
fi
log "installing python deps (bleak)"
sudo -u "${PIUSER}" "${VENV}/bin/pip" install --quiet --upgrade pip
sudo -u "${PIUSER}" "${VENV}/bin/pip" install --quiet --upgrade bleak

# ---- 5. .env template --------------------------------------------------------
if [ ! -f "${ENV_FILE}" ]; then
  log "writing .env template (fill in WU creds)"
  cat > "${ENV_FILE}" <<EOF
# Weather Underground station credentials (REQUIRED for WU upload).
WU_ID=
WU_KEY=
# SSH deploy key used for git push (private key path on the Pi).
DEPLOY_KEY=${DEPLOY_KEY_DEFAULT}
# Known-hosts file used for the github.com host key.
KNOWN_HOSTS=${KNOWN_HOSTS}
# Watchdog staleness threshold in seconds (default 1200 = 4 missed cycles).
WATCHDOG_STALE_SEC=1200
EOF
  chown "${PIUSER}:${PIUSER}" "${ENV_FILE}"
fi
chmod 600 "${ENV_FILE}"
set -a
# shellcheck source=/dev/null
. "${ENV_FILE}"
set +a
DEPLOY_KEY="${DEPLOY_KEY:-${DEPLOY_KEY_DEFAULT}}"

# ---- 6. git wiring (system-scope so it holds for root running against pi tree) -
log "wiring git"
git -C "${REPO}" remote get-url origin >/dev/null 2>&1 \
  && git -C "${REPO}" remote set-url origin "${GIT_REMOTE_URL}" \
  || git -C "${REPO}" remote add origin "${GIT_REMOTE_URL}"
git config --system --get-all safe.directory 2>/dev/null | grep -qxF "${REPO}" \
  || git config --system --add safe.directory "${REPO}"
git config --system user.name  "sensorpush-pi"
git config --system user.email "sensorpush@localhost"
git config --system core.hooksPath /dev/null
[ -f "${DEPLOY_KEY}" ] && chmod 600 "${DEPLOY_KEY}" || log "WARN: deploy key ${DEPLOY_KEY} not present yet"

# known_hosts (idempotent: rebuild + sort -u; accept-new TOFU, never prompt).
touch "${KNOWN_HOSTS}"; chown "${PIUSER}:${PIUSER}" "${KNOWN_HOSTS}"; chmod 644 "${KNOWN_HOSTS}"
{ cat "${KNOWN_HOSTS}"; ssh-keyscan -t ed25519,ecdsa,rsa github.com 2>/dev/null; } \
  | sort -u > "${KNOWN_HOSTS}.new" && mv "${KNOWN_HOSTS}.new" "${KNOWN_HOSTS}"
chown "${PIUSER}:${PIUSER}" "${KNOWN_HOSTS}"

# branch main tracks origin/main (best-effort; requires a prior fetch).
git -C "${REPO}" fetch --no-tags origin main >/dev/null 2>&1 || true
git -C "${REPO}" branch --set-upstream-to=origin/main main >/dev/null 2>&1 || true

# one-off stale-lock sweep.
find "${REPO}/.git" -maxdepth 3 -name '*.lock' -type f -delete 2>/dev/null || true

# ---- 7. BlueZ AutoEnable (idempotent [Policy] edit) --------------------------
MAIN_CONF="/etc/bluetooth/main.conf"
if [ -f "${MAIN_CONF}" ]; then
  log "ensuring ${MAIN_CONF} [Policy] AutoEnable=true"
  python3 - "${MAIN_CONF}" <<'PY'
import sys, re
p = sys.argv[1]
txt = open(p).read()
lines = txt.splitlines()
out, in_policy, set_done, changed = [], False, False, False
for ln in lines:
    s = ln.strip()
    if s.startswith("[") and s.endswith("]"):
        if in_policy and not set_done:
            out.append("AutoEnable=true"); set_done = True; changed = True
        in_policy = (s.lower() == "[policy]")
    if in_policy and re.match(r'\s*#?\s*AutoEnable\s*=', ln, re.I):
        if ln.strip() != "AutoEnable=true":
            ln = "AutoEnable=true"; changed = True
        set_done = True
    out.append(ln)
if in_policy and not set_done:
    out.append("AutoEnable=true"); set_done = True; changed = True
if not any(l.strip().lower() == "[policy]" for l in out):
    out += ["", "[Policy]", "AutoEnable=true"]; changed = True
if changed:
    open(p, "w").write("\n".join(out) + "\n")
    print("changed")
PY
fi

# ---- 8. rfkill unblock + BT / time / network enablement ----------------------
log "unblocking bluetooth + enabling boot units"
rfkill unblock bluetooth 2>/dev/null || true
systemctl enable bluetooth.service 2>/dev/null || true
systemctl enable hciuart.service 2>/dev/null || true          # onboard UART BT
systemctl enable systemd-time-wait-sync.service 2>/dev/null || true  # make time-sync.target mean SYNCED
# network-online provider: enable the one that MATCHES the active network stack,
# so publish.service's After=network-online.target is a real wait rather than a
# passively-satisfied no-op. On classic Raspberry Pi OS (dhcpcd) neither NM nor
# networkd is present; dhcpcd provides its own wait. If none is recognized,
# publish still degrades safely (the reading is written locally, WU/git retry).
if systemctl is-enabled NetworkManager.service >/dev/null 2>&1 \
   || systemctl is-active NetworkManager.service >/dev/null 2>&1; then
  log "network-online provider: NetworkManager-wait-online"
  systemctl enable NetworkManager-wait-online.service 2>/dev/null || true
elif systemctl is-enabled systemd-networkd.service >/dev/null 2>&1 \
   || systemctl is-active systemd-networkd.service >/dev/null 2>&1; then
  log "network-online provider: systemd-networkd-wait-online"
  systemctl enable systemd-networkd-wait-online.service 2>/dev/null || true
elif systemctl list-unit-files dhcpcd.service >/dev/null 2>&1 \
   && systemctl list-unit-files dhcpcd.service 2>/dev/null | grep -q dhcpcd; then
  log "network-online provider: dhcpcd (Raspberry Pi OS default)"
  systemctl enable dhcpcd.service 2>/dev/null || true
else
  log "WARN: no recognized network-online provider; publish relies on failure-isolation"
fi
# restart bluetooth so AutoEnable takes effect now.
systemctl restart bluetooth 2>/dev/null || true

# ---- 9. persistent + capped journald (survive power loss, bounded SD wear) ---
log "configuring persistent journald"
install -d /var/log/journal
install -d /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/sensorpush.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=200M
RuntimeMaxUse=64M
EOF
systemctl restart systemd-journald 2>/dev/null || true

# ---- 10. SD-card longevity: noatime on rootfs --------------------------------
if mount | grep -q ' / ' && ! mount | grep ' / ' | grep -q noatime; then
  log "remounting / noatime"
  mount -o remount,noatime / 2>/dev/null || true
fi
if [ -f /etc/fstab ] && ! grep -Eq '^\s*[^#].*\s/\s.*noatime' /etc/fstab; then
  log "adding noatime to / in /etc/fstab"
  # append noatime to the mount options of the / entry only.
  awk 'BEGIN{OFS="\t"} /^[^#]/ && $2=="/" && $4 !~ /noatime/ {$4=$4",noatime"} {print}' \
    /etc/fstab > /etc/fstab.new && mv /etc/fstab.new /etc/fstab
fi

# ---- 11. install + enable systemd units --------------------------------------
log "installing systemd units"
for u in sensorpush-bt.service sensorpush-publish.service sensorpush-publish.timer \
         sensorpush-watchdog.service sensorpush-watchdog.timer; do
  install -m 644 "${SRC}/systemd/${u}" "${SYSTEMD_DIR}/${u}"
done
systemctl daemon-reload
systemctl enable --now sensorpush-bt.service
systemctl enable --now sensorpush-publish.timer
systemctl enable --now sensorpush-watchdog.timer

log "done. Fill in ${ENV_FILE} (WU_ID/WU_KEY) if blank, then run: ${SRC}/selftest.sh"
