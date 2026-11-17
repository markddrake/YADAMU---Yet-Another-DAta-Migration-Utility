#!/bin/bash
#
# One-time build of the read-only base disks the KVM container boots from, starting from
# Teradata's VMware appliance (Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova).
# Run on the docker host as a user in the kvm group:
#
#   makeBase.sh <path to .ova, or the disk-0.vmdk extracted from it> <base directory>
#
# The guest's root password is the appliance default, iumb123.
#
# 1. Convert the appliance disk to qcow2 and add the two empty 200G disks the database expects.
# 2. First boot: run tdvm-init (the appliance's first-boot configuration, which builds the database).
# 3. Second boot: repair what the first shutdown broke (see below), start and cleanly stop Teradata.
# 4. Verification boot, through throwaway overlays as the container does: Teradata must start by itself.
#
# The guest clock is set to 2023-06-01 and the guest has no outbound network while tdvm-init runs:
# tdvm-init drives Teradata's PUT tool over TLS, and PUT's certificate expired on 2023-09-05, so at the
# real date the PUT client rejects it and tdvm-init hangs forever. PUT is only used for configuration,
# so later boots (the container) run at the real date.
#
set -e
OVA=$(realpath "$1")
BASE=$2
ROOT_PASSWORD=iumb123
[ -f "$OVA" ] && [ -n "$BASE" ] || { echo "usage: $0 <ova> <base directory>"; exit 1; }
[ -e "$BASE/disk-0.qcow2" ] && { echo "$BASE/disk-0.qcow2 exists, remove the base first"; exit 1; }
mkdir -p "$BASE/build"
cd "$BASE"
GUEST="ssh -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes -i $PWD/tdvm_key -p 2222 root@127.0.0.1"

# boot_guest <name> <disk file prefix> <extra netdev options>
boot_guest() {
  rm -f build/monitor.sock
  qemu-system-x86_64 -name $1 -enable-kvm -machine pc -cpu host -smp 4 -m 8192 \
    -device ahci,id=ahci \
    -drive file=$2-0.qcow2,if=none,id=d0,format=qcow2 -device ide-hd,drive=d0,bus=ahci.0 \
    -drive file=$2-1.qcow2,if=none,id=d1,format=qcow2 -device ide-hd,drive=d1,bus=ahci.1 \
    -drive file=$2-2.qcow2,if=none,id=d2,format=qcow2 -device ide-hd,drive=d2,bus=ahci.2 \
    -netdev user,id=n0$3,net=10.10.10.0/24,host=10.10.10.100,dhcpstart=10.10.10.101,hostfwd=tcp:127.0.0.1:2222-:22 \
    -device e1000,netdev=n0 \
    -display none -serial file:build/serial-$1.log \
    -monitor unix:build/monitor.sock,server,nowait \
    -pidfile build/qemu.pid -daemonize
}

wait_for_ssh() {
  for i in $(seq 1 60); do $GUEST true 2>/dev/null && return 0; sleep 5; done
  return 1
}

wait_for_logons() {
  for i in $(seq 1 80); do
    $GUEST pdestate -a 2>/dev/null | grep -q "Logons are enabled" && { $GUEST pdestate -a; return 0; }
    sleep 15
  done
  $GUEST pdestate -a
  return 1
}

# Stop Teradata before powering off: a guest powered off while Teradata is (re)starting comes back with
# "PanicLoopDetected". The clock goes back to the real date first.
stop_and_power_off() {
  $GUEST /etc/init.d/tpa stop || true
  for i in $(seq 1 40); do
    $GUEST pdestate 2>/dev/null | grep -q "DOWN/" && break
    sleep 5
  done
  $GUEST "pdestate; date -s @$(date +%s); sync"
  local pid=$(cat build/qemu.pid)
  echo system_powerdown | nc -NU build/monitor.sock > /dev/null
  while [ -e /proc/$pid ]; do sleep 5; done
}

trap 'kill $(cat build/qemu.pid) 2>/dev/null' ERR

echo "Converting appliance disk"
case "$OVA" in
  *.vmdk) qemu-img convert -O qcow2 "$OVA" disk-0.qcow2 ;;
  *)      tar -xf "$OVA" -C build disk-0.vmdk
          qemu-img convert -O qcow2 build/disk-0.vmdk disk-0.qcow2
          rm build/disk-0.vmdk ;;
esac
qemu-img create -q -f qcow2 disk-1.qcow2 200G
qemu-img create -q -f qcow2 disk-2.qcow2 200G
[ -f tdvm_key ] || ssh-keygen -q -t ed25519 -N "" -C tdvm -f tdvm_key

echo "First boot (no outbound network)"
boot_guest TDATA-17-BUILD disk ",restrict=on"

echo "Installing ssh key in guest"
printf '#!/bin/sh\necho %s\n' $ROOT_PASSWORD > build/askpass.sh
chmod 700 build/askpass.sh
for i in $(seq 1 60); do
  SSH_ASKPASS=$PWD/build/askpass.sh SSH_ASKPASS_REQUIRE=force DISPLAY=none setsid \
    ssh -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o PubkeyAuthentication=no -o NumberOfPasswordPrompts=1 -p 2222 root@127.0.0.1 \
    'mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys' < tdvm_key.pub && break
  sleep 5
done
rm build/askpass.sh
# Set from inside the guest: under KVM the guest takes its wall clock from kvmclock, which ignores -rtc base.
$GUEST date -s 2023-06-01T12:00:00

# tdvm-init asks for a time zone, an NTP server and the root password, in that order (the password prompt
# shows first in its log only because it goes to unbuffered stderr). The PUT client it starts gives up
# ("TD Put Process has ended prematurely ... invalid root password") when its stdin reaches end of file,
# and tdvm-init then skips its remaining steps, so stdin stays open (a FIFO held by fd 3) until it exits.
run_tdvm_init() {
  mkfifo build/stdin
  $GUEST /usr/local/bin/tdvm-init < build/stdin > build/tdvm-init.log 2>&1 &
  local init=$!
  exec 3> build/stdin
  printf 'America/Los_Angeles\ntime.nist.gov\n%s\n' $ROOT_PASSWORD >&3
  wait $init || true
  exec 3>&-
  rm build/stdin
  tail -20 build/tdvm-init.log
  grep -q "tdvm-init has completed succesfully" build/tdvm-init.log
}

# PUT's client also gives up if PUT's services are not settled yet. They are still starting shortly after
# boot, and restart after the clock change. So wait until the guest has been up 5 minutes and PUT's
# port (9000) is listening, then a minute more.
echo "Waiting for PUT services"
for i in $(seq 1 60); do
  $GUEST 'test $(cut -d. -f1 /proc/uptime) -ge 300 && netstat -tln | grep -q ":9000 "' && break
  sleep 10
done
sleep 60

echo "Running tdvm-init"
if ! run_tdvm_init; then
  echo "Running tdvm-init again"
  sleep 60
  run_tdvm_init
fi

echo "Waiting for logons to be enabled"
wait_for_logons
# setDbsFlags.sh, tdvm-init's last step, forces a TPA restart. Let it finish before stopping Teradata.
sleep 120
wait_for_logons

# The first shutdown after tdvm-init deletes the PDE's /usr/pde/bin/pdemain (every build so far, at
# either clock, after a clean TPA stop; later shutdowns don't). Without it Teradata never starts
# ("startpde(): can't find pdemain"). Keep a copy to put back on the second boot.
$GUEST cp -p /usr/pde/bin/pdemain /root/pdemain.17.10.02.01
echo "Stopping Teradata and powering off guest"
stop_and_power_off

echo "Second boot"
boot_guest TDATA-17-BUILD disk ""
wait_for_ssh
$GUEST 'rpm -V ppde | grep missing || true'
$GUEST 'test -f /usr/pde/bin/pdemain || { cp -p /root/pdemain.17.10.02.01 /usr/pde/bin/pdemain && echo "pdemain restored"; }'
$GUEST '! rpm -V ppde | grep missing'
$GUEST 'rm -f /var/opt/teradata/tdtemp/PanicLoopDetected; pdestate -a | grep -q "Logons are enabled" || { systemctl restart recondsrv; sleep 5; /etc/init.d/tpa start; }'
wait_for_logons
stop_and_power_off
chmod 444 disk-*.qcow2

# Boot the finished base the way the container does (through overlays, at the real date) and check
# Teradata starts by itself and stops cleanly.
echo "Verification boot"
for d in 0 1 2; do
  qemu-img create -q -f qcow2 -F qcow2 -b $PWD/disk-$d.qcow2 build/verify-$d.qcow2
done
boot_guest TDATA-17-VERIFY build/verify ""
sleep 60
wait_for_logons
stop_and_power_off
rm build/verify-*.qcow2
echo "Base built in $PWD"
ls -ls disk-*.qcow2
