#!/bin/bash
#
# Container entrypoint: boot the Teradata appliance from the read-only base disks in /vm/base,
# writing to qcow2 overlays in /vm/run. Teradata (1025) is forwarded to the container's port 1025,
# the guest's sshd to 127.0.0.1:2222 inside the container (see "health" below).
#
#   tdvm.sh          boot and wait (the container's CMD). SIGTERM powers the guest off cleanly.
#   tdvm.sh health   exit 0 once the database accepts logons (the compose healthcheck)
#   tdvm.sh guest    run a command in the guest as root, e.g. "tdvm.sh guest pdestate -a"
#
BASE=/vm/base
RUN=/vm/run
MEM=${TDVM_MEM:-8192}
CPUS=${TDVM_CPUS:-4}
GUEST="ssh -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes -i $BASE/tdvm_key -p 2222 root@127.0.0.1"

case "$1" in
  health)
    $GUEST pdestate -a 2>/dev/null | grep -q "Logons are enabled"
    exit $?
    ;;
  guest)
    shift
    exec $GUEST "$@"
    ;;
esac

set -e
mkdir -p $RUN
for d in 0 1 2; do
  [ -f $RUN/disk-$d.qcow2 ] || qemu-img create -q -f qcow2 -F qcow2 -b $BASE/disk-$d.qcow2 $RUN/disk-$d.qcow2
done

# Same guest addressing as the VirtualBox NAT network the appliance was first set up on: 10.10.10.101
qemu-system-x86_64 -name TDATA-17 -enable-kvm -machine pc -cpu host -smp $CPUS -m $MEM \
  -device ahci,id=ahci \
  -drive file=$RUN/disk-0.qcow2,if=none,id=d0,format=qcow2,cache=writeback -device ide-hd,drive=d0,bus=ahci.0 \
  -drive file=$RUN/disk-1.qcow2,if=none,id=d1,format=qcow2,cache=writeback -device ide-hd,drive=d1,bus=ahci.1 \
  -drive file=$RUN/disk-2.qcow2,if=none,id=d2,format=qcow2,cache=writeback -device ide-hd,drive=d2,bus=ahci.2 \
  -netdev user,id=n0,net=10.10.10.0/24,host=10.10.10.100,dhcpstart=10.10.10.101,hostfwd=tcp:127.0.0.1:2222-:22,hostfwd=tcp::1025-:1025 \
  -device e1000,netdev=n0 \
  -display none -serial stdio \
  -monitor unix:$RUN/monitor.sock,server,nowait < /dev/null &
QEMU=$!
set +e

powerdown() {
  # Stop Teradata first: a guest powered off while Teradata is starting comes back with "PanicLoopDetected"
  echo "Stopping Teradata"
  timeout 120 $GUEST /etc/init.d/tpa stop || true
  echo "Powering down Teradata guest"
  echo system_powerdown | nc -NU $RUN/monitor.sock > /dev/null || true
  wait $QEMU
  exit 0
}
trap powerdown TERM INT
wait $QEMU
