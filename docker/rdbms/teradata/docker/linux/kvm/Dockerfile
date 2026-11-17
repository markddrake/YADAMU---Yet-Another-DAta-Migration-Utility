#
# Teradata 17.10 (the VMware appliance) run under qemu/KVM inside a container.
#
# The image holds no Teradata bits. The appliance disks are built once on the host by makeBase.sh
# and bind-mounted read-only at /vm/base. Each container boots them through its own qcow2 overlays
# in /vm/run, so "docker compose down" followed by "up" is a fresh database, and "stop" / "start"
# keeps the data.
#
# Needs /dev/kvm on the host. No VirtualBox, no host kernel module, no --privileged.
#
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get -y install --no-install-recommends qemu-system-x86 qemu-utils netcat-openbsd openssh-client \
 && rm -rf /var/lib/apt/lists/*
COPY tdvm.sh /usr/local/bin/tdvm.sh
RUN chmod +x /usr/local/bin/tdvm.sh
EXPOSE 1025
CMD ["/usr/local/bin/tdvm.sh"]
