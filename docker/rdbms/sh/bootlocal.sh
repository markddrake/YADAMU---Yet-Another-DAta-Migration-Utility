#/bin/sh
# Make sure /dev/sda get's mounted first
sleep 5
sudo cat /var/lib/boot2docker/fstabEntries >> /etc/fstab
sudo mount -a
sudo mount
