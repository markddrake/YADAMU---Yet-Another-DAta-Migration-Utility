#/bin/sh
sudo cat /var/lib/boot2docker/fstabEntries >> /etc/fstab
sudo mount -a
sudo mount
