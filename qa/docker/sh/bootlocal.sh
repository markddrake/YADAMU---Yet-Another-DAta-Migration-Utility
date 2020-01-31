echo "LABEL=ORA1903-01 /mnt/sda1/var/lib/docker/volumes/ORA1903-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=ORA1803-01 /mnt/sda1/var/lib/docker/volumes/ORA1803-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=ORA1220-01 /mnt/sda1/var/lib/docker/volumes/ORA1220-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=ORA1210-01 /mnt/sda1/var/lib/docker/volumes/ORA1210-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=ORA1120-01 /mnt/sda1/var/lib/docker/volumes/ORA1120-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=MYSQL80-01 /mnt/sda1/var/lib/docker/volumes/MYSQL80-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=MSSQL17-01 /mnt/sda1/var/lib/docker/volumes/MSSQL17-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=MSSQL19-01 /mnt/sda1/var/lib/docker/volumes/MSSQL19-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=PGSQL12-01 /mnt/sda1/var/lib/docker/volumes/PGSQL12-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
echo "LABEL=MARIA10-01 /mnt/sda1/var/lib/docker/volumes/MARIA10-01 ext4 defaults 0 0" | sudo tee -a /etc/fstab
sudo mount -a
sudo mount