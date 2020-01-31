sfdisk /dev/sdb << EOF
, 16G
EOF
sfdisk /dev/sdc << EOF
, 16G
EOF
sfdisk /dev/sdd << EOF
, 16G
EOF
sfdisk /dev/sde << EOF
, 16G
EOF
sfdisk /dev/sdf << EOF
, 16G
EOF
sfdisk /dev/sdg << EOF
, 16G
EOF
sfdisk /dev/sdh << EOF
, 16G
EOF
sfdisk /dev/sdi << EOF
, 16G
EOF
sfdisk /dev/sdj << EOF
, 16G
EOF
sfdisk /dev/sdk << EOF
, 16G
EOF


mkfs --type ext4 /dev/sdb1
mkfs --type ext4 /dev/sdc1
mkfs --type ext4 /dev/sdd1
mkfs --type ext4 /dev/sde1
mkfs --type ext4 /dev/sdf1
mkfs --type ext4 /dev/sdg1
mkfs --type ext4 /dev/sdh1
mkfs --type ext4 /dev/sdi1
mkfs --type ext4 /dev/sdj1
mkfs --type ext4 /dev/sdk1

e2label /dev/sdb1 ORA1903-01
e2label /dev/sdc1 ORA1803-01
e2label /dev/sdd1 ORA1220-01
e2label /dev/sde1 ORA1210-01
e2label /dev/sdf1 ORA1120-01
e2label /dev/sdg1 MYSQL80-01
e2label /dev/sdh1 MSSQL17-01
e2label /dev/sdi1 MSSQL19-01
e2label /dev/sdj1 PGSQL12-01
e2label /dev/sdk1 MARIA10-01
                 
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1903-01
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1803-01
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1220-01
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1210-01
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1120-01
mkdir /mnt/sda1/var/lib/docker/volumes/MYSQL80-01
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL17-01
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL19-01
mkdir /mnt/sda1/var/lib/docker/volumes/PGSQL12-01
mkdir /mnt/sda1/var/lib/docker/volumes/MARIA10-01