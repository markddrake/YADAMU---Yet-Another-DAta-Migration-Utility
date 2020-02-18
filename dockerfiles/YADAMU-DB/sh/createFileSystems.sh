#/bin/bash
sfdisk /dev/sdb << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdb1
e2label /dev/sdb1 ORA1903-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1903-01-DATA
sfdisk /dev/sdc << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdc1
e2label /dev/sdc1 ORA1803-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1803-01-DATA
sfdisk /dev/sdd << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdd1
e2label /dev/sdd1 ORA1220-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1220-01-DATA
sfdisk /dev/sde << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sde1
e2label /dev/sde1 ORA1210-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1210-01-DATA
sfdisk /dev/sdf << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdf1
e2label /dev/sdf1 ORA1120-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1120-01-DATA
sfdisk /dev/sdg << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdg1
e2label /dev/sdg1 MYSQL80-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/MYSQL80-01-DATA
sfdisk /dev/sdh << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdh1
e2label /dev/sdh1 MSSQL17-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL17-01-DATA
sfdisk /dev/sdi << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdi1
e2label /dev/sdi1 MSSQL19-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL19-01-DATA
sfdisk /dev/sdj << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdj1
e2label /dev/sdj1 PGSQL12-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/PGSQL12-01-DATA
sfdisk /dev/sdk << EOF
, 65536M
EOF
mkfs --type ext4 /dev/sdk1
e2label /dev/sdk1 MARIA10-01-DATA
mkdir /mnt/sda1/var/lib/docker/volumes/MARIA10-01-DATA
sfdisk /dev/sdl << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdl1
e2label /dev/sdl1 ORA1903-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1903-01-DIAG
sfdisk /dev/sdm << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdm1
e2label /dev/sdm1 ORA1803-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1803-01-DIAG
sfdisk /dev/sdn << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdn1
e2label /dev/sdn1 ORA1220-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1220-01-DIAG
sfdisk /dev/sdo << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdo1
e2label /dev/sdo1 ORA1210-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1210-01-DIAG
sfdisk /dev/sdp << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdp1
e2label /dev/sdp1 ORA1120-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/ORA1120-01-DIAG
sfdisk /dev/sdq << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdq1
e2label /dev/sdq1 MYSQL80-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/MYSQL80-01-DIAG
sfdisk /dev/sdr << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdr1
e2label /dev/sdr1 MSSQL17-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL17-01-DIAG
sfdisk /dev/sds << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sds1
e2label /dev/sds1 MSSQL19-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/MSSQL19-01-DIAG
sfdisk /dev/sdt << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdt1
e2label /dev/sdt1 PGSQL12-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/PGSQL12-01-DIAG
sfdisk /dev/sdu << EOF
, 2048M
EOF
mkfs --type ext4 /dev/sdu1
e2label /dev/sdu1 MARIA10-01-DIAG
mkdir /mnt/sda1/var/lib/docker/volumes/MARIA10-01-DIAG
