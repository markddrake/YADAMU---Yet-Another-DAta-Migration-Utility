#!/bin/bash
copyOracle() {
  sudo mkdir $VOLUME/scripts
  sudo cp /home/docker/scripts/fixOracleContainer* $VOLUME/scripts
  sudo cp -r /home/docker/sampleSchemas            $VOLUME
  sudo chown -R 54321:54321 $VOLUME/scripts
  sudo chown -R 54321:54321 $VOLUME/sampleSchemas  
}
cp scripts/bootlocal.sh  /var/lib/boot2docker
sudo bash scripts/addVolumes.sh
sudo bash /var/lib/boot2docker
export VOLUME_ROOT=/mnt/sda1/var/lib/docker/volumes/
export VOLUME=$VOLUME_ROOT/ORA1903-01/_data
copyOracle
export VOLUME=$VOLUME_ROOT/ORA1803-01/_data
copyOracle
export VOLUME=$VOLUME_ROOT/ORA1220-01/_data
copyOracle
export VOLUME=$VOLUME_ROOT/ORA1120-01/_data
copyOracle
