docker stop TDATA17-01
docker rm TDATA17-01
docker volume rm TDATA17-01-APPLIANCE
docker volume create TDATA17-01-APPLIANCE
docker build --tag teradata/17 --file Dockerfile .
docker run --name TDATA17-01 -d -v TDATA17-01-APPLIANCE:/root/VBox -v /tmp/.X11-unix:/tmp/.X11-unix    --device /dev/vboxdrv:/dev/vboxdrv --network YADAMU-NET -p 1025:1025 teradata/17
docker logs -f TDATA17-01