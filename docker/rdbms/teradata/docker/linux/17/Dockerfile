FROM ubuntu:24.04
RUN apt-get update \
 && apt-get -y install wget gnupg2 software-properties-common net-tools iputils-ping ssh sshpass apt-utils  \
 && wget -q https://www.virtualbox.org/download/oracle_vbox_2016.asc -O- | apt-key add - \
 && wget -q https://www.virtualbox.org/download/oracle_vbox.asc -O- | apt-key add - \
 && add-apt-repository "deb https://download.virtualbox.org/virtualbox/debian noble contrib" \
 && apt-get -y install virtualbox \
 && apt-get -y install virtualbox-ext-pack <<EOF
Y
EOF
RUN apt-get --reinstall install virtualbox-dkms
WORKDIR /root
COPY Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova .
COPY createTDVM.sh .
copy exec_tdvm_init.sh .
#CMD virtualbox
#CMD sleep 365d
CMD /root/createTDVM.sh
#
# Usage: 
# docker run --name TDATA17-01 -v TDATA17-01-APPLIANCE:/root/VBox --device /dev/vboxdrv:/dev/vboxdrv --network host teradata/16
#