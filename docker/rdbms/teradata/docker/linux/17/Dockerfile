FROM ubuntu:20.04
RUN apt-get update \
 && apt-get -y install wget gnupg2 software-properties-common net-tools iputils-ping ssh sshpass 
# && wget -q https://www.virtualbox.org/download/oracle_vbox_2016.asc -O- | apt-key add - \
# && wget -q https://www.virtualbox.org/download/oracle_vbox.asc -O- | apt-key add - \
# && add-apt-repository "deb https://download.virtualbox.org/virtualbox/debian focal contrib"
#RUN apt-get -y install virtualbox-6.1
WORKDIR /root
RUN wget https://download.virtualbox.org/virtualbox/6.1.34/virtualbox-6.1_6.1.34-150636~Ubuntu~eoan_amd64.deb -O virtualbox-6.1_6.1.34-150636~Ubuntu~eoan_amd64.deb \
 && wget https://download.virtualbox.org/virtualbox/6.1.34/Oracle_VM_VirtualBox_Extension_Pack-6.1.34.vbox-extpack -O Oracle_VM_VirtualBox_Extension_Pack-6.1.34.vbox-extpack \
 && apt -y install ./virtualbox-6.1_6.1.34-150636~Ubuntu~eoan_amd64.deb 
COPY Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova .
# COPY Oracle_VM_VirtualBox_Extension_Pack-6.1.34.vbox-extpack .
COPY createTDVM.sh .
copy exec_tdvm_init.sh .
#CMD virtualbox
#CMD sleep 365d
CMD /root/createTDVM.sh
#
# Usage: 
# docker run --name TDATA17-01 -v TDATA17-01-APPLIANCE:/root/VBox --device /dev/vboxdrv:/dev/vboxdrv --network host teradata/16
#