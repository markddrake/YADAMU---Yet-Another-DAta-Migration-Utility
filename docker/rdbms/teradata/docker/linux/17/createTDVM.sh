VBoxManage extpack install Oracle_VM_VirtualBox_Extension_Pack-6.1.32.vbox-extpack <<EOF
Y
EOF
#VBoxManage natnetwork stop --netname cfgnet
#VBoxManage dhcpserver remove --network=cfgnet
#VBoxManage natnetwork remove --netname cfgnet
VBoxManage natnetwork add --netname cfgnet --dhcp on --network  "10.10.10.0/24" --enable
VBoxManage natnetwork modify --netname cfgnet --port-forward-4 "ssh:tcp:[]:1022:[10.10.10.101]:22"
VBoxManage natnetwork modify --netname cfgnet --port-forward-4 "tdata:tcp:[]:1025:[10.10.10.101]:1025"
VBoxManage dhcpserver add --network=cfgnet --enable --server-ip=10.10.10.100 --lower-ip=10.10.10.101 --upper-ip=10.10.10.101 --netmask 255.255.255.0
VBoxManage natnetwork start --netname cfgnet
VBoxManage list --long natnets
VBoxManage list --long dhcpservers
#VBoxManage createvm  --name TDATA-17 --ostype OpenSUSE_64 --register --default
VBoxManage import Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova --vsys 0 --vmname TDATA-17  --basefolder /root/VBox/Appliances
VBoxManage modifyvm TDATA-17 --memory 8192 --vram 64
VBoxManage modifyvm TDATA-17 --nic1 none
VBoxManage modifyvm TDATA-17 --nic1 natnetwork --nat-network1 cfgnet
#VBoxManage modifyvm TDATA-17 --nic1 bridged --nictype1 82545EM --bridgeadapter1 eno1
VBoxManage storageattach TDATA-17 --storagectl SCSI --port 0 --medium none
VBoxManage storagectl TDATA-17 --name IDE --remove
VBoxManage storagectl TDATA-17 --name SCSI --remove
VBoxManage storagectl TDATA-17 --name SATA --add sata
VBoxManage createmedium disk --filename  "/root/VBox/Appliances/TDATA-17/disk-1.vmdk" --size 204800 --format VMDK
VBoxManage createmedium disk --filename  "/root/VBox/Appliances/TDATA-17/disk-2.vmdk" --size 204800 --format VMDK
VBoxManage storageattach TDATA-17 --storagectl SATA --port 0 --medium  "/root/VBox/Appliances/TDATA-17/disk-0.vmdk" --type hdd
VBoxManage storageattach TDATA-17 --storagectl SATA --port 1 --medium  "/root/VBox/Appliances/TDATA-17/disk-1.vmdk" --type hdd
VBoxManage storageattach TDATA-17 --storagectl SATA --port 2 --medium  "/root/VBox/Appliances/TDATA-17/disk-2.vmdk" --type hdd
VBoxManage startvm TDATA-17 --type=headless
tail -f ./.config/VirtualBox/VBoxSVC.log &
tail -f ./VBox/Appliances/TDATA-17/Logs/VBox.log &
VBoxManage showvminfo TDATA-17
sshpass -p iubm123 ssh -p 1022 -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" root@127.0.0.1 <<EOF
tdvm-init
America/Los_Angeles
time.nist.gov
iumb123
EOF
echo 'Created Teradata instance'
#VBoxManage controlvm TDATA-17 acpipowerbutton
sleep 365d
