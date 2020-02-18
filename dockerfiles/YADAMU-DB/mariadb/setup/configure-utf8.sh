cd /var/lib/mysql
mkdir -p /var/lib/mysql/conf.d
mv setup/utf-8.cnf /var/lib/mysql/conf.d
cd /etc/mysql/conf.d
ln -s /var/lib/mysql/conf.d/utf-8.cnf