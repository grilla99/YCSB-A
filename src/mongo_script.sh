#!/bin/bash
#set debug mode
set -x

ins=$(cat <<- END
[mongodb-org-4.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/4.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-4.0.asc
END
)

echo "$ins" | sudo tee /etc/yum.repos.d/mongodb-org-4.0.repo

yes Y | sudo yum install -y mongodb-org

hugepages=$(cat <<- EOP
#!/bin/bash
### BEGIN INIT INFO
# Provides:          disable-transparent-hugepages
# Required-Start:    $local_fs
# Required-Stop:
# X-Start-Before:    mongod mongodb-mms-automation-agent
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Disable Linux transparent huge pages
# Description:       Disable Linux transparent huge pages, to improve
#                    database performance.
### END INIT INFO

case \$1 in
  start)
    if [ -d /sys/kernel/mm/transparent_hugepage ]; then
      thp_path=/sys/kernel/mm/transparent_hugepage
    elif [ -d /sys/kernel/mm/redhat_transparent_hugepage ]; then
      thp_path=/sys/kernel/mm/redhat_transparent_hugepage
    else
      return 0
    fi

    echo 'never' > \${thp_path}/enabled
    echo 'never' > \${thp_path}/defrag

    re='^[0-1]+\$'
    if [[ \$(cat \${thp_path}/khugepaged/defrag) =~ \$re ]]
    then
      # RHEL 7
      echo 0  > \${thp_path}/khugepaged/defrag
    else
      # RHEL 6
      echo 'no' > \${thp_path}/khugepaged/defrag
    fi

    unset re
    unset thp_path
    ;;
esac
EOP
)

echo "$hugepages" | sudo tee /etc/init.d/disable-transparent-hugepages

sudo chmod 755 /etc/init.d/disable-transparent-hugepages

sudo chkconfig --add disable-transparent-hugepages

softlim=$(cat <<- END
mongod soft nproc 64000
END
)
echo "$softlim" | sudo tee -a /etc/security/limits.conf

mongoconf=$(cat <<- END
# mongod.conf

# for documentation of all options, see:
#   http://docs.mongodb.org/manual/reference/configuration-options/

# where to write logging data.
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log

# how the process runs
processManagement:
  fork: true  # fork and run in background
  pidFilePath: /var/run/mongodb/mongod.pid  # location of pidfile
  timeZoneInfo: /usr/share/zoneinfo

# Where and how to store data.
storage:
  dbPath: /var/lib/mongo
  journal:
    enabled: true
  engine: wiredTiger
  wiredTiger:
    collectionConfig:
      blockCompressor: none
# network interfaces
net:
  port: 27017
  bindIp: mongo.domain.net  # Enter 0.0.0.0,:: to bind to all IPv4 and IPv6 addresses or, alternatively, use the net.bindIpAll setting.
  ssl:
    mode: requireSSL
    PEMKeyFile: /etc/ssl/mongo_ssl/mongodb.pem
    CAFile: /etc/ssl/mongo_ssl/CA.pem
    allowInvalidCertificates: true
    allowInvalidHostnames: true

#security:
#  authorization: enabled
#  keyFile: /var/lib/mongo/rsetkey
#operationProfiling:

#replication:
#  replSetName: TestRS-0

#sharding:

## Enterprise-Only Options

#auditLog:

#snmp:
END
)
echo "$mongoconf" | sudo tee /etc/mongod.conf

mkdir /home/ec2-user/mongo_ssl
cd /home/ec2-user//mongo_ssl
openssl req -out CA.pem -new -x509 -days 365 -keyout CAPrivKey.pem -subj "/C=IN/ST=karnataka/O=Organisation/CN=*.domain.net/emailAddress=user@domain.com" -nodes

echo "00" > serial_num.srl # two random digits number
openssl genrsa -out mongodb.key 2048
openssl req -key mongodb.key -new -out mongodb.req -subj  "/C=IN/ST=karnataka/O=Organisation/CN=server/CN=*.domain.net/emailAddress=user@domain.com" -nodes
openssl x509 -req -in mongodb.req -CA CA.pem -CAkey CAPrivKey.pem -CAserial serial_num.srl -out mongodb.crt -days 365
cat mongodb.key mongodb.crt > mongodb.pem
openssl verify -CAfile CA.pem mongodb.pem

openssl genrsa -out mclient.key 2048
openssl req -key mclient.key -new -out mclient.req -subj "/C=IN/ST=karnataka/O=Organisation/CN=client/emailAddress=user@domain.com" -nodes
openssl x509 -req -in mclient.req -CA CA.pem -CAkey CAPrivKey.pem -CAserial serial_num.srl -out mclient.crt -days 365
cat mclient.key mclient.crt > mclient.pem
openssl verify -CAfile CA.pem mclient.pem

#once everything is completed let's move it to /etc/ssl/ folder

sudo cp -R /home/ec2-user/mongo_ssl /etc/ssl
cd mongo_ssl
sudo chown mongod:mongod *

sudo openssl rand -base64 756 |sudo tee /var/lib/mongo/rsetkey
sudo chmod 400 /var/lib/mongo/rsetkey
sudo chown mongod:mongod /var/lib/mongo/rsetkey

sudo service mongod start

sudo chkconfig mongod on





