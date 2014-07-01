#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
TMPDIR=$CURDIR/../insalltmp
rm -rf $TMPDIR
sudo docker rmi balihoo/fulfillment
sudo docker pull dockerfile/java

mkdir -p $TMPDIR
cp $CURDIR/../target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar $TMPDIR
cp $CURDIR/dockerinstall.script $TMPDIR/install.sh
chmod a+x $TMPDIR/install.sh
CONTAINER=$(sudo docker run -d -v $TMPDIR:/share dockerfile/java /share/install.sh)
echo saviong container $CONTAINER
sudo docker commit $CONTAINER balihoo/fulfillment


