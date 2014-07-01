#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
TMPDIR=$CURDIR/../insalltmp
rm -rf $TMPDIR
mkdir -p $TMPDIR
cp $CURDIR/../target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar $TMPDIR
cp $CURDIR/dockerinstall.script $TMPDIR/install.sh
chmod a+x $TMPDIR/install.sh
DOCKERCMD=sudo docker run -d -v $TMPDIR:/share dockerfile/java /share/install.sh
CONTAINER=$("$DOCKERCMD")
sudo docker commit $CONTAINER balihoo/fulfillment


