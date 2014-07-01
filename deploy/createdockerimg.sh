#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
ROOTDIR=$CURDIR/..
TMPDIR=$ROOTDIR/installtmp
echo "clean up"
rm -rf $TMPDIR
sudo docker rmi -f balihoo/fulfillment
echo "get latest java image from docker..."
sudo docker pull dockerfile/java

mkdir -p $TMPDIR

echo "create fat jar..."
pushd .
cd $ROOTDIR
sbt assembly
popd

JARNAME=$CURDIR/../target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar
if [ -e $JARNAME ]
then
  cp -r $ROOTDIR/config $TMPDIR
  cp $JARNAME $TMPDIR
  cp $ROOTDIR/launch_fulfillment $TMPDIR
  cp $CURDIR/dockerinstall.script $TMPDIR/install.sh
  chmod a+x $TMPDIR/install.sh
  chmod a+x $TMPDIR/launch_fulfillment
  CONTAINER=$(sudo docker run -d -v $TMPDIR:/share dockerfile/java /share/install.sh)
  echo saving container $CONTAINER
  sudo docker commit $CONTAINER balihoo/fulfillment
  echo "saving image..."
  sudo docker save balihoo/fulfillment | gzip > ffdockerimg.tar.gz
else
  echo "fat jar not found: $JARFILE"
fi
