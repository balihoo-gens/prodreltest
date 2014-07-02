#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
ROOTDIR=$CURDIR/..
TMPDIR=$ROOTDIR/installtmp
#DOCKER_BASE=dockerfile/java
#DOCKER_BASE=maxexcloo/java
DOCKER_BASE=ubuntu
echo "clean up"
rm -rf $TMPDIR
docker rmi -f balihoo/fulfillment
#echo "get latest java image from docker..."
#sudo docker pull $DOCKER_BASE

mkdir -p $TMPDIR

echo "create fat jar..."
pushd .
cd $ROOTDIR
#sbt assembly
popd

JARNAME=$CURDIR/../target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar
if [ -e $JARNAME ]
then
  cp -r $ROOTDIR/config $TMPDIR
  cp $JARNAME $TMPDIR/fulfillment.jar
  cp $ROOTDIR/launch_fulfillment $TMPDIR
  cp $CURDIR/dockerinstall.script $TMPDIR/install.sh
  cp $CURDIR/srcdockerfile $TMPDIR/Dockerfile
  cp $CURDIR/supervisord.conf $TMPDIR
  chmod a+x $TMPDIR/install.sh
  chmod a+x $TMPDIR/launch_fulfillment
#  CONTAINER=$(sudo docker run -d -v $TMPDIR:/share $DOCKER_BASE /share/install.sh)
#  echo saving container $CONTAINER
#  sudo docker commit $CONTAINER balihoo/fulfillment
  docker build -t balihoo/fulfillment $TMPDIR
  echo "saving image..."
  docker save balihoo/fulfillment | gzip > ffdockerimg_precise.tar.gz
else
  echo "fat jar not found: $JARFILE"
fi
