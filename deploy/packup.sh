#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
ROOTDIR=$CURDIR/..
TMPDIR=$ROOTDIR/installtmp

echo "clean up"
rm -rf $TMPDIR
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
  cp $JARNAME $TMPDIR/fulfillment.jar
  cp $ROOTDIR/launch_fulfillment $TMPDIR
  chmod a+x $TMPDIR/launch_fulfillment
  tar -zcf fulfillment-$(date +%s).tar.gz $TMPDIR
else
  echo "fat jar not found: $JARFILE"
fi
