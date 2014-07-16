#! /bin/bash
CURDIR=$(cd $(dirname "$0"); pwd)
ROOTDIR=$CURDIR/..
TMPDIR=$ROOTDIR/installtmp
JARNAME=$CURDIR/../target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar

rm -rf $TMPDIR
mkdir -p $TMPDIR

echo "creating fat jar..."
pushd .
cd $ROOTDIR
sbt assembly
RES=$?
popd

if [ $RES -eq 0 ]
then
  if [ -e $JARNAME ]
  then
    echo "gathering configs"
    cp -r $ROOTDIR/config $TMPDIR
    echo "gathering fat jar"
    cp $JARNAME $TMPDIR/fulfillment.jar
    echo "gathering launch script"
    cp $ROOTDIR/launch_fulfillment $TMPDIR
    echo "setting launch script execute permissions"
    chmod a+x $TMPDIR/launch_fulfillment
    TARBALL=fulfillment-$(date +%s).tar.gz
    echo "creating tarball: ${TARBALL}"
    tar -zcf ${TARBALL} ${TMPDIR}
    echo "cleaning up"
    rm -rf $TMPDIR
    echo done
  else
    echo "fat jar not found: $JARFILE"
  fi
else
  echo "failed to create fat jar"
fi

exit ${RES}
