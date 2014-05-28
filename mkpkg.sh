#!/bin/bash

declare -a NVR
NVR=(`head -1 debian/changelog | \
	sed 's/\([^[:space:]]*\)[[:space:]]*(\([^-\.]*\)[-\.]\([^)]*\)).*/\1 \2 \3/'`)
NAME=${NVR[0]}
VERSION=${NVR[1]}
RELEASE=${NVR[2]}

WD=`mktemp -d /var/tmp/$NAME-XXXXXX`
SRCDIR="$WD/$NAME-$VERSION"
mkdir $SRCDIR

cp -r cmake CMakeLists.txt debian service src testing \
	evfibers include msgpack-proto-ragel tools README.md \
	$SRCDIR

chmod -R u+rw $SRCDIR
pushd $WD
rm $NAME*.dsc
dpkg-source -b $NAME-$VERSION
sudo pbuilder --build $NAME*.dsc
popd

mv $WD/*.dsc $WD/*.tar.gz ..
rm -rf $WD

