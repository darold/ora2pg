#!/bin/sh
#
# Script used to create the Debian package tree. This script must be
# executed in his directory
#
LPWD=`pwd`
DEST=packaging/debian/ora2pg
cd ../../
perl Makefile.PL \
  INSTALLDIRS=vendor \
  QUIET=1 \
  CONFDIR=/etc/ora2pg \
  DOCDIR=/usr/share/doc/ora2pg \
  DESTDIR=$DEST || exit 1

make && make install DESTDIR=$DEST

echo "Compressing man pages"
find $DEST/usr/share/man/ -type f -name "*.?" -exec gzip -9 {} \;
find $DEST/usr/share/man/ -type f -name "*.?pm" -exec gzip -9 {} \;

cd $LPWD

