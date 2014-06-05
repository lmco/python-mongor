#!/bin/bash

SPEC_FILE=python-pymongor.spec 

# Change to source dir
cd $(dirname $0)/../

SRC_DIR=`pwd`

echo 'Running setup.py bdist_rpm'
python setup.py bdist_rpm

echo 'Setting up build environment'
mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
echo '%_topdir %(echo $HOME)/rpmbuild' > ~/.rpmmacros

echo 'Copying source files to rpmbuild directory'
cp dist/*.tar.gz ~/rpmbuild/SOURCES/
cp rpm/rotate_mongodb.cron ~/rpmbuild/SOURCES/
cp rpm/$SPEC_FILE ~/rpmbuild/SPECS/

echo 'Making RPM'
cd ~/rpmbuild/SPECS/
rpmbuild -ba $SPEC_FILE

