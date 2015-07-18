#!/usr/bin/env sh

set -vx

# Our thrift executable needs an updated libstdc++
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test || exit 1
sudo apt-get -y -qq update || exit 1
sudo apt-get -y -qq upgrade || exit 1
sudo apt-get -y -qq dist-upgrade || exit 1

SRC_DIR="`pwd`"
cd "$HOME"

# Download thrift
wget https://www.dropbox.com/s/7vzw0jdqurm6uay/thrift-092.tar.gz  || exit 1
tar -xf thrift-092.tar.gz || exit 1

# setup maven
if [ ! -d .m2 ]; then
  mkdir .m2 || exit 1
fi

# replace default settings.xml.
cd .m2
sed "s?REPLACE?\<thrift.executable\>$HOME/thrift\</thrift.executable\>?" \
    "$SRC_DIR/build/travis-settings.xml" > settings.xml 
cat "$HOME/.m2/settings.xml"
cd "$SRC_DIR"
