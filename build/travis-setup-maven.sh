#!/usr/bin/env sh

SRC_DIR="`pwd`"
cd $HOME
if [ ! -d .m2 ]; then
  mkdir .m2
fi

# replace default settings.xml.
cd .m2
sed "s?REPLACE?\<thrift.executable\>$HOME/diqube-thrift-092/bin/thrift\</thrift.executable\>?" \
    "$SRC_DIR/build/travis-settings.xml" > settings.xml 
cat "$HOME/.m2/settings.xml"
cd "$SRC_DIR"