#!/usr/bin/env sh

SRC_DIR="`pwd`"
cd $HOME
if [ ! -f .m2/settings.xml ]; then
  if [ ! -d .m2 ]; then
    mkdir .m2
  fi
  cd .m2
  sed "s?REPLACE?\<thrift.executable\>$HOME/thrift-092/compiler/cpp/thrift\</thrift.executable\>?" \
    "$SRC_DIR/build/travis-settings.xml" > settings.xml 
fi
cat "$HOME/.m2/settings.xml"
cd "$SRC_DIR"