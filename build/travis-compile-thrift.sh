#!/usr/bin/env sh

set -vx

if [ ! -d "$HOME/diqube-thrift-092" ]; then
  SRC_DIR="`pwd`"
  cd "$HOME"
  git clone https://git-wip-us.apache.org/repos/asf/thrift.git thrift
  mv thrift diqube-thrift-092
  cd diqube-thrift-092
  git checkout 0.9.2
  ./bootstrap.sh
  ./configure
  make
  cd "$SRC_DIR"
else
  echo 'Using cached directory.';
fi

