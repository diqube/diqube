#!/usr/bin/env sh

set -vx

if [ -d "$HOME/diqube-thrift-092" ]; then
  rm -rf "$HOME/diqube-thrift-092";
fi

SRC_DIR="`pwd`"
cd "$HOME"
git clone https://github.com/apache/thrift.git diqube-thrift-092
cd diqube-thrift-092
git checkout 0.9.2

contrib/installDependencies.sh || exit 1
contrib/installCXXDependencies.sh || exit 1
./bootstrap.sh || exit 1
./configure --enable-static --with-java --without-qt4 --without-c_glib --without-csharp  --without-erlang \
  --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension \
  --without-ruby --without-haskell --without-go --without-d  || exit 1
mkdir bin && cd bin && cmake ../compiler/cpp/ && make -j4 || exit 1
cd "$SRC_DIR"
