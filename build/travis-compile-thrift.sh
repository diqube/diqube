#!/usr/bin/env sh
#
# diqube: Distributed Query Base.
#
# Copyright (C) 2015 Bastian Gloeckle
#
# This file is part of diqube.
#
# diqube is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


set -vx

if [ -d "$HOME/diqube-thrift-092" ]; then
  rm -rf "$HOME/diqube-thrift-092";
fi

SRC_DIR="`pwd`"
cd "$HOME"
git clone https://github.com/apache/thrift.git diqube-thrift-092
cd diqube-thrift-092
git checkout 0.9.2

sh contrib/installDependencies.sh || exit 1
sh contrib/installCXXDependencies.sh || exit 1
./bootstrap.sh || exit 1
./configure --enable-static --with-java --without-qt4 --without-c_glib --without-csharp  --without-erlang \
  --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension \
  --without-ruby --without-haskell --without-go --without-d  || exit 1
mkdir bin && cd bin && cmake ../compiler/cpp/ && make -j4 || exit 1
cd "$SRC_DIR"
