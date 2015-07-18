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