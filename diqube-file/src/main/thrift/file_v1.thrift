//
// diqube: Distributed Query Base.
//
// Copyright (C) 2015 Bastian Gloeckle
//
// This file is part of diqube.
//
// diqube is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

namespace java org.diqube.file.v1

struct SDiqubeFileHeader {
    1: string magic,
    2: i32 fileVersion,
    3: i32 contentVersion
}

struct SDiqubeFileFooter {
    1: i64 numberOfRows,
    2: i32 numberOfTableShards,
    3: string comment
}

struct SDiqubeFileFooterInfo {
    1: i32 footerLengthBytes
}