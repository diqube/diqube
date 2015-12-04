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

namespace java org.diqube.im.thrift.v1

struct SIdentitiesHeader {
    1: i32 version
}

struct SPassword {
    1: binary hash,
    2: binary salt
}

struct SPermission {
    1: string permissionName,
    2: optional list<string> objects
}

struct SUser {
    1: string username,
    2: string email,
    3: SPassword password,
    4: optional list<SPermission> permissions
}

struct SIdentities {
    1: list<SUser> users
}
