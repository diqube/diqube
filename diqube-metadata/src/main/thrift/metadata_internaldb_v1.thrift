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

namespace java org.diqube.metadata.thrift.v1

include "${diqube.thrift.dependencies}/base.thrift"

// Keep in sync with TableMetadata and FieldMetadata java classes!

enum SFieldType {
  STRING,
  LONG,
  DOUBLE,
  CONTAINER
}

struct SFieldMetadata {
  1: string fieldName,
  2: SFieldType fieldType,
  3: bool repeated
}

struct STableMetadata {
  1: string tableName,
  2: list<SFieldMetadata> fields
}

struct SMetadataEntry {
  1: i64 versionNumber,
  2: optional STableMetadata metadata // can be null in case the tabel was up for recomputation currently.
}
