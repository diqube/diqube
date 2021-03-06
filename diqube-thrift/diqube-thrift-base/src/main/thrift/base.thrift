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

namespace java org.diqube.thrift.base.thrift

// ======= NODE =======

union RNodeAddress {
  1: optional RNodeDefaultAddress defaultAddr,
  2: optional RNodeHttpAddress httpAddr
}

struct RNodeHttpAddress {
  1: string url
}

struct RNodeDefaultAddress {
  1: string host,
  2: i16 port
}

enum RNodeAccessType {
  DEFAULT,
  HTTP
}

struct RUUID {
  1: i64 lower,
  2: i64 upper
}

// ======= VALUE =======

union RValue {
  1: optional string strValue,
  2: optional i64 longValue,
  3: optional double doubleValue
}

// ======= AUTH =======

struct TicketClaim {
    1: RUUID ticketId,
    2: string username,
    3: bool isSuperUser,
    4: i64 validUntil
}

struct Ticket {
    // IMPORTANT: TicketUtil depends on this ordering, as it records the bytes of a serialized Ticket used to describe
    // the TicketClaim! Those bytes are then used to calculate the signature.
    1: TicketClaim claim,
    2: binary signature
}


exception AuthenticationException {
    1: string message 
}

exception AuthorizationException {
    1: string message 
}

// ======= METADATA =======

enum FieldType {
  // keep in sync with ColumnType
  STRING, LONG, DOUBLE,
  // additional FieldType for which there is no ColumnType. This is for field which do not contain data themselves, but
  // only different fields. See Doc for FieldMetadata.
  CONTAINER
}

// Metadata of a field of a table.
//
// Note that for a repeated field, only one FieldMetadata will be available, although there are multiple
// ColumnShards (i.e. there is a ColumnShard for each index of the repeated field). The field name is stripped
// of all repetition indices the column name might have (like "[0]", "[5]" or "[length]").
//
// Note that there are also fields for which no direct ColumnShard is available, as for each column "a.b", "a"
// is a field, too (FieldType#CONTAINER), although "a" does not contain any values directly and there is no
// ColumnShard therefore.
struct FieldMetadata {
  1: string fieldName,
  2: FieldType fieldType,
  3: bool repeated
}

// Additional information about a table like the fields it contains and what data types these fields have.
struct TableMetadata {
  1: string tableName,
  2: list<FieldMetadata> fields
}