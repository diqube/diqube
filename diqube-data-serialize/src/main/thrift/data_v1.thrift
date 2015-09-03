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

namespace java org.diqube.data.serialize.thrift.v1

struct STableShard {
    1: string tableName,
    2: list<SColumnShard> columnShards
} 

struct SColumnShard {
    1: string name,
    2: SColumnType type,
    3: SDictionary dictionary,
    4: list<SColumnPage> pages
}

struct SColumnPage {
    1: string name,
    2: i64 firstRowId,
    3: SLongDictionary pageDict,
    4: SLongCompressedArray values
}

enum SColumnType {
    STRING,
    LONG,
    DOUBLE
}

union SDictionary {
    1: optional SStringDictionary stringDict,
    2: optional SDoubleDictionary doubleDict,
    3: optional SLongDictionary longDict
}

// ========== String dict

union SStringDictionary {
    1: optional SStringDictionaryTrie trie,
    2: optional SStringDictionaryConstant constant
}

struct SStringDictionaryConstant {
    1: string value,
    2: i64 id
}

struct SStringDictionaryTrie {
    1: string firstValue,
    2: string lastValue,
    3: i64 lastId,
    4: SStringDictionaryTrieNode rootNode
}

union SStringDictionaryTrieNode {
    1: optional SStringDictionaryTrieParentNode parentNode,
    2: optional SStringDictionaryTrieTerminalNode terminalNode
}

struct SStringDictionaryTrieParentNode {
    1: map<string, SStringDictionaryTrieNode> childNodes,
    2: i64 minId,
    3: i64 maxId
}

struct SStringDictionaryTrieTerminalNode {
    1: i64 terminalId
}

// ========== Double dict

union SDoubleDictionary {
    1: optional SDoubleDictionaryFpc fpc
    2: optional SDoubleDictionaryConstant constant
}

struct SDoubleDictionaryConstant {
    1: double value,
    2: i64 id
}

struct SDoubleDictionaryFpc {
    1: double lowestValue,
    2: double highestValue,
    3: i64 highestId,
    4: list<SDoubleDictionaryFpcPage> pages 
}

struct SDoubleDictionaryFpcPage {
    1: i64 firstId,
    2: i32 size,
    3: SDoubleDictionaryFpcState startState,
    4: binary data
}

struct SDoubleDictionaryFpcState {
    1: list<i64> fcmHashTable,
    2: list<i64> dfcmHashTable,
    3: byte fcmHash,
    4: byte dfcmHash,
    5: i64 lastValue
}

// ========== Long dict

union SLongDictionary {
    1: optional SLongDictionaryArray arr,
    2: optional SLongDictionaryConstant constant,
    3: optional SLongDictionaryEmpty empty
}

struct SLongDictionaryEmpty {
}

struct SLongDictionaryConstant {
    1: i64 value,
    2: i64 id
}

struct SLongDictionaryArray {
    1: SLongCompressedArray arr
}

// ========== Long array

union SLongCompressedArray {
    1: optional SLongCompressedArrayBitEfficient bitEfficient,
    2: optional SLongCompressedArrayRLE rle,
    3: optional SLongCompressedArrayReference ref
}
 
struct SLongCompressedArrayBitEfficient {
    1: i32 size,
    2: i32 numberOfBitsPerValue,
    3: bool isSorted,
    4: bool isSameValue,
    5: bool containsSignBit,
    6: list<i64> compressedValues,
    7: list<i32> longMinValueLocations,
    8: i64 minValue,
    9: i64 absoluteMinValue,
    10: i64 maxValue
}

struct SLongCompressedArrayRLE {
    1: bool isSorted,
    2: i64 numberOfDifferentTuples,
    3: i64 maxValue,
    4: i64 minValue,
    5: i64 secondMinValue,
    6: i64 maxCount,
    7: i64 minCount,
    8: optional list<i64> compressedValues,
    9: optional list<i64> compressedCounts,
    10: optional SLongCompressedArray delegateCompressedValue,
    11: optional SLongCompressedArray delegateCompressedCounts,
    12: i32 size
}

struct SLongCompressedArrayReference {
    1: bool isSorted,
    2: bool isSameValue,
    3: i64 refPoint,
    4: i64 min,
    5: i64 secondMin,
    6: i64 max,
    7: optional list<i64> compressedValues,
    8: optional SLongCompressedArray delegateCompressedValues
}