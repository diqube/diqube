/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.data.dictionary;

import java.util.NavigableMap;
import java.util.Set;

/**
 * An arbitrary dictionary, mapping values to long IDs.
 * 
 * The first ID to be used is always 0L.
 * 
 * See sub-interfaces.
 *
 * @author Bastian Gloeckle
 */
public interface Dictionary<T> {
  /**
   * The maximum ID of the values in this dictionary or <code>null</code> if there are no entries at all.
   */
  public Long getMaxId();

  /**
   * Return the value of a specific ID.
   * 
   * <p>
   * As dictionaries are usually compressed, this call will decompress a the corresponding value.
   * 
   * @param id
   *          The ID of the entry to return the value of.
   * @return The value.
   * @throws IllegalArgumentException
   *           If the value could not be found.
   */
  public T decompressValue(long id) throws IllegalArgumentException;

  /**
   * Return the values of specific IDs.
   * 
   * <p>
   * As dictionaries are usually compressed, this call will decompress a the corresponding value.
   * 
   * @param id
   *          The IDs of the entry to return the value of.
   * @return The values, preserving the same order as the input array.
   * @throws IllegalArgumentException
   *           If any value could not be found.
   */
  public T[] decompressValues(Long[] id) throws IllegalArgumentException;

  /**
   * Return the ID of a specific (decompressed) value.
   * 
   * @param value
   *          The decompressed value to search the ID of.
   * @return The id.
   * @throws IllegalArgumentException
   *           If the value could not be found.
   */
  public long findIdOfValue(T value) throws IllegalArgumentException;

  /**
   * Return the IDs of a specific set of values.
   * 
   * @param sortedValues
   *          The decompressed values to search the ID of.
   * @return Array containing the IDs of the values (array indices of input & output match). For values that were not
   *         contained, the resulting array will have a value of -1L.
   */
  public Long[] findIdsOfValues(T[] sortedValues);

  /**
   * Return the equal or next greater ID of a specific value.
   * 
   * @param value
   *          The decompressed value to search the ID of.
   * @return The id. If >= 0 then the returned ID is the ID of the equal value. If < 0 then the result is
   *         <code>-(id + 1)</code>, where id is the ID of the next greater value. <code>null</code> in case this dict
   *         neither contains the value nor any greater element.
   */
  public Long findGtEqIdOfValue(T value);

  /**
   * Return the equal or next smaller ID of a specific value.
   * 
   * @param value
   *          The decompressed value to search the ID of.
   * @return The id. If >= 0 then the returned ID is the ID of the equal value. If < 0 then the result is
   *         <code>-(id + 1)</code>, where id is the ID of the next smaller value. <code>null</code> in case this dict
   *         neither contains the value nor any smaller element.
   */
  public Long findLtEqIdOfValue(T value);

  /**
   * Checks if the dictionary contains any of the provided values.
   * 
   * @param sortedValues
   *          The values, in a sorted array.
   * @return true in case at least one of the values is contained in this dictionary.
   */
  public boolean containsAnyValue(T[] sortedValues);

  /**
   * Checks if the dictionary contains any that is greater or equal to the given value.
   * 
   * @return true in case at least one of the values is contained in this dictionary.
   */
  public boolean containsAnyValueGtEq(T value);

  /**
   * Checks if the dictionary contains any that is greater than the given value.
   * 
   * @return true in case at least one of the values is contained in this dictionary.
   */
  public boolean containsAnyValueGt(T value);

  /**
   * Checks if the dictionary contains any that is 'less than or equal' to the given value.
   * 
   * @return true in case at least one of the values is contained in this dictionary.
   */
  public boolean containsAnyValueLtEq(T value);

  /**
   * Checks if the dictionary contains any that is less than the given value.
   * 
   * @return true in case at least one of the values is contained in this dictionary.
   */
  public boolean containsAnyValueLt(T value);

  /**
   * Returns the IDs of the entries whose value is Greater or Equal to the given one.
   * 
   * @return is sorted.
   */
  public Set<Long> findIdsOfValuesGtEq(T value);

  /**
   * Returns the IDs of the entries whose value is 'greater' than the given one.
   * 
   * @return is sorted.
   */
  public Set<Long> findIdsOfValuesGt(T value);

  /**
   * Returns the IDs of the entries whose value is 'less than' the given one.
   * 
   * @return is sorted.
   */
  public Set<Long> findIdsOfValuesLt(T value);

  /**
   * Returns the IDs of the entries whose value is 'less than or equal' to the given one.
   * 
   * @return is sorted.
   */
  public Set<Long> findIdsOfValuesLtEq(T value);

  /**
   * Traverses both this and the given other dict, compares the values and returns IDs of both dicts where they have
   * equal values.
   * 
   * @return The value of <i>this</i> dict at the left ID is equal to the value of <i>otherDict</i> at the right ID.
   */
  public NavigableMap<Long, Long> findEqualIds(Dictionary<T> otherDict);

  /**
   * Traverses both this and the given other dict, compares the values and returns IDs of both dicts where they have
   * matching values.
   * 
   * @return Returns a (key, value) pair of column value IDs. The 'key' is an ID in this dict. The 'value' is an ID in
   *         otherDict. For each ID in this dict where the >= equation holds to any element in otherDict, there is a
   *         key.
   * 
   *         The value of the returned map is either positive or negative. If it is <b>positive<b>, it is the column
   *         value ID of the otherDict whose value is <b>equal</b> to the value of this dict at the ID which is denoted
   *         by the returned key.
   * 
   *         If the value of the returned map is <b>negative<b>, it is encoded as <code>value = -(otherId + 1)</code>,
   *         where otherId is the greatest ID in otherDict whose value is smaller than the value of this dict at the ID
   *         which is denoted by the key. This then means that the value of this dict at the given ID is <b>strongly
   *         greater</b> than all values in the otherDict whose value is <= otherId.
   */
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<T> otherDict);

  /**
   * Traverses both this and the given other dict, compares the values and returns IDs of both dicts where they have
   * matching values.
   * 
   * @return Returns a (key, value) pair of column value IDs. The 'key' is an ID in this dict. The 'value' is an ID in
   *         otherDict. For each ID in this dict where the <= equation holds to any element in otherDict, there is a
   *         key.
   * 
   *         The value of the returned map is either positive or negative. If it is <b>positive<b>, it is the column
   *         value ID of the otherDict whose value is <b>equal</b> to the value of this dict at the ID which is denoted
   *         by the returned key.
   * 
   *         If the value of the returned map is <b>negative<b>, it is encoded as <code>value = -(otherId + 1)</code>,
   *         where otherId is the smallest ID in otherDict whose value is greater than the value of this dict at the ID
   *         which is denoted by the key. This then means that the value of this dict at the given ID is <b>strongly
   *         smaller</b> than all values in the otherDict whose value is >= otherId.
   */
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<T> otherDict);

  /**
   * @return An approximate number of bytes taken up by this {@link Dictionary}. Note that this is only an
   *         approximation!
   */
  public long calculateApproximateSizeInBytes();
}
