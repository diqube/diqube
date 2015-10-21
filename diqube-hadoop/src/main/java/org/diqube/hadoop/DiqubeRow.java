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
package org.diqube.hadoop;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

/**
 * Data prepared for a .diqube file representing one row in the resulting diqube table.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeRow implements Serializable {
  private static final long serialVersionUID = 1L;

  private DiqubeData data;

  /**
   * Return a {@link DiqubeData} object that can be filled with the data of one row.
   * 
   * The returned object contains the hierarchical data that will be written into one row of a diqube table.
   */
  public DiqubeData withData() {
    data = new DiqubeData(null);
    return data;
  }

  public DiqubeData getData() {
    return data;
  }

  /**
   * Hierarchical data that is to be stored in the .diqube file.
   * 
   * This class accepts the data in a fluent interface:
   * 
   * <pre>
   * DiqubeRow row = ...;
   * row.withData().withData("columnA", "value1")
   *               .withData("columnB", 25.1)
   *               .withData("columnC", 1L)
   *               .withNewDiqubeData("columnD")
   *                  .withData("d_a", "hello w")
   *                  .done()
   *               .addNewRepeatedDiqubeData("columnE")
   *                  .withData("e_a", 1L)
   *                  .done()
   *               .addNewRepeatedDiqubeData("columnE")
   *                  .withData("e_a", 2L)
   *                  .done()
   *               .done();
   * </pre>
   * 
   * This would end up in the following row:
   * 
   * <pre>
   * columnA | columnB | columnC | columnD.d_a | columnE[0].e_a | columnE[1].e_a | columnE[length]
   * --------|---------|---------|-------------|----------------|----------------|----------------
   * value1  | 25.1    | 1       | hello w     | 1              | 2              | 2
   * </pre>
   */
  public class DiqubeData implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> data = new HashMap<>();
    private Map<String, List<Object>> repeatedData = new HashMap<>();
    private DiqubeData parent;

    private DiqubeData(DiqubeData parent) {
      this.parent = parent;
    }

    public DiqubeData withData(String fieldName, Long data) {
      return withDataInternal(fieldName, data);
    }

    public DiqubeData withData(String fieldName, Double data) {
      return withDataInternal(fieldName, data);
    }

    public DiqubeData withData(String fieldName, String data) {
      return withDataInternal(fieldName, data);
    }

    public DiqubeData withData(String fieldName, Object data) throws IllegalArgumentException {
      if (!(data instanceof String) && !(data instanceof Long) && !(data instanceof Double)
          && !(data instanceof DiqubeData))
        throw new IllegalArgumentException(data.getClass().getSimpleName() + " not supported.");
      return withDataInternal(fieldName, data);
    }

    public DiqubeData withNewDiqubeData(String fieldName) {
      DiqubeData res = new DiqubeData(this);
      withDataInternal(fieldName, res);
      return res;
    }

    private DiqubeData withDataInternal(String fieldName, Object data) {
      this.data.put(fieldName, data);
      return this;
    }

    public DiqubeData addRepeatedData(String fieldName, String data) {
      return addRepeatedDataInternal(fieldName, data);
    }

    public DiqubeData addRepeatedData(String fieldName, Long data) {
      return addRepeatedDataInternal(fieldName, data);
    }

    public DiqubeData addRepeatedData(String fieldName, Double data) {
      return addRepeatedDataInternal(fieldName, data);
    }

    public DiqubeData addRepeatedData(String fieldName, Object data) throws IllegalArgumentException {
      if (!(data instanceof String) && !(data instanceof Long) && !(data instanceof Double)
          && !(data instanceof DiqubeData))
        throw new IllegalArgumentException(data.getClass().getSimpleName() + " not supported.");
      return addRepeatedDataInternal(fieldName, data);
    }

    public DiqubeData addNewRepeatedDiqubeData(String fieldName) {
      DiqubeData res = new DiqubeData(this);
      addRepeatedDataInternal(fieldName, res);
      return res;
    }

    private DiqubeData addRepeatedDataInternal(String fieldName, Object data) {
      if (!this.repeatedData.containsKey(fieldName))
        this.repeatedData.put(fieldName, new ArrayList<>());
      this.repeatedData.get(fieldName).add(data);
      return this;
    }

    public DiqubeData done() {
      return parent;
    }

    public boolean isEmpty() {
      return data.isEmpty() && repeatedData.isEmpty();
    }

    /**
     * Validates the data in this {@link DiqubeData} and in all its transitive children. Automatically called by
     * {@link DiqubeRecordWriter}.
     * 
     * @throws IllegalStateException
     *           If anything is wrong.
     */
    /* package */ void validate() throws IllegalStateException {
      if (isEmpty())
        // accept empty data object, DiqubeRecordWriter will handle that correctly.
        return;

      Set<String> multipleMappedKeys = Sets.intersection(data.keySet(), repeatedData.keySet());
      if (!multipleMappedKeys.isEmpty())
        throw new IllegalStateException(
            "The following fieldNames have been used for normal data and repeated data which is not allowed: "
                + multipleMappedKeys);

      for (Entry<String, List<Object>> repeatedEntry : repeatedData.entrySet()) {
        List<Object> values = repeatedEntry.getValue();
        String fieldName = repeatedEntry.getKey();

        List<Class<?>> valueClasses = values.stream().map(d -> d.getClass()).distinct().collect(Collectors.toList());

        if (valueClasses.size() != 1) {
          throw new IllegalStateException(
              "Field '" + fieldName + "' has multiple types of data as values, which is not allowed: " + valueClasses);
        }
      }

      // validate children
      Collection<DiqubeData> children = new ArrayList<>();
      data.values().stream().filter(d -> d instanceof DiqubeData).forEach(d -> children.add((DiqubeData) d));
      repeatedData.values().stream().flatMap(l -> l.stream()).filter(d -> d instanceof DiqubeData)
          .forEach(d -> children.add((DiqubeData) d));

      children.forEach(d -> d.validate());
    }

    /* package */Map<String, Object> getData() {
      return data;
    }

    /* package */Map<String, List<Object>> getRepeatedData() {
      return repeatedData;
    }

  }

}
