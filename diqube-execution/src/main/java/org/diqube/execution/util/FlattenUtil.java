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
package org.diqube.execution.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.flatten.AdjustableConstantLongDictionary;
import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedDelegateLongDictionary;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.flatten.FlattenedTableShard;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.dbl.dict.ConstantDoubleDictionary;
import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.types.str.dict.ConstantStringDictionary;
import org.diqube.data.types.str.dict.StringDictionary;
import org.diqube.data.util.FlattenedTableNameGenerator;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.env.querystats.QueryableLongColumnShardFacade;
import org.diqube.execution.util.ColumnPatternUtil.ColumnPatternContainer;
import org.diqube.execution.util.ColumnPatternUtil.LengthColumnMissingException;
import org.diqube.execution.util.ColumnPatternUtil.PatternException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.compression.CompressedDoubleDictionaryBuilder;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.loader.compression.CompressedStringDictionaryBuilder;
import org.diqube.util.Pair;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;

/**
 * Flattens a {@link Table} on a specific (repeated) field.
 * 
 * <p>
 * The resulting table will have a different number of rows, as for each index of the repeated field of each row, a new
 * row will be provided.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenUtil {

  private static final long CONSTANT_PAGE_DICT_INTERMEDIARY_VALUE = 0L;

  @Inject
  private FlattenDataFactory factory;

  @Inject
  private RepeatedColumnNameGenerator repeatedColNameGen;

  @Inject
  private FlattenedTableNameGenerator flattenedTableNameGen;

  @Inject
  private ColumnPatternUtil colPatternUtil;

  public FlattenedTable flattenTable(Table inputTable, String flattenByField)
      throws IllegalArgumentException, PatternException, LengthColumnMissingException {
    if (inputTable instanceof FlattenedTable)
      throw new IllegalArgumentException("Cannot flatten an already flattened table.");

    if (!flattenByField.endsWith(repeatedColNameGen.allEntriesIdentifyingSubstr()))
      throw new IllegalArgumentException(
          "Flatten-By field does not end with '" + repeatedColNameGen.allEntriesIdentifyingSubstr() + "'");

    String resultTableName = flattenedTableNameGen.createFlattenedTableName(inputTable, flattenByField);

    List<TableShard> flattenedTableShards = new ArrayList<>();
    for (TableShard shard : inputTable.getShards())
      flattenedTableShards.add(flattenTableShard(resultTableName, shard, flattenByField));

    return factory.createFlattenedTable(resultTableName, flattenedTableShards);
  }

  private TableShard flattenTableShard(String resultTableName, TableShard inputTableShard, String flattenByField)
      throws PatternException, LengthColumnMissingException, IllegalStateException {
    String[] flattenFieldSplit =
        flattenByField.split(Pattern.quote(repeatedColNameGen.allEntriesIdentifyingSubstr() + "."));
    List<String> repeatedFieldsAlongPath = new ArrayList<>();
    String prev = "";
    for (String splitPart : flattenFieldSplit) {
      if (!"".equals(prev))
        prev += ".";

      prev += splitPart;
      if (!splitPart.endsWith(repeatedColNameGen.allEntriesIdentifyingSubstr()))
        prev += repeatedColNameGen.allEntriesIdentifyingSubstr();

      repeatedFieldsAlongPath.add(prev);
    }

    // calculate the most specific patterns first - colPatternUtil will return its lists in the same ordering!
    repeatedFieldsAlongPath = Lists.reverse(repeatedFieldsAlongPath);

    ColumnPatternContainer patterns = colPatternUtil.findColNamesForColNamePattern(
        lengthColName -> new QueryableLongColumnShardFacade(inputTableShard.getLongColumns().get(lengthColName)),
        repeatedFieldsAlongPath);

    // transpose result of colPatternUtil: Collect all the most specific patterns in a set, then the second-most
    // specific patterns etc.
    // Later we want to first check if a colname matches one of the most specfic patterns as prefix and replace that,
    // before checking if it matches some less-specific patterns.
    List<Set<String>> prefixesToReplace = new ArrayList<>();
    for (int i = 0; i < repeatedFieldsAlongPath.size(); i++)
      prefixesToReplace.add(new HashSet<>());
    for (List<String> patternList : patterns.getMaximumColumnPatterns()) {
      for (int i = 0; i < patternList.size(); i++)
        prefixesToReplace.get(i).add(patternList.get(i));
    }

    // Prefix replacements based on index in prefixesToReplace: If a prefix of prefixesToReplace.get(0) is found, that
    // prefix needs to be replaced by replacements.get(0).
    List<String> replacements = repeatedFieldsAlongPath.stream()
        .map(pattern -> pattern.replaceAll(Pattern.quote(repeatedColNameGen.allEntriesIdentifyingSubstr()), ""))
        .collect(Collectors.toList());

    // map from new column name to input column names that column is based upon. Note that input col names might not
    // exist in inputTableShard, see comments below when newColumn is filled.
    Map<String, SortedSet<String>> newColumns = new HashMap<>();
    // output cols whose row-values are based on using input cols values and each row value of those inputs is the value
    // of multiple output cols
    Set<String> multiplyingOutputCols = new HashSet<>();

    Set<String> allInputColNames = inputTableShard.getColumns().keySet();

    for (String inputColName : allInputColNames) {
      String newColName = null;
      String foundPrefix = null;
      int foundPatternIdx = -1;
      for (int patternIdx = 0; patternIdx < prefixesToReplace.size(); patternIdx++) {
        Set<String> prefixes = prefixesToReplace.get(patternIdx);
        for (String prefix : prefixes) {
          if (inputColName.startsWith(prefix)) {
            newColName = inputColName.replaceFirst(Pattern.quote(prefix), replacements.get(patternIdx));
            foundPrefix = prefix;
            foundPatternIdx = patternIdx;
            if (patternIdx > 0)
              // not the first list of prefixes matched (= created from pattern equalling the "flatten-by"), but
              // less-specific patterns matched. That means that this column needs to act in a way, that the value of
              // one input row needs to be projected to multiple rows on the output side.
              multiplyingOutputCols.add(newColName);
            break;
          }
        }
        if (newColName != null)
          break;
      }

      if (newColName == null) {
        // no replacement found, this column is on different path than the flattened one, do not flatten, do not
        // replace.
        newColName = inputColName;
        // At the same time, this column needs to be multiplied: One row of the input col needs to be available in
        // multiple rows in the output.
        multiplyingOutputCols.add(newColName);
      }

      if (!newColumns.containsKey(newColName))
        newColumns.put(newColName, new TreeSet<>());

      // Add all "potentially available" input columns to the newColName. It could be that for a specific repetition, a
      // child-field is missing, e.g. a[0].c does not exist, but a[1].c does. Nevertheless, we need to reserve some
      // "space" for a[0].c in the new column a.c, because otherwise the rows of an existing a[0].d will mess up with
      // the rows of a[1].c, because a.c does contain the values of rows of a[1].c first, but a.d does contain a[0].d
      // first
      if (foundPatternIdx == -1)
        newColumns.get(newColName).add(inputColName);
      else {
        // add all eg. a[*].c as input columns, no matter if they exist or not.
        for (String inputPref : prefixesToReplace.get(foundPatternIdx))
          newColumns.get(newColName).add(inputColName.replaceFirst(Pattern.quote(foundPrefix), inputPref));
      }
    }

    // TODO remove unneeded [length] cols of flattened cols!

    // prepare information of single rows:

    NavigableMap<Long, Integer> multiplyingFactor = new TreeMap<>();
    // map from input col prefix to rowIds that are not available for all cols starting with that prefix.
    NavigableMap<String, NavigableSet<Long>> rowIdsNotAvailableForInputCols = new TreeMap<>();

    // number of rows that are generated for one of the prefixes created based on the flatten-by value. Example: When
    // flattening over a[*], this will contain: a[0] -> generates X rows, a[1] -> generates Y rows.
    Map<String, Integer> numberOfRowsByFlattenedPrefix = new HashMap<>();

    for (long inputRowId = inputTableShard.getLowestRowId(); inputRowId < inputTableShard.getLowestRowId()
        + inputTableShard.getNumberOfRowsInShard(); inputRowId++) {

      // find the cols of the "flatten-by" field that actually exist for this row.
      Set<List<String>> colPatterns = patterns.getColumnPatterns(inputRowId);
      Set<String> mostSpecificColPatterns = // most-specific = the flatten-by field!
          colPatterns.stream().flatMap(l -> Stream.of(l.get(0))).collect(Collectors.toSet());

      // This row will produce this many rows in the output.
      int numberOfNewRows = mostSpecificColPatterns.size();
      multiplyingFactor.put(inputRowId, numberOfNewRows);
      mostSpecificColPatterns.forEach(colPattern -> numberOfRowsByFlattenedPrefix.merge(colPattern, 1, Integer::sum));

      // This row might not have valid values for all those repeated cols that are available in the Table for the
      // flatten-by field. Find those columns that are missing.
      for (String notAvailableColName : Sets.difference(prefixesToReplace.get(0), mostSpecificColPatterns)) {
        if (!rowIdsNotAvailableForInputCols.containsKey(notAvailableColName))
          rowIdsNotAvailableForInputCols.put(notAvailableColName, new TreeSet<>());
        rowIdsNotAvailableForInputCols.get(notAvailableColName).add(inputRowId);
      }
    }

    // Build new col shards
    List<StandardColumnShard> flattenedColShards = new ArrayList<>();
    for (String newColName : newColumns.keySet()) {
      long nextFirstRowId = inputTableShard.getLowestRowId();

      // map from an artificial ID to the dictionary of an input column. The artificial ID is built the following way:
      // The first dict has artificial ID 0.
      // The second dict has artificial ID = number of entries in first dict
      // The third dict has artificial ID = number of entries in second dict
      // and so on
      // -> basically every entry in the dict has it's own artificial ID. These must not be overlapping!
      Map<Long, Dictionary<?>> origColDicts = new HashMap<>();
      long nextColAndColDictId = 0L;

      // map from "artificial" column ID to list of flattened col pages.
      Map<Long, List<ColumnPage>> flattenedColPages = new HashMap<>();

      // find colType by searching an input col that exists and taking the coltype of that one.
      ColumnType colType = newColumns.get(newColName).stream()
          .filter(inputColName -> inputTableShard.getColumns().containsKey(inputColName))
          .map(inputColName -> inputTableShard.getColumns().get(inputColName).getColumnType()).findAny().get();

      // Build for each input colpage in each input col a separate new colPage. This is far from optimal, but we can
      // merge those pages later on. TODO #27.
      for (String inputColName : newColumns.get(newColName)) {
        long curColId = nextColAndColDictId;

        if (!inputTableShard.getColumns().containsKey(inputColName)) {
          // This col does not exist, therefore we add an "empty" colPage, which resolves statically to the colTypes'
          // default value.

          // The size of the page is identified by the number of rows that flattened prefix would have.
          int noOfRows = -1;
          for (String prefix : numberOfRowsByFlattenedPrefix.keySet()) {
            if (inputColName.startsWith(prefix)) {
              noOfRows = numberOfRowsByFlattenedPrefix.get(prefix);
              break;
            }
          }
          if (noOfRows == -1)
            throw new IllegalStateException("Could not find number of rows for empty ColPage.");

          ColumnPage newPage = factory.createFlattenedConstantColumnPage(newColName + "#" + nextFirstRowId,
              // create a dict whose value we will change later on.
              factory.createAdjustableConstantLongDictionary(CONSTANT_PAGE_DICT_INTERMEDIARY_VALUE), //
              nextFirstRowId, noOfRows);
          flattenedColPages.put(nextColAndColDictId, new ArrayList<>(Arrays.asList(newPage)));
          nextFirstRowId += noOfRows;

          // assume we had an input col dict for this non-existing col. These dicts will later be merged to one single
          // dict - we need to make sure that that merged dict contains the default value returned from the page dict we
          // created above.
          if (inputColName.endsWith(repeatedColNameGen.lengthIdentifyingSuffix()))
            // length cols get "0" as default.
            origColDicts.put(curColId, new ConstantLongDictionary(0L, 0L));
          else
            origColDicts.put(curColId, createDictionaryWithOnlyDefaultValue(colType));

          nextColAndColDictId++; // single entry dict!

          continue;
        }

        Dictionary<?> colShardDict = inputTableShard.getColumns().get(inputColName).getColumnShardDictionary();
        origColDicts.put(curColId, colShardDict);
        nextColAndColDictId += colShardDict.getMaxId() + 1;

        flattenedColPages.put(curColId, new ArrayList<>());

        for (ColumnPage inputPage : inputTableShard.getColumns().get(inputColName).getPages().values()) {
          ColumnPage newPage;
          // create new page without the final dictionaries! We simply use the original Dicts first.
          if (multiplyingOutputCols.contains(newColName)) {
            Map<Long, Integer> curPageMultiplyingFactor = multiplyingFactor.subMap(inputPage.getFirstRowId(),
                inputPage.getFirstRowId() + inputPage.getValues().size());

            newPage = factory.createFlattenedMultiplicatingColumnPage(newColName + "#" + nextFirstRowId,
                factory.createFlattenedDelegateLongDictionary(inputPage.getColumnPageDict()), inputPage,
                curPageMultiplyingFactor, nextFirstRowId);
          } else {
            Set<Long> notAvailableRowIdsThisPage;
            String interestingPrefix = rowIdsNotAvailableForInputCols.floorKey(inputColName);
            if (interestingPrefix != null && inputColName.startsWith(interestingPrefix))
              notAvailableRowIdsThisPage = rowIdsNotAvailableForInputCols.get(interestingPrefix)
                  .subSet(inputPage.getFirstRowId(), inputPage.getFirstRowId() + inputPage.getValues().size());
            else
              notAvailableRowIdsThisPage = new HashSet<>();

            newPage = factory.createFlattenedCombiningColumnPage(newColName + "#" + nextFirstRowId,
                factory.createFlattenedDelegateLongDictionary(inputPage.getColumnPageDict()), inputPage,
                notAvailableRowIdsThisPage, nextFirstRowId);
          }

          flattenedColPages.get(curColId).add(newPage);
          nextFirstRowId += newPage.size();
        }
      }

      Pair<Dictionary<?>, Map<Long, Map<Long, Long>>> mergeDictInfo = mergeDicts(newColName, colType, origColDicts);
      Dictionary<?> colDict = mergeDictInfo.getLeft();

      // after merging the col dict, we need to adjust the colPage dicts to map to the new col value IDs!
      for (Entry<Long, Map<Long, Long>> mapInfoEntry : mergeDictInfo.getRight().entrySet()) {
        long colId = mapInfoEntry.getKey();
        Map<Long, Long> mapInfo = mapInfoEntry.getValue();

        if (mapInfo != null && !mapInfo.isEmpty()) {
          for (ColumnPage page : flattenedColPages.get(colId)) {
            if (page.getColumnPageDict() instanceof FlattenedDelegateLongDictionary) {
              FlattenedDelegateLongDictionary delegateDict = (FlattenedDelegateLongDictionary) page.getColumnPageDict();
              LongDictionary<?> origDict = delegateDict.getDelegate();

              LongDictionary<?> mappedDict = createValueMappedLongDict(origDict, mapInfo);

              delegateDict.setDelegate(mappedDict);
            } else if (page.getColumnPageDict() instanceof AdjustableConstantLongDictionary) {
              AdjustableConstantLongDictionary<?> dict = (AdjustableConstantLongDictionary<?>) page.getColumnPageDict();
              dict.setValue(mapInfo.get(CONSTANT_PAGE_DICT_INTERMEDIARY_VALUE));
            } else
              throw new IllegalStateException("Cannot adjust IDs in unknown page dict: " + page.getColumnPageDict());
          }
        }
      }

      // TODO #27: make sure default value is in dict and use it in constant pages!

      List<ColumnPage> flatFlattenedColPages =
          flattenedColPages.values().stream().flatMap(l -> l.stream()).collect(Collectors.toList());
      StandardColumnShard flattenedColShard = null;
      switch (colType) {
      case STRING:
        flattenedColShard = factory.createFlattenedStringStandardColumnShard(newColName, (StringDictionary<?>) colDict,
            inputTableShard.getLowestRowId(), flatFlattenedColPages);
        break;
      case LONG:
        flattenedColShard = factory.createFlattenedLongStandardColumnShard(newColName, (LongDictionary<?>) colDict,
            inputTableShard.getLowestRowId(), flatFlattenedColPages);
        break;
      case DOUBLE:
        flattenedColShard = factory.createFlattenedDoubleStandardColumnShard(newColName, (DoubleDictionary<?>) colDict,
            inputTableShard.getLowestRowId(), flatFlattenedColPages);
        break;
      }

      flattenedColShards.add(flattenedColShard);
    }

    FlattenedTableShard flattenedTableShard = factory.createFlattenedTableShard(resultTableName, flattenedColShards);

    return flattenedTableShard;
  }

  /**
   * Merges multiple col dicts into one.
   * 
   * <p>
   * The input dictionaries are expected to be of type T. T must be {@link Comparable} (which though is no problem for
   * our values of String, Long, Double).
   * 
   * @param inputDicts
   *          The col dicts of the input cols, indexed by an artificial "dictionary id", which for one dict basically is
   *          the number of entries of all previous dicts.
   * @return Pair of merged dictionary and for each input dict ID a mapping map. That map maps from old dict ID of a
   *         value to the new dict ID in the merged dict. Map can be empty.
   */
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> Pair<Dictionary<?>, Map<Long, Map<Long, Long>>> mergeDicts(String colName,
      ColumnType colType, Map<Long, Dictionary<?>> inputDicts) throws IllegalStateException {
    Map<Long, Map<Long, Long>> resMappingMap = new HashMap<>();

    if (inputDicts.size() == 1) {
      return new Pair<>(inputDicts.values().iterator().next(), resMappingMap);
    }

    Map<Long, PeekingIterator<Pair<Long, T>>> iterators = new HashMap<>();
    for (Entry<Long, Dictionary<?>> e : inputDicts.entrySet()) {
      if (e.getValue().getMaxId() == null)
        continue;
      iterators.put(e.getKey(), Iterators.peekingIterator(((Dictionary<T>) e.getValue()).iterator()));
    }

    // order the next elements of all dicts by their value.
    // Pair of (Pair of ID in dict and value) and dictId
    PriorityQueue<Pair<Pair<Long, T>, Long>> nextElements =
        new PriorityQueue<>((p1, p2) -> p1.getLeft().getRight().compareTo(p2.getLeft().getRight()));

    for (Entry<Long, PeekingIterator<Pair<Long, T>>> e : iterators.entrySet())
      nextElements.add(new Pair<>(e.getValue().peek(), e.getKey()));

    // map from value to new ID which will be fed into the dictionary builder.
    NavigableMap<T, Long> entityMap = new TreeMap<>();
    long nextEntityId = 0L;

    Pair<T, Long> previous = null;

    // traverse all dictionaries and build mapping list
    while (!nextElements.isEmpty()) {
      Pair<Pair<Long, T>, Long> p = nextElements.poll();
      Long dictId = p.getRight();
      Pair<Long, T> valuePair = p.getLeft();

      // move iterator forward
      iterators.get(dictId).next();
      if (iterators.get(dictId).hasNext())
        nextElements.add(new Pair<>(iterators.get(dictId).peek(), dictId));

      long idInInputDict = valuePair.getLeft();
      if (previous == null || valuePair.getRight().compareTo(previous.getLeft()) > 0) {
        long resultNewId = nextEntityId++;

        entityMap.put(valuePair.getRight(), resultNewId);

        previous = new Pair<>(valuePair.getRight(), resultNewId);
      }

      if (!resMappingMap.containsKey(dictId))
        resMappingMap.put(dictId, new HashMap<>());
      resMappingMap.get(dictId).put(idInInputDict, previous.getRight());
    }

    Dictionary<?> resDict = null;
    Map<Long, Long> builderAdjustMap = null;
    switch (colType) {
    case LONG:
      CompressedLongDictionaryBuilder longBuilder = new CompressedLongDictionaryBuilder();
      longBuilder.withDictionaryName(colName).fromEntityMap((NavigableMap<Long, Long>) entityMap);
      Pair<LongDictionary<?>, Map<Long, Long>> longPair = longBuilder.build();
      builderAdjustMap = longPair.getRight();
      resDict = longPair.getLeft();
      break;
    case STRING:
      CompressedStringDictionaryBuilder stringBuilder = new CompressedStringDictionaryBuilder();
      stringBuilder.fromEntityMap((NavigableMap<String, Long>) entityMap);
      Pair<StringDictionary<?>, Map<Long, Long>> stringPair = stringBuilder.build();
      builderAdjustMap = stringPair.getRight();
      resDict = stringPair.getLeft();
      break;
    case DOUBLE:
      CompressedDoubleDictionaryBuilder doubleBuilder = new CompressedDoubleDictionaryBuilder();
      doubleBuilder.fromEntityMap((NavigableMap<Double, Long>) entityMap);
      Pair<DoubleDictionary<?>, Map<Long, Long>> doublePair = doubleBuilder.build();
      builderAdjustMap = doublePair.getRight();
      resDict = doublePair.getLeft();
      break;
    }

    if (!builderAdjustMap.isEmpty())
      throw new IllegalStateException(
          "IDs of new col dict for col " + colName + " were adjusted although that was not expected!");

    return new Pair<Dictionary<?>, Map<Long, Map<Long, Long>>>(resDict, resMappingMap);
  }

  /**
   * Creates and returns a new {@link LongDictionary} based on a different {@link LongDictionary}, but the values
   * returned by the original dict are mapped to new values.
   * 
   * @param valueMap
   *          Map from old value returned from origDict to the new value it should have
   */
  private LongDictionary<?> createValueMappedLongDict(LongDictionary<?> origDict, Map<Long, Long> valueMap)
      throws IllegalStateException {
    Long[] ids = LongStream.rangeClosed(0, origDict.getMaxId()).mapToObj(Long::valueOf).toArray(l -> new Long[l]);
    Long[] values = origDict.decompressValues(ids);
    NavigableMap<Long, Long> valueToTempId = new TreeMap<>();
    for (int i = 0; i < values.length; i++) {
      Long val;
      if (valueMap.containsKey(values[i]))
        val = valueMap.get(values[i]);
      else
        val = values[i];
      valueToTempId.put(val, ids[i]);
    }

    CompressedLongDictionaryBuilder builder = new CompressedLongDictionaryBuilder().fromEntityMap(valueToTempId);

    Pair<LongDictionary<?>, Map<Long, Long>> buildRes = builder.build();

    if (!buildRes.getRight().isEmpty())
      // The builder should not have changed the IDs, because the mappings from old value to new value should have
      // retained the same ordering of the values (see mergeDicts!). If there would be something returned here, we would
      // need to adjust the values in the "getValues()" array of the col pages (=not implemented).
      throw new IllegalStateException("Creating new ColPage dict changed IDs!");

    return buildRes.getLeft();
  }

  /**
   * Create a new dictionary of the correct type, which will have a single entry at ID 0: the default value for the
   * given type.
   */
  private Dictionary<?> createDictionaryWithOnlyDefaultValue(ColumnType colType) {
    switch (colType) {
    case STRING:
      return new ConstantStringDictionary(LoaderColumnInfo.DEFAULT_STRING, 0L);
    case LONG:
      return new ConstantLongDictionary(LoaderColumnInfo.DEFAULT_LONG, 0L);
    case DOUBLE:
      return new ConstantDoubleDictionary(LoaderColumnInfo.DEFAULT_DOUBLE, 0L);
    }
    return null; // never happens
  }

}
