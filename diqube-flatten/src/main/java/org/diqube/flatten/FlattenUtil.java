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
package org.diqube.flatten;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.UUID;
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
import org.diqube.data.flatten.FlattenedIndexRemovingColumnPage;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.flatten.FlattenedTableShard;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.dbl.dict.ConstantDoubleDictionary;
import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.types.str.dict.ConstantStringDictionary;
import org.diqube.data.types.str.dict.StringDictionary;
import org.diqube.data.util.FlattenedTableNameGenerator;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.executionenv.querystats.QueryableLongColumnShardFacade;
import org.diqube.executionenv.util.ColumnPatternUtil;
import org.diqube.executionenv.util.ColumnPatternUtil.ColumnPatternContainer;
import org.diqube.executionenv.util.ColumnPatternUtil.LengthColumnMissingException;
import org.diqube.executionenv.util.ColumnPatternUtil.PatternException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.compression.CompressedDoubleDictionaryBuilder;
import org.diqube.loader.compression.CompressedLongArrayBuilder;
import org.diqube.loader.compression.CompressedLongArrayBuilder.BitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongArrayBuilder.ReferenceAndBitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.loader.compression.CompressedStringDictionaryBuilder;
import org.diqube.util.DiqubeCollectors;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;

/**
 * Flattens a {@link Table} on a specific (repeated) field, i.e. that for each entry in the repeated field that is
 * denoted by the flatten-by field the resulting table will contain a separate row.
 * 
 * <p>
 * The resulting table will have a different number of rows, as for each index of the repeated field of each row, a new
 * row will be provided.
 * 
 * <p>
 * Example input table with two rows and a nested array:
 * 
 * <pre>
 * { a : [ { b : 1 },
 *         { b : 2 } ],
 *   c : 9 },
 * { a : [ { b : 3 },
 *         { b : 4 } ],
 *   c : 10}
 * </pre>
 * 
 * When flattenning this over "a[*]", all elements in the a[.] array are separated into a single row (= table with 4
 * rows):
 * 
 * <pre>
 * { a.b : 1, c : 9 },
 * { a.b : 2, c : 9 },
 * { a.b : 3, c : 10 },
 * { a.b : 4, c : 10 }
 * </pre>
 * 
 * <p>
 * Note that values are not validated anyhow. That means that if a specific entry in the array did not have all fields
 * defined, those non-defined fields will be non-defined in the resulting rows. TODO #14: Support optional columns.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenUtil {
  private static final Logger logger = LoggerFactory.getLogger(FlattenUtil.class);

  private static final long CONSTANT_PAGE_DICT_INTERMEDIARY_VALUE = 0L;

  @Inject
  private FlattenDataFactory factory;

  @Inject
  private RepeatedColumnNameGenerator repeatedColNameGen;

  @Inject
  private FlattenedTableNameGenerator flattenedTableNameGen;

  @Inject
  private ColumnPatternUtil colPatternUtil;

  /**
   * Flatten the given table by the given flatten-by field, returning a premilinary flattened table (see below).
   * 
   * <p>
   * For details, see class doc.
   * 
   * <p>
   * For each TableShard, one new {@link TableShard} will be created. Note that each flattened table shard will have the
   * same firstRowId as the corresponding input table Shard - although the flattened shards will usually contain more
   * rows. This means that most probably the rowIds will be overlapping in the returned flattenedTable! <b>This needs to
   * be fixed after calling this method, otherwise the Table is not usable!</b>. Typically a table is spread over
   * multiple cluster nodes, which means that fixing the firstRowIds requires communicating with the other nodes,
   * therefore this util class does not take care of this.
   * 
   * @param inputTable
   *          The table that should be flattened. This cannot be an already flattened table.
   * @param inputTableShards
   *          Specify the tableShards to work on. If this is not set (== <code>null</code>), then the tableShards will
   *          be read from the inputTable.
   * @param flattenByField
   *          The field which should be flattened by, in the usual "all-array-notation" as defined in
   *          {@link RepeatedColumnNameGenerator} (e.g. a[*].c.b[*] to get a single row for each index in all the "b"
   *          arrays in a[*].c).
   * @param flattenId
   *          The ID of the flattening that should be used to generate the output table name.
   * @return The flattened table.
   * @throws IllegalArgumentException
   *           If a passed argument is invalid.
   * @throws PatternException
   *           If the flattenByField pattern was not recognized.
   * @throws LengthColumnMissingException
   *           If any required "length" col is missing.
   * @throws IllegalStateException
   *           If the table cannot be flattened for any reason.
   */
  public FlattenedTable flattenTable(Table inputTable, Collection<TableShard> inputTableShards, String flattenByField,
      UUID flattenId)
          throws IllegalArgumentException, IllegalStateException, PatternException, LengthColumnMissingException {
    if (inputTable instanceof FlattenedTable)
      throw new IllegalArgumentException("Cannot flatten an already flattened table.");

    if (!flattenByField.endsWith(repeatedColNameGen.allEntriesIdentifyingSubstr()))
      throw new IllegalArgumentException(
          "Flatten-By field does not end with '" + repeatedColNameGen.allEntriesIdentifyingSubstr() + "'");

    String resultTableName =
        flattenedTableNameGen.createFlattenedTableName(inputTable.getName(), flattenByField, flattenId);

    if (inputTableShards == null)
      inputTableShards = inputTable.getShards();

    List<TableShard> flattenedTableShards = new ArrayList<>();
    for (TableShard shard : inputTableShards)
      flattenedTableShards.add(flattenTableShard(resultTableName, shard, flattenByField));

    Set<Long> firstRowIdsOfInputShards =
        inputTableShards.stream().map(shard -> shard.getLowestRowId()).collect(Collectors.toSet());

    return factory.createFlattenedTable(resultTableName, flattenedTableShards, firstRowIdsOfInputShards);
  }

  /**
   * Flattens a single {@link TableShard}.
   * 
   * <p>
   * This works as follows:
   * 
   * <ol>
   * <li>Find all patterns the flatten-by-field pattern matches to. These are then the prefixes of the column names of
   * which a new row will be created.
   * <li>Also find the names of the length columns of these patterns.
   * <li>Produce a to-do list: What is the name of the output columns and what input columns is that output column
   * created from?
   * <ul>
   * <li>Is the new column a "multiplicating col"? These cols are cols that are outside of the path of the repeated
   * column that is flattened over. Nevertheless each input col contains a value for that row: A single row-value of the
   * input columns needs to be available for multiple cols on the output table.
   * <li>Remove previously found length-columns from to-be-created col list (when flattening over a[*] we do not want a
   * a[length] column to appear in the output!).
   * </ul>
   * <li>Iterate over all rows of the input col and identify for each row and identify (1) how many output rows that row
   * will create (taking into account the length columns of the flatten-by field in that row) and (2) if this row is
   * missing of any child-fields (i.e. there is an array a[*].c[*], when flattening over a[*], there are output cols
   * a.c[0], a.c[1], a.c[2], but it could be that a specific row does not contain a.c[2], because that row simply does
   * not have that many entries in the array.
   * <li>Build the new columns - each new column can be either "multiplicating" (see above), in which case the col pages
   * are repeated accordingly (and no-longer repeated rows are removed from the repeated colpages) or they can be
   * "flattned" - in which case the col is a sub-field of the flattened one and we only need to remove rows that do not
   * contain any value.
   * </ol>
   * 
   * We need to ensure that we do not mess up with the row-ordering of the various output columns: Each output column
   * needs to have the same number of rows and the rowIds need to match correctly. Therefore, when creating a column
   * e.g. based on inputColumns where we do not have realized all, we need to insert "constant" column pages into the
   * output which will then resolve to default values. Example:
   * 
   * Source table:
   * 
   * <pre>
   * {a:[ { b:[1] },
   *      { b:[2, 3] }]},
   * {a:[ { b:[4] },
   *      { b:[5, 6] }]}
   * </pre>
   * 
   * In this example, there will be no column a[0].b[1] in the input (as all a[0]s only have at max a single entry in
   * .b). Would we now map new columns to col pages of old columns in the following way (flattened over a[*]; displayed
   * is the list of col pages that are consecutively accessed for a new column):
   * 
   * <pre>
   * a.b[0] = [ all col pages of a[0].b[0] ]
   * a.b[1] = [ all col pages of a[0].b[1], all col pages of a[1].b[1] ]
   * a.b[length] = [ all col pages of a[0].b[length], all col pages of a[1].b[length] ]
   * </pre>
   * 
   * .. in that way we would mess up as a.b[0] would have less rows than a.b[1] -> we need to add a "constant" colPage
   * to a.b[0] to resolve to a default value. Note that we nevertheless will probably never resolve those default values
   * (at least in this example) as the a.b[length] value will not allow us to iterate that far in the corresponding
   * rows.
   * 
   * <p>
   * Note that the resulting TableShard will have the same first Row ID as the input TableShard. If multiple TableShards
   * of the same table are flattened (this is usually the case), then after flattening them, the row IDs might overlap
   * (since every TableShard has the original firstRow ID, but each table shard contains more rows). The rowIds need to
   * be adjusted afterwards!.
   */
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

    Set<String> allInputLengthColsOfFlattenedFields = new HashSet<>();

    ColumnPatternContainer patterns = colPatternUtil.findColNamesForColNamePattern(lengthColName -> {
      allInputLengthColsOfFlattenedFields.add(lengthColName);
      return new QueryableLongColumnShardFacade(inputTableShard.getLongColumns().get(lengthColName));
    } , repeatedFieldsAlongPath);

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
    Set<String> multiplicatingOutputCols = new HashSet<>();

    Set<String> allInputColNames = inputTableShard.getColumns().keySet();

    for (String inputColName : allInputColNames) {
      if (allInputLengthColsOfFlattenedFields.contains(inputColName))
        // Remove certian length columns from the set of to-be-created columns. For example when flattenning over a[*],
        // we do not want to create a[length] column, as it simply does not make sense any more as each of the entries
        // in a[*] is now a separate row.
        continue;

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
              // Example: matched: a[0], but flattened over a[*].b[*]
              multiplicatingOutputCols.add(newColName);
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
        multiplicatingOutputCols.add(newColName);
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

    logger.trace("Will flatten following columns using following input cols (limit): {}",
        Iterables.limit(newColumns.entrySet(), 100));
    logger.trace("Following columns will be multiplicating (limit): {}",
        Iterables.limit(multiplicatingOutputCols, 100));

    // prepare information of single rows:

    NavigableMap<Long, Integer> multiplicationFactor = new TreeMap<>();
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
      multiplicationFactor.put(inputRowId, numberOfNewRows);
      mostSpecificColPatterns.forEach(colPattern -> numberOfRowsByFlattenedPrefix.merge(colPattern, 1, Integer::sum));

      // This row might not have valid values for all those repeated cols that are available in the Table for the
      // flatten-by field. Find those columns that are missing.
      for (String notAvailableColName : Sets.difference(prefixesToReplace.get(0), mostSpecificColPatterns)) {
        if (!rowIdsNotAvailableForInputCols.containsKey(notAvailableColName))
          rowIdsNotAvailableForInputCols.put(notAvailableColName, new TreeSet<>());
        rowIdsNotAvailableForInputCols.get(notAvailableColName).add(inputRowId);
      }
    }

    logger.trace("Multiplication factors are the following for all rows (limit): {}",
        Iterables.limit(multiplicationFactor.entrySet(), 100));

    int maxMultiplicationFactor = multiplicationFactor.values().stream().mapToInt(Integer::intValue).max().getAsInt();

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
      // The artificial ID is defined in a way so it can be fed to #mergeDicts(.)
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

        if (multiplicatingOutputCols.contains(newColName)) {
          for (int multiplication = 0; multiplication < maxMultiplicationFactor; multiplication++)
            for (ColumnPage inputPage : inputTableShard.getColumns().get(inputColName).getPages().values()) {
              ColumnPage newPage;
              // create new page without the final dictionaries! We simply use the original Dicts first.
              // newPage = factory.createFlattenedMultiplicatingColumnPage(newColName + "#" + nextFirstRowId,
              // factory.createFlattenedDelegateLongDictionary(inputPage.getColumnPageDict()), inputPage,
              // new HashMap<>(), nextFirstRowId);

              Map<Long, Integer> curPageMultiplyingFactor = multiplicationFactor.subMap(inputPage.getFirstRowId(),
                  inputPage.getFirstRowId() + inputPage.getValues().size());

              final int curMultiplicationNo = multiplication;
              NavigableSet<Long> notAvailableRowIds = LongStream
                  .range(inputPage.getFirstRowId(), inputPage.getFirstRowId() + inputPage.getValues().size()).filter( //
                      rowId -> //
                      (curPageMultiplyingFactor.containsKey(rowId) ? curPageMultiplyingFactor.get(rowId)
                          : 1) <= curMultiplicationNo)
                  .mapToObj(Long::valueOf).collect(DiqubeCollectors.toNavigableSet());

              newPage = createRowRemovedFlattenedColumnPage(newColName + "#" + nextFirstRowId, inputPage,
                  notAvailableRowIds, nextFirstRowId);

              if (newPage != null) {
                flattenedColPages.get(curColId).add(newPage);
                nextFirstRowId += newPage.size();
              }
            }
        } else {
          for (ColumnPage inputPage : inputTableShard.getColumns().get(inputColName).getPages().values()) {
            ColumnPage newPage;
            // create new page without the final dictionaries! We simply use the original Dicts first.
            SortedSet<Long> notAvailableRowIdsThisPage;
            String interestingPrefix = rowIdsNotAvailableForInputCols.floorKey(inputColName);
            if (interestingPrefix != null && inputColName.startsWith(interestingPrefix))
              notAvailableRowIdsThisPage = rowIdsNotAvailableForInputCols.get(interestingPrefix)
                  .subSet(inputPage.getFirstRowId(), inputPage.getFirstRowId() + inputPage.getValues().size());
            else
              notAvailableRowIdsThisPage = new TreeSet<>();

            newPage = createRowRemovedFlattenedColumnPage(newColName + "#" + nextFirstRowId, inputPage,
                notAvailableRowIdsThisPage, nextFirstRowId);

            if (newPage != null) {
              flattenedColPages.get(curColId).add(newPage);
              nextFirstRowId += newPage.size();
            }
          }
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

      logger.trace("Created flattened column {}", newColName);
    }

    FlattenedTableShard flattenedTableShard = factory.createFlattenedTableShard(resultTableName, flattenedColShards);

    logger.trace("Created flattened table shard " + resultTableName);

    return flattenedTableShard;
  }

  /**
   * Creates a new ColumnPage the default way for a flattened col shard (= removes specific rows from the page).
   * 
   * @param colPageName
   *          Name of the new page
   * @param delegate
   *          The delegate {@link ColumnPage} of the input col shard.
   * @param notAvailableRowIds
   *          The RowIds that should not be available in the result col page as compared to the input col page.
   * @param newFirstRowId
   *          firstRowId the new page would have
   * @return The new {@link ColumnPage}. Can be <code>null</code> in case the new colPage should not be added (e.g. if
   *         it would be empty).
   */
  private ColumnPage createRowRemovedFlattenedColumnPage(String colPageName, ColumnPage delegate,
      SortedSet<Long> notAvailableRowIds, long newFirstRowId) {
    if (delegate.getValues().size() == notAvailableRowIds.size())
      // we'd remove all values from the delegate, so we actually do not need to build a page at all.
      return null;

    // TODO #27: Use heuristic to identify the least-memory-intensive way to store this.
    // It could be that we remove that much indices here that it would be easier to (1) store the indices that are still
    // enabled or (2) re-create the column completely.
    ColumnPage newPage = factory.createFlattenedIndexRemovingColumnPage(colPageName,
        factory.createFlattenedDelegateLongDictionary(delegate.getColumnPageDict()), delegate,
        compressRemovedRowIdIndicesToRemovedIndicesArray(notAvailableRowIds, delegate.getFirstRowId()), newFirstRowId);

    return newPage;
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
   *          the number of entries of all previous dicts. First ID is 0L.
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

  /**
   * Transforms a set of rowIds which should not be accessible in {@link FlattenedIndexRemovingColumnPage} to the
   * {@link CompressedLongArray} of "removed indices" of the values array of that page, as that is needed to construct a
   * {@link FlattenedIndexRemovingColumnPage}.
   * 
   * @param removedRowIds
   * @param firstRowId
   * @return
   */
  private CompressedLongArray<?> compressRemovedRowIdIndicesToRemovedIndicesArray(SortedSet<Long> removedRowIds,
      long firstRowId) {
    // do not use RunLengthLongArray startegy, as stated by FlattenedIndexRemovingColumnPage
    @SuppressWarnings("unchecked")
    CompressedLongArrayBuilder builder = new CompressedLongArrayBuilder()
        .withStrategies(BitEfficientCompressionStrategy.class, ReferenceAndBitEfficientCompressionStrategy.class);

    long[] values = removedRowIds.stream().mapToLong(rowId -> rowId - firstRowId).toArray();

    builder.withValues(values);

    return builder.build();
  }

}
