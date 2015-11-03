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
package org.diqube.executionenv.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.querystats.QueryableLongColumnShard;
import org.diqube.executionenv.resolver.QueryableLongColumnShardResolver;

import com.google.common.collect.Iterables;

/**
 * Utility class that provides the resolution of column name patterns (= column names that contain "[*]" etc, see
 * {@link RepeatedColumnNameGenerator}).
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ColumnPatternUtil {
  @Inject
  private RepeatedColumnNameGenerator repeatedColNames;

  /**
   * Replaces all the [*] strings in the pattern with actual column indices.
   * 
   * @param lengthColResolver
   *          The {@link QueryableLongColumnShardResolver} that should be used to find "length" columns of the repeated
   *          fields.
   * @param pattern
   *          The column name pattern
   * @return A {@link ColumnPatternContainer} that can provide the column names.
   * @throws LengthColumnMissingException
   *           in case a "length" column for one of the fields that are marked with "[*]" in the input pattern is not
   *           available in the provided lengthColResolver.
   */
  public ColumnPatternContainer findColNamesForColNamePattern(QueryableLongColumnShardResolver lengthColResolver,
      String pattern) throws LengthColumnMissingException {
    return findColNamesForColNamePattern(lengthColResolver, Arrays.asList(pattern));
  }

  /**
   * Replaces all the [*] strings in the patterns with actual column indices.
   * 
   * The patterns have to follow the same "path" for this to work. This means that the repetitions need to be on the
   * same fields, whereas not all fields need to repeat over all those fields. There will be one result for each
   * resulting column name of the most-repeated pattern, where the column names of the less-repeated patterns will be
   * the column names of the "parent" field of the most-repeated pattern. Examples for results (assuming length = 2 on
   * all repeated fields):
   * 
   * <pre>
   * a[*].b.c[*], a[*].b.d
   * 
   * ->
   * 
   * <pre>
   * a[0].b.c[0], a[0].b.d
   * a[0].b.c[1], a[0].b.d
   * a[1].b.c[0], a[1].b.d
   * a[1].b.c[1], a[1].b.d
   * </pre>
   * 
   * <pre>
   * a[*].b.c[*], a[*].b.c[*]
   * 
   * ->
   * 
   * a[0].b.c[0], a[0].b.c[0]
   * a[0].b.c[1], a[0].b.c[1]
   * a[1].b.c[0], a[1].b.c[0]
   * a[1].b.c[1], a[1].b.c[1]
   * </pre>
   * 
   * <pre>
   * a[*].b.c[*], a.x
   * 
   * -> 
   * 
   * a[0].b.c[0], a.x
   * a[0].b.c[1], a.x
   * a[1].b.c[0], a.x
   * a[1].b.c[1], a.x
   * </pre>
   * 
   * @param lengthColResolver
   *          The {@link QueryableLongColumnShardResolver} that should be used to find "length" columns of the repeated
   *          fields.
   * @param patterns
   *          The patterns that should be resolved, adhering to the fact that they follow the same "path" (see above).
   * @return A {@link ColumnPatternContainer} that can be used to fetch the colnames.
   * @throws PatternException
   *           in case the patterns do not repeat on the same "path".
   * @throws LengthColumnMissingException
   *           In case a "length" column for one of the fields that are marked with "[*]" in the input patterns is not
   *           available in the provided lengthColResolver.
   */
  public ColumnPatternContainer findColNamesForColNamePattern(QueryableLongColumnShardResolver lengthColResolver,
      List<String> patterns) throws PatternException, LengthColumnMissingException {

    if (patterns.size() > 1) {
      // Validate that patterns "repeat" in the same paths. For example the following is invalid:
      // a.b[*].c.d[*].e
      // a.x.c[*].d
      // This is invalid, because we need to find one "path" through the repeated fields for the most-times repeated
      // pattern, and all other patterns have to resolve to values along that "path".
      // We validate this by finding the "last repeated field" for each pattern (= field name of the last field with a
      // [*]). We then take the longest one (=the most specific one) and validate that this longest one "startsWith"
      // each of the other "last repeated field" strings.

      List<String> lastRepeatedFields = patterns.stream().map(s -> {
        int lastRepeatedIdx = s.lastIndexOf(repeatedColNames.allEntriesIdentifyingSubstr());
        if (lastRepeatedIdx == -1)
          // we no not care about fields that are not repeated at all.
          return null;
        return s.substring(0, lastRepeatedIdx);
      }).filter(s -> s != null).sorted((s1, s2) -> -1 * Integer.compare(s1.length(), s2.length()))
          .collect(Collectors.toList());

      if (lastRepeatedFields.size() > 1) {
        String longestRepeatedField = lastRepeatedFields.get(0);

        // TODO support case with fixed index: a.b[*].c[*].d, a.b[0].c[*].d
        Optional<String> badMatched;
        if ((badMatched = lastRepeatedFields.stream().filter(s -> !longestRepeatedField.startsWith(s)).findAny())
            .isPresent())
          throw new PatternException("Column pattern set invalid, as the patterns repeat on different paths: "
              + longestRepeatedField + " vs. " + badMatched.get());
      }
    }

    if (!patterns.stream().anyMatch(p -> p.contains(repeatedColNames.allEntriesIdentifyingSubstr())))
      throw new PatternException("No [*] in any pattern");

    List<List<String>> baseNames = new ArrayList<>();
    for (String pattern : patterns) {
      List<String> newBaseNames =
          new ArrayList<>(Arrays.asList(pattern.split(Pattern.quote(repeatedColNames.allEntriesIdentifyingSubstr()))));

      if (pattern.endsWith(repeatedColNames.allEntriesIdentifyingSubstr()))
        // last baseName will not be repeated, but in this pattern the last one /is/ repeated. Append empty string to
        // simulate correct behaviour.

        newBaseNames.add("");

      baseNames.add(newBaseNames);
    }

    return new ColumnPatternContainer(lengthColResolver, baseNames);
  }

  /** for tests */
  void setRepeatedColNames(RepeatedColumnNameGenerator repeatedColNames) {
    this.repeatedColNames = repeatedColNames;
  }

  /**
   * Exception showing that the provided pattern(s) are invalid.
   */
  public static class PatternException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PatternException(String msg) {
      super(msg);
    }
  }

  /**
   * Exception showing that a length column is missing for a field that has a [*].
   */
  public static class LengthColumnMissingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public LengthColumnMissingException(String msg) {
      super(msg);
    }
  }

  /**
   * Contains the actual column names of resolved patterns.
   */
  public class ColumnPatternContainer {
    private static final long MAX_LEN = Long.MIN_VALUE;

    /**
     * Map from indices (for each occurrence of [*] one) to a list of {@link ConcatStringProvider}s, for each pattern
     * one.
     */
    private Map<List<Long>, List<ConcatStringProvider>> stringProviders = new HashMap<>();
    /**
     * The input patterns, split up at [*]. The resulting column names will have indices after each basename, except for
     * the last one (which will not be "repeated"). Note that each pattern may have a different number of baseNames, but
     * all patterns are along the same "path".
     */
    private List<List<String>> baseNames;
    /** index in {@link #baseNames} where the list of basenames is the longest. */
    private int longestPatternBaseNameIndex;
    /** number of [*] that need to be inserted. */
    private int numberOfStars;
    private QueryableLongColumnShardResolver lengthColResolver;

    /**
     * 
     * @param baseNames
     *          The patterns. Each pattern needs to be split up into a List<String> by splitting the string at [*]. This
     *          {@link ColumnPatternContainer} will then fill in indices "between" two of these baseNames. Note that all
     *          baseNames need to be along the same "path".
     */
    private ColumnPatternContainer(QueryableLongColumnShardResolver lengthColResolver, List<List<String>> baseNames)
        throws LengthColumnMissingException {
      this.lengthColResolver = lengthColResolver;
      this.baseNames = baseNames;
      numberOfStars = -1;
      for (int i = 0; i < baseNames.size(); i++)
        if (baseNames.get(i).size() > numberOfStars) {
          numberOfStars = baseNames.get(i).size() - 1; // -1 -> last baseName will not get an [*] appended!
          longestPatternBaseNameIndex = i;
        }

      List<ConcatStringProvider> parentProviders = new ArrayList<>(baseNames.size());
      for (int i = 0; i < baseNames.size(); i++)
        parentProviders.add(null);
      createStringProviders(baseNames, 0, new ArrayList<>(numberOfStars), stringProviders, parentProviders);
    }

    /**
     * Return a set of a list of colnames (for each input pattern one entry in the list) with filled in repetition
     * indices for the lengths of the given rowId.
     * 
     * For each input pattern there will be one string in the returned lists. And there are potentially multiple lists
     * in a set, each list containing a different index combination.
     */
    public Set<List<String>> getColumnPatterns(long rowId) {
      Set<List<String>> res = new HashSet<>();
      getColumnPatternsRecursive(rowId, 0, new ArrayList<>(), res);
      return res;
    }

    /**
     * See {@link #getColumnPatterns(long)}, but assuming there is only a single pattern, merges the values of the
     * one-element-lists into the set directly.
     */
    public Set<String> getColumnPatternsSinglePattern(long rowId) {
      return getColumnPatterns(rowId).stream().flatMap(l -> Stream.of(Iterables.getOnlyElement(l)))
          .collect(Collectors.toSet());
    }

    /**
     * Return a set of list of colnames (for each input pattern one entry in the list) with filled in repetition indices
     * of the maximum length of all rows. This then is the union of the result of {@link #getColumnPatterns(long)} for
     * all rowIds (in respect to the current {@link ExecutionEnvironment}, of course).
     */
    public Set<List<String>> getMaximumColumnPatterns() {
      Set<List<String>> res = new HashSet<>();
      getColumnPatternsRecursive(MAX_LEN, 0, new ArrayList<>(), res);
      return res;
    }

    /**
     * See {@link #getMaximumColumnPatterns()},but assuming there is only a single pattern, merges the values of the
     * one-element-lists into the set directly.
     */
    public Set<String> getMaximumColumnPatternsSinglePattern() {
      return getMaximumColumnPatterns().stream().flatMap(l -> Stream.of(Iterables.getOnlyElement(l)))
          .collect(Collectors.toSet());
    }

    /**
     * Returns the "length" columns for the column with the given parent indices.
     */
    private QueryableLongColumnShard getLengthColumn(List<Long> indices) throws LengthColumnMissingException {
      StringBuilder sb = new StringBuilder();
      for (int idx = 0; idx < indices.size(); idx++)
        sb.append(
            repeatedColNames.repeatedAtIndex(baseNames.get(longestPatternBaseNameIndex).get(idx), indices.get(idx)));

      sb.append(baseNames.get(longestPatternBaseNameIndex).get(indices.size()));
      String lenColName = repeatedColNames.repeatedLength(sb.toString());
      QueryableLongColumnShard res = lengthColResolver.getLongColumnShard(lenColName);
      if (res == null)
        throw new LengthColumnMissingException("Missing column " + lenColName);
      return res;
    }

    /**
     * Recursively creates all columnNames that are valid for the given row.
     * 
     * @param rowId
     *          The row to receive the lengths from. If {@link #MAX_LEN}, then not the lengths of a specific row will be
     *          used, but the maximum lengths.
     * @param replaceIdx
     *          Index of the [*] whose value should be found in this recursive call. Provide 0 initially.
     * @param rowIndices
     *          List of indices the parent incarnations chose currently, provide empty list initially.
     * @param res
     *          The result. A Set of list where each list is one index possibility for all patterns.
     */
    private void getColumnPatternsRecursive(long rowId, int replaceIdx, List<Long> rowIndices, Set<List<String>> res)
        throws LengthColumnMissingException {
      if (replaceIdx == numberOfStars) {
        List<String> cols = stringProviders.get(rowIndices).stream()
            .map(concatStringProvider -> concatStringProvider.create()).collect(Collectors.toList());
        res.add(cols);
        return;
      }

      long len;
      QueryableLongColumnShard lenCol = getLengthColumn(rowIndices);
      if (rowId != MAX_LEN) {
        long lenColValId = lenCol.resolveColumnValueIdForRow(rowId);
        len = lenCol.getColumnShardDictionary().decompressValue(lenColValId);
      } else
        len = lenCol.getColumnShardDictionary().decompressValue(lenCol.getColumnShardDictionary().getMaxId());

      rowIndices.add(0L);
      for (long repetitionIdx = 0; repetitionIdx < len; repetitionIdx++) {
        rowIndices.set(replaceIdx, repetitionIdx);
        getColumnPatternsRecursive(rowId, replaceIdx + 1, rowIndices, res);
      }
      rowIndices.remove(rowIndices.size() - 1);
    }

    /**
     * Create the {@link ConcatStringProvider}s for the given props.
     * 
     * @param baseNames
     *          List of parts of patterns. Each part is a "baseName" - you can get them by basically splitting the
     *          pattern at '[*]'. For each Pattern, there is a list of base names here.
     * @param fillIdx
     *          The index of the [*] that should be filled in this recursive call. 0 for a start.
     * @param indices
     *          List of indices the parent incarnations of this method created currently, an empty list for a start.
     * @param res
     *          Add results here: For each index combination there is a list of {@link ConcatStringProvider}s (for each
     *          pattern one).
     * @param parentStringProviders
     *          List of the parent String providers of parent incarnations of this recursive method - indexed by the
     *          baseName index (just like the outer list of param baseNames). For a start, use a list that contains
     *          (length(baseNames) "null" values).
     */
    private void createStringProviders(List<List<String>> baseNames, int fillIdx, List<Long> indices,
        Map<List<Long>, List<ConcatStringProvider>> res, List<ConcatStringProvider> parentStringProviders)
            throws LengthColumnMissingException {
      if (fillIdx == numberOfStars) {
        // add the last parts of the patterns to the strings. These parts did not have a [*] appended!
        List<ConcatStringProvider> finalProviders = new ArrayList<>();
        for (int i = 0; i < parentStringProviders.size(); i++) {
          ConcatStringProvider newProvider =
              new ConcatStringProvider(parentStringProviders.get(i), Iterables.getLast(baseNames.get(i)), null);
          finalProviders.add(newProvider);
        }
        res.put(new ArrayList<>(indices), finalProviders);
        return;
      }

      QueryableLongColumnShard lengthCol = getLengthColumn(indices);
      long maxLen =
          lengthCol.getColumnShardDictionary().decompressValue(lengthCol.getColumnShardDictionary().getMaxId());
      indices.add(0L);
      for (long lenIdx = 0; lenIdx < maxLen; lenIdx++) {
        List<ConcatStringProvider> delegateParentStringProviders = new ArrayList<>(parentStringProviders);

        indices.set(indices.size() - 1, lenIdx);
        for (int baseNameIdx = 0; baseNameIdx < baseNames.size(); baseNameIdx++) {
          if (baseNames.get(baseNameIdx).size() - 1 > fillIdx) { // last baseName should not get repeated.
            ConcatStringProvider newStringProvider = new ConcatStringProvider( //
                parentStringProviders.get(baseNameIdx), //
                baseNames.get(baseNameIdx).get(fillIdx), //
                (int) lenIdx);
            delegateParentStringProviders.set(baseNameIdx, newStringProvider);
          }
        }
        createStringProviders(baseNames, fillIdx + 1, indices, res, delegateParentStringProviders);
      }
      indices.remove(indices.size() - 1);
    }

    /**
     * Helper class: Hierarchical string creation with caching of the strings.
     */
    private class ConcatStringProvider {
      private String cachedValue;
      private ConcatStringProvider parent;
      private String baseName;
      private Integer repeatedIdx;

      private ConcatStringProvider(ConcatStringProvider parent, String baseName, Integer repeatedIdx) {
        this.parent = parent;
        this.baseName = baseName;
        this.repeatedIdx = repeatedIdx;
      }

      public String create() {
        if (cachedValue != null)
          return cachedValue;

        StringBuilder sb = new StringBuilder();
        createFill(sb);
        cachedValue = sb.toString();

        return cachedValue;
      }

      protected void createFill(StringBuilder sb) {
        if (cachedValue != null) {
          sb.append(cachedValue);
          return;
        }

        if (parent != null)
          parent.createFill(sb);

        if (repeatedIdx != null)
          sb.append(repeatedColNames.repeatedAtIndex(baseName, repeatedIdx));
        else
          sb.append(baseName);

        cachedValue = sb.toString();
      }
    }
  }
}
