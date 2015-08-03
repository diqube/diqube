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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.util.Pair;

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

  private String allPattern;

  @PostConstruct
  public void initialize() {
    allPattern = repeatedColNames.allEntriesIdentifyingSubstr();
  }

  /**
   * Replaces all the [*] strings in the pattern with actual column indices.
   * 
   * @param env
   *          The {@link ExecutionEnvironment} that should be used to find "length" columns of the repeated fields.
   * @param pattern
   *          The column name pattern
   * @param lengthProvider
   *          Function to find out how many objects should be assumed a column contains. Parameter to this method is the
   *          "length" column (= the column of a repeated column that contains the number of elements in that repeated
   *          column for each row). Callers can e.g. resolve the number of elements for a specific rowId in
   *          "lengthProvider" or they could simply take the "max" value of the whole column to make this method return
   *          a list of all possible column names that could be valid for all rows in the table.
   * @return Set of string, the final column names based on the lengths provided by the lengthProvider. If there is no
   *         [*] the result will simply contain the pattern.
   * @throws LengthColumnMissingException
   *           in case a "length" column for one of the fields that are marked with "[*]" in the input pattern is not
   *           available in the provided env.
   */
  public Set<String> findColNamesForColNamePattern(ExecutionEnvironment env, String pattern,
      Function<QueryableLongColumnShard, Long> lengthProvider) throws LengthColumnMissingException {
    List<Pair<String, Integer>> parents = Arrays.asList(new Pair<>("", 0));
    return findColNamesForColNamePattern(env, Arrays.asList(pattern), parents, lengthProvider).stream()
        .map(list -> Iterables.getOnlyElement(list)).collect(Collectors.toSet());
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
   * @param env
   *          The {@link ExecutionEnvironment} that should be used to find "length" columns of the repeated fields.
   * @param lengthProvider
   *          Function to find out how many objects should be assumed to be contained for a column. Parameter to this
   *          method is the "length" column (= the column of a repeated column that contains the number of elements in
   *          that repeated column for each row). Callers can e.g. resolve the number of elements for a specific rowId
   *          in "lengthProvider" or they could simply take the "max" value of the whole column to make this method
   *          return a list of all possible column names that could be valid for all rows in the table.
   * @param patterns
   *          The patterns that should be resolved, adhering to the fact that they follow the same "path" (see above).
   * @param parentColNamesAndStartIndices
   *          While this method is being executed, it will recursively call itself. Then this list contains pairs of the
   *          current column name of a specific pattern (matched by index in list) and the next index in the pattern
   *          where the search for another [*] should start. For an initial call, provide a List with the same amount of
   *          entries as "patterns", each Pair having an empty string and the index 0.
   * @return Set of list of strings. Each list represents one combination of the patterns provided with filled in column
   *         indices. The result can be empty.
   * @throws PatternException
   *           in case the patterns do not repeat on the same "path".
   * @throws LengthColumnMissingException
   *           In case a "length" column for one of the fields that are marked with "[*]" in the input patterns is not
   *           available in the provided env.
   */
  public Set<List<String>> findColNamesForColNamePattern(ExecutionEnvironment env, List<String> patterns,
      Function<QueryableLongColumnShard, Long> lengthProvider) throws PatternException, LengthColumnMissingException {

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
        int lastRepeatedIdx = s.lastIndexOf(allPattern);
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

    List<Pair<String, Integer>> parents =
        patterns.stream().map(pattern -> new Pair<>("", 0)).collect(Collectors.toList());

    return findColNamesForColNamePattern(env, patterns, parents, lengthProvider);
  }

  /**
   * Implementation method doing the work to resolve the [*]s in the patterns.
   * 
   * See {@link #findColNamesForColNamePattern(ExecutionEnvironment, String, Function)} and
   * {@link #findColNamesForColNamePattern(ExecutionEnvironment, List, Function)}.
   * 
   * @param parentColNamesAndStartIndices
   *          While this method is being executed, it will recursively call itself. Then this list contains pairs of the
   *          current column name of a specific pattern (matched by index in list) and the next index in the pattern
   *          where the search for another [*] should start. For an initial call, provide a List with the same amount of
   *          entries as "patterns", each Pair having an empty string and the index 0.
   */
  private Set<List<String>> findColNamesForColNamePattern(ExecutionEnvironment env, List<String> patterns,
      List<Pair<String, Integer>> parentColNamesAndStartIndices,
      Function<QueryableLongColumnShard, Long> lengthProvider) throws PatternException, LengthColumnMissingException {
    Set<List<String>> res = new HashSet<>();

    @SuppressWarnings("unchecked")
    List<Pair<String, Integer>> newParentColNamesBase = Arrays.asList(new Pair[parentColNamesAndStartIndices.size()]);

    // walk over all the patterns/parentColNamesAndStartIndices and identify (1) columns where still a [*] needs to be
    // resolved and (2) if no more [*] need to be resolved for a pattern, find the final column name and put it into
    // newParentColNamesBase.

    // map from pair of "parentColName" (= the input to this method including a trailing . if needed) and "baseName" =
    // the name of the next repeated field, relative to "parentColName" to a list of pattern-indices (index in patterns
    // and parentColNames) where that combination of parentColName and baseName needs to be resolved next.
    Map<Pair<String, String>, List<Integer>> repeatedNamesToIndex = new HashMap<>();
    for (int patternIdx = 0; patternIdx < patterns.size(); patternIdx++) {
      String pattern = patterns.get(patternIdx);
      String parentColName = parentColNamesAndStartIndices.get(patternIdx).getLeft();
      int startIdx = parentColNamesAndStartIndices.get(patternIdx).getRight();

      if (startIdx == Integer.MAX_VALUE) {
        // this is a final columns name.
        newParentColNamesBase.set(patternIdx, parentColNamesAndStartIndices.get(patternIdx));
        continue;
      }

      int idx = pattern.indexOf(allPattern, startIdx);
      if (idx == -1) {
        // for this pattern there are no more substitutions needed, so we create a "final" entry in parentColNames

        StringBuilder sb = new StringBuilder();
        sb.append(parentColName);
        if (startIdx < pattern.length())
          // if the pattern contains some substring /after/ the last [*] then we simply append it here, too.
          sb.append(pattern.substring(startIdx));

        String finalColName = sb.toString();

        newParentColNamesBase.set(patternIdx, new Pair<String, Integer>(finalColName, Integer.MAX_VALUE));
        continue;
      }

      // we found an additional [*] in the pattern.
      String baseName;
      if (startIdx == 0)
        baseName = pattern.substring(startIdx, idx);
      else
        baseName = pattern.substring(startIdx + 1 /* skip previous . */, idx);

      if (!parentColName.equals(""))
        parentColName += ".";

      Pair<String, String> newColPair = new Pair<>(parentColName, baseName);

      // store pair of parentColName and baseName in repeatedNamesToIndex
      if (!repeatedNamesToIndex.containsKey(newColPair))
        repeatedNamesToIndex.put(newColPair, new ArrayList<>());
      repeatedNamesToIndex.get(newColPair).add(patternIdx);
    }

    if (repeatedNamesToIndex.size() > 1)
      throw new PatternException("Pattern resolves to different paths, which is not supported currently.");

    // if we have any more [*] that were found, resolve them recursively. We typically expect only one entry in
    // repeatedNamesToIndex, otherwise we would follow different "paths".
    for (Pair<String, String> newColNamePair : repeatedNamesToIndex.keySet()) {
      String parentColName = newColNamePair.getLeft();
      String baseName = newColNamePair.getRight();

      String lenColName = repeatedColNames.repeatedLength(parentColName + baseName);
      QueryableLongColumnShard lenCol = env.getLongColumnShard(lenColName);
      if (lenCol == null)
        throw new LengthColumnMissingException("Column " + lenColName + " not available.");

      long len = lengthProvider.apply(lenCol);

      // we now have the length of that field that we need to iterate over, so lets do that.
      for (long fieldIndex = 0; fieldIndex < len; fieldIndex++) {
        List<Pair<String, Integer>> newParentColNames = new ArrayList<>(newParentColNamesBase);
        String curColName = parentColName + repeatedColNames.repeatedAtIndex(baseName, fieldIndex);
        int newStartIdx = parentColName.length() + baseName.length() + allPattern.length();

        for (int patternIdx : repeatedNamesToIndex.get(newColNamePair))
          newParentColNames.set(patternIdx, new Pair<>(curColName, newStartIdx));

        // as we have only one path to follow (= only one entry in repeatedNamesToIndex) we now have all indice in
        // newParentColNames set, so we can recurse deeper to resolve the next indices/create the final result.
        res.addAll(findColNamesForColNamePattern(env, patterns, newParentColNames, lengthProvider));
      }
    }

    if (repeatedNamesToIndex.isEmpty()) {
      // we did not find any more [*] before, so we have our final result available in newParentColNamesBase, transform
      // that into the final result representation.
      List<String> finalColNames = newParentColNamesBase.stream().map(p -> p.getLeft()).collect(Collectors.toList());
      res = new HashSet<>(Arrays.asList(finalColNames));
    }

    return res;
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
}
