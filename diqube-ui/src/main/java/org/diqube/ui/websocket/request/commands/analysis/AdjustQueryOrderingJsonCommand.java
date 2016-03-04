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
package org.diqube.ui.websocket.request.commands.analysis;

import javax.inject.Inject;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.ParseException;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.QueryBuilder;
import org.diqube.ui.analysis.QueryBuilder.QueryBuilderException;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.TableJsonResult;
import org.diqube.ui.websocket.result.analysis.QueryJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Changes the ORDER BY clause of a query.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QueryJsonResult} (the new query).
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = AdjustQueryOrderingJsonCommand.NAME)
public class AdjustQueryOrderingJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "adjustQueryOrdering";

  @JsonProperty
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @TypeScriptProperty
  public String queryId;

  /**
   * ORDER BY this "request".
   * 
   * This is not the name of a column, but the "request string" of a column/a projected or aggregated column that can
   * directly be used in the ORDER: Take the value from {@link TableJsonResult#columnRequests} instead of
   * {@link TableJsonResult#columnNames}.
   */
  @JsonProperty
  @TypeScriptProperty
  public String orderByRequest;

  @JsonProperty
  @TypeScriptProperty
  public boolean orderAsc;

  @JsonIgnore
  @Inject
  private AnalysisFactory factory;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);

    if (qube == null)
      throw new RuntimeException("Qube not found: " + qubeId);

    UiQuery query = qube.getQuery(queryId);
    if (query == null)
      throw new RuntimeException("Query not found: " + queryId);

    UiSlice slice = analysis.getSlice(qube.getSliceId());
    if (slice == null)
      throw new RuntimeException("Slice not found: " + qube.getSliceId());

    // since we cannot first parse a query, adjust the parsed one and then generate a query string out of it again, we
    // do some string replacement here... The parser would've removed all the whitespace etc., too, so the query would
    // look pretty ugly after a parser-based replacement.

    String diql = query.getDiql();
    String diqlLower = diql.toLowerCase();

    // search correct "ORDER BY" clause - note that the substring "order by" could appear in content strings (e.g. as
    // constant parameter to a function that is ordered by).

    boolean hasOrderByClause = false;
    int curIdx = diqlLower.lastIndexOf("order by");
    while (curIdx != -1) {
      // try to parse order clause here.
      try {
        DiqlParseUtil.parseOrderClauseWithAntlr(diql.substring(curIdx));
        hasOrderByClause = true;
        break;
      } catch (ParseException e) {
        // no valid order by clause. swallow.
      }

      curIdx = diqlLower.lastIndexOf("order by", curIdx);
    }

    String newQueryDiql;

    if (!hasOrderByClause)
      newQueryDiql = diql;
    else
      newQueryDiql = diql.substring(0, curIdx); // ORDER BY clause is at the end

    newQueryDiql += "ORDER BY " + orderByRequest + ((orderAsc) ? " ASC" : " DESC");

    // validate that we have a valid query!
    try {
      UiQuery newQuery = factory.createQuery(queryId, query.getName(), newQueryDiql, query.getDisplayType());
      QueryBuilder queryBuilder = new QueryBuilder();
      String newFinalQuery = queryBuilder.withAnalysis(analysis).withQuery(newQuery).withSlice(slice).build();

      DiqlParseUtil.parseWithAntlr(newFinalQuery);
    } catch (QueryBuilderException | ParseException e) {
      throw new RuntimeException("Cannot automatically adjust the query: " + e.getMessage(), e);
    }

    query.setDiql(newQueryDiql);

    return () -> resultHandler.sendData(new QueryJsonResult(query,
        // Analysis version is updated by the time this Runnable is called.
        analysis.getVersion()));
  }

}
