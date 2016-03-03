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
package org.diqube.ui.websocket.request.commands.analysis.util;

import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.ParseException;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.visitors.SelectStmtVisitor;
import org.diqube.name.FunctionBasedColumnNameBuilderFactory;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.ui.analysis.QueryBuilder;
import org.diqube.ui.analysis.QueryBuilder.QueryBuilderException;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.result.analysis.QueryInfoJsonResult;
import org.diqube.util.Pair;

/**
 * Builds {@link QueryInfoJsonResult}.
 *
 * @author Bastian Gloeckle
 */
public class QueryInfoJsonResultBuilder {
  private UiAnalysis analysis;
  private UiQuery query;
  private UiSlice slice;
  private String queryId;
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  public QueryInfoJsonResultBuilder(RepeatedColumnNameGenerator repeatedColumnNameGenerator,
      FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory) {
    this.repeatedColumnNameGenerator = repeatedColumnNameGenerator;
    this.functionBasedColumnNameBuilderFactory = functionBasedColumnNameBuilderFactory;
  }

  public QueryInfoJsonResultBuilder withQueryId(String queryId) {
    this.queryId = queryId;
    return this;
  }

  public QueryInfoJsonResultBuilder withAnalysis(UiAnalysis analysis) {
    this.analysis = analysis;
    return this;
  }

  public QueryInfoJsonResultBuilder withQuery(UiQuery query) {
    this.query = query;
    return this;
  }

  public QueryInfoJsonResultBuilder withSlice(UiSlice slice) {
    this.slice = slice;
    return this;
  }

  /**
   * @throws RuntimeException
   *           If anything is invalid.
   */
  public QueryInfoJsonResult build() throws RuntimeException {
    QueryBuilder queryBuilder = new QueryBuilder();
    queryBuilder.withAnalysis(analysis);
    queryBuilder.withQuery(query);
    queryBuilder.withSlice(slice);

    String queryString;
    try {
      queryString = queryBuilder.build();
    } catch (QueryBuilderException e) {
      throw new RuntimeException("Query invalid: " + e.getMessage(), e);
    }

    DiqlStmtContext ctx;
    try {
      ctx = DiqlParseUtil.parseWithAntlr(queryString);
    } catch (ParseException e) {
      throw new RuntimeException("Query invalid: " + e.getMessage(), e);
    }
    ExecutionRequest executionRequest =
        ctx.accept(new SelectStmtVisitor(repeatedColumnNameGenerator, functionBasedColumnNameBuilderFactory));

    String orderColName;
    boolean orderAsc;
    boolean isOrderedBySingleCol =
        executionRequest.getOrder() != null && executionRequest.getOrder().getColumns().size() == 1;

    if (isOrderedBySingleCol) {
      Pair<String, Boolean> p = executionRequest.getOrder().getColumns().iterator().next();
      orderColName = p.getLeft();
      orderAsc = p.getRight();
    } else {
      // not valid, since isOrderedBySingleCol is false!
      orderColName = "";
      orderAsc = false;
    }

    return new QueryInfoJsonResult(queryId, queryString, isOrderedBySingleCol, orderColName, orderAsc);
  }
}
