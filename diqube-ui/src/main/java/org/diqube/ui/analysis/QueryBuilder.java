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
package org.diqube.ui.analysis;

import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.ParseException;
import org.diqube.diql.antlr.DiqlParser.ComparisonContext;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.diql.antlr.DiqlParser.ResultValueContext;
import org.diqube.diql.antlr.DiqlParser.SelectStmtContext;
import org.diqube.diql.antlr.DiqlParser.TableNameContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a full diql query out of information from a {@link UiAnalysis}, {@link UiQuery} and {@link UiSlice}.
 * 
 * <p>
 * The UI splits the lgoic into multiple building blocks: The {@link UiAnalysis} is based on a single table (from-clause
 * for all queries), then the {@link UiSlice} defines the slice of data that is inspected (where-clause for all queries)
 * and the {@link UiQuery} defines the rest of the query. This builder takes these information and builds a final diql
 * string out of the pieces.
 *
 * @author Bastian Gloeckle
 */
public class QueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(QueryBuilder.class);

  private UiAnalysis analysis;
  private UiQuery query;
  private UiSlice slice;

  public QueryBuilder withAnalysis(UiAnalysis analysis) {
    this.analysis = analysis;
    return this;
  }

  public QueryBuilder withQuery(UiQuery query) {
    this.query = query;
    return this;
  }

  public QueryBuilder withSlice(UiSlice slice) {
    this.slice = slice;
    return this;
  }

  /**
   * @return A valid diql string that can be sent to a diqube-server for evaluation.
   * @throws QueryBuilderException
   *           if anything went wrong.
   */
  public String build() throws QueryBuilderException {
    String inputQuery = query.getDiql();

    StringBuilder sb = new StringBuilder();

    DiqlStmtContext diqlStmt;
    try {
      diqlStmt = DiqlParseUtil.parseWithAntlr(inputQuery);
    } catch (ParseException e) {
      throw new QueryBuilderException(e.getMessage(), e);
    }

    SelectStmtContext selectStmt = diqlStmt.getChild(SelectStmtContext.class, 0);
    if (selectStmt.getChild(TableNameContext.class, 0) != null)
      throw new QueryBuilderException("Query contains a FROM clause.");

    int lastResultValueIdx = 0;
    while (selectStmt.getChild(ResultValueContext.class, lastResultValueIdx) != null)
      lastResultValueIdx++;
    lastResultValueIdx--;
    ResultValueContext lastResultValue = selectStmt.getChild(ResultValueContext.class, lastResultValueIdx);

    // copy everything up to the point where we have to insert the "from" (= "select a, b, c").
    sb.append(inputQuery.substring(0, lastResultValue.getStop().getStopIndex() + 1));
    sb.append(" from ");
    sb.append(analysis.getTable());
    sb.append(" ");

    int firstIndexOfRemainingInputString = lastResultValue.getStop().getStopIndex() + 1;

    boolean whereKeyWordAppended = false;
    ComparisonContext whereCtx = selectStmt.getChild(ComparisonContext.class, 0);
    if (whereCtx != null) {
      // the query already contains a "where"!
      sb.append(" where (");
      sb.append(inputQuery.substring(whereCtx.getStart().getStartIndex(), whereCtx.getStop().getStopIndex() + 1));
      sb.append(") ");
      whereKeyWordAppended = true;
      firstIndexOfRemainingInputString = whereCtx.getStop().getStopIndex() + 1;
    }

    if (!slice.getSliceDisjunctions().isEmpty()
        || (slice.getManualConjunction() != null && !"".equals(slice.getManualConjunction()))) {
      if (whereKeyWordAppended)
        sb.append(" and (");
      else
        sb.append(" where (");

      boolean andNeeded = false;
      for (UiSliceDisjunction disjunction : slice.getSliceDisjunctions()) {
        if (!disjunction.getDisjunctionValues().isEmpty()) {
          if (andNeeded)
            sb.append(" and ");
          andNeeded = true;

          sb.append("(");
          boolean firstValue = true;
          for (String disjunctionValue : disjunction.getDisjunctionValues()) {
            if (!firstValue)
              sb.append(" or ");
            firstValue = false;
            sb.append(disjunction.getFieldName());
            sb.append(" = ");
            sb.append(disjunctionValue);
          }
          sb.append(")");
        }
      }

      if (slice.getManualConjunction() != null && !"".equals(slice.getManualConjunction())) {
        if (andNeeded)
          sb.append(" and ");
        sb.append("(");
        sb.append(slice.getManualConjunction());
        sb.append(")");
      }

      sb.append(")");
    }

    sb.append(inputQuery.substring(firstIndexOfRemainingInputString));

    String res = sb.toString();

    logger.info("Created final query for UiQuery {}: {}", query.getId(), res);

    return res;
  }

  public static class QueryBuilderException extends Exception {
    private static final long serialVersionUID = 1L;

    public QueryBuilderException(String msg) {
      super(msg);
    }

    public QueryBuilderException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
