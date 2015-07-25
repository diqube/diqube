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
package org.diqube.plan.visitors;

import java.util.ArrayList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.http.ParseException;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.AnyValueContext;
import org.diqube.diql.antlr.DiqlParser.GroupByClauseContext;
import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.GroupRequest;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;

/**
 * Parses a GROUP BY and its optional HAVING clause. A pair representing those two is returned accordingly.
 * 
 * <p>
 * If aggregations/projections are encountered, they are automatically added to
 * {@link ExecutionRequestVisitorEnvironment#getExecutionRequest()}.
 *
 * @author Bastian Gloeckle
 */
public class GroupByVisitor extends DiqlBaseVisitor<Pair<GroupRequest, ComparisonRequest>> {

  private ExecutionRequestVisitorEnvironment env;

  private RepeatedColumnNameGenerator repeatedColNames;

  public GroupByVisitor(ExecutionRequestVisitorEnvironment env, RepeatedColumnNameGenerator repeatedColNames) {
    this.env = env;
    this.repeatedColNames = repeatedColNames;
  }

  @Override
  public Pair<GroupRequest, ComparisonRequest> visitGroupByClause(GroupByClauseContext groupByCtx) {

    GroupRequest groupRequestRes = new GroupRequest();

    int anyValueCnt = 0;
    AnyValueContext anyValueCtx;
    while ((anyValueCtx = groupByCtx.getChild(AnyValueContext.class, anyValueCnt++)) != null) {
      ColumnOrValue groupBy = anyValueCtx.accept(new AnyValueVisitor(env, repeatedColNames));

      if (!groupBy.getType().equals(ColumnOrValue.Type.COLUMN))
        throw new ParseException("Can only group on columns.");

      groupRequestRes.getGroupColumns().add(groupBy.getColumnName());
    }

    ComparisonRequest havingRequestRes = groupByCtx
        .accept(new ComparisonVisitor(env, repeatedColNames, new ArrayList<Class<? extends ParserRuleContext>>()));

    return new Pair<GroupRequest, ComparisonRequest>(groupRequestRes, havingRequestRes);
  }

  @Override
  protected Pair<GroupRequest, ComparisonRequest> aggregateResult(Pair<GroupRequest, ComparisonRequest> aggregate,
      Pair<GroupRequest, ComparisonRequest> nextResult) {
    if (nextResult == null)
      return aggregate;
    return nextResult;
  }

}
