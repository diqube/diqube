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

import java.util.Arrays;
import java.util.List;

import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.GroupByClauseContext;
import org.diqube.diql.antlr.DiqlParser.SelectStmtContext;
import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ExecutionRequest;
import org.diqube.plan.request.GroupRequest;
import org.diqube.plan.request.OrderRequest;
import org.diqube.plan.request.ResolveValueRequest;
import org.diqube.util.Pair;

/**
 * Starts inspecting a whole Select stmt and produces an {@link ExecutionRequest} that represents all information
 * available in the select stmt.
 *
 * @author Bastian Gloeckle
 */
public class SelectStmtVisitor extends DiqlBaseVisitor<ExecutionRequest> {

  private RepeatedColumnNameGenerator repeatedColNames;

  public SelectStmtVisitor(RepeatedColumnNameGenerator repeatedColNames) {
    this.repeatedColNames = repeatedColNames;
  }

  @Override
  public ExecutionRequest visitSelectStmt(SelectStmtContext selectStmt) {
    ExecutionRequest executionRequest = new ExecutionRequest();
    ExecutionRequestVisitorEnvironment env = new ExecutionRequestVisitorEnvironment(executionRequest);

    String tableName = selectStmt.accept(new TableNameVisitor());
    executionRequest.setTableName(tableName);

    // scan GROUP BY
    Pair<GroupRequest, ComparisonRequest> groupBySteps = selectStmt.accept(new GroupByVisitor(env, repeatedColNames));
    if (groupBySteps != null) {
      executionRequest.setGroup(groupBySteps.getLeft());

      if (groupBySteps.getRight() != null)
        executionRequest.setHaving(groupBySteps.getRight());
    }

    // scan WHERE clause
    ComparisonRequest restrictions = selectStmt.accept(new ComparisonVisitor(env, repeatedColNames,
        // we want to parse the WHERE clause here, so do not visit any sub-tree of GROUP BYs (as that might contain a
        // HAVING
        // clause with additional comparison contexts, but we do not want to visit them here!)
        Arrays.asList(new Class[] { GroupByClauseContext.class })));
    if (restrictions != null)
      executionRequest.setWhere(restrictions);

    // scan order by
    OrderRequest orderSteps = selectStmt.accept(new OrderVisitor(env, repeatedColNames));
    if (orderSteps != null)
      executionRequest.setOrder(orderSteps);

    // scan result values
    List<ResolveValueRequest> resultValues = selectStmt.accept(new ResultValueVisitor(env, repeatedColNames));
    if (resultValues != null)
      executionRequest.setResolveValues(resultValues);

    return executionRequest;
  }

}
