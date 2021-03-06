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
package org.diqube.diql.visitors;

import java.util.Arrays;
import java.util.List;

import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.GroupByClauseContext;
import org.diqube.diql.antlr.DiqlParser.SelectStmtContext;
import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FromRequest;
import org.diqube.diql.request.GroupRequest;
import org.diqube.diql.request.OrderRequest;
import org.diqube.diql.request.ResolveValueRequest;
import org.diqube.name.FunctionBasedColumnNameBuilderFactory;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.util.Pair;

/**
 * Starts inspecting a whole Select stmt and produces an {@link ExecutionRequest} that represents all information
 * available in the select stmt.
 *
 * @author Bastian Gloeckle
 */
public class SelectStmtVisitor extends DiqlBaseVisitor<ExecutionRequest> {

  private RepeatedColumnNameGenerator repeatedColNames;
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  public SelectStmtVisitor(RepeatedColumnNameGenerator repeatedColNames,
      FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory) {
    this.repeatedColNames = repeatedColNames;
    this.functionBasedColumnNameBuilderFactory = functionBasedColumnNameBuilderFactory;
  }

  @Override
  public ExecutionRequest visitSelectStmt(SelectStmtContext selectStmt) {
    ExecutionRequest executionRequest = new ExecutionRequest();
    ExecutionRequestVisitorEnvironment env = new ExecutionRequestVisitorEnvironment(executionRequest);

    FromRequest fromRequest = selectStmt.accept(new TableNameVisitor());
    executionRequest.setFromRequest(fromRequest);

    // scan GROUP BY
    Pair<GroupRequest, ComparisonRequest> groupBySteps =
        selectStmt.accept(new GroupByVisitor(env, repeatedColNames, functionBasedColumnNameBuilderFactory));
    if (groupBySteps != null) {
      executionRequest.setGroup(groupBySteps.getLeft());

      if (groupBySteps.getRight() != null)
        executionRequest.setHaving(groupBySteps.getRight());
    }

    // scan WHERE clause
    @SuppressWarnings("unchecked")
    ComparisonRequest restrictions =
        selectStmt.accept(new ComparisonVisitor(env, repeatedColNames, functionBasedColumnNameBuilderFactory,
            // we want to parse the WHERE clause here, so do not visit any sub-tree of GROUP BYs (as that might contain
            // a
            // HAVING
            // clause with additional comparison contexts, but we do not want to visit them here!)
            Arrays.asList(new Class[] { GroupByClauseContext.class })));
    if (restrictions != null)
      executionRequest.setWhere(restrictions);

    // scan order by
    OrderRequest orderSteps =
        selectStmt.accept(new OrderVisitor(env, repeatedColNames, functionBasedColumnNameBuilderFactory));
    if (orderSteps != null)
      executionRequest.setOrder(orderSteps);

    // scan result values
    List<ResolveValueRequest> resultValues =
        selectStmt.accept(new ResultValueVisitor(env, repeatedColNames, functionBasedColumnNameBuilderFactory));
    if (resultValues != null)
      executionRequest.setResolveValues(resultValues);

    return executionRequest;
  }

}
