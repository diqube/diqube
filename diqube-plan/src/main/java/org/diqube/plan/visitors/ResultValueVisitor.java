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
import java.util.Arrays;
import java.util.List;

import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.ResultValueContext;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.request.ResolveValueRequest;
import org.diqube.plan.util.FunctionBasedColumnNameBuilderFactory;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.ColumnOrValue.Type;

/**
 * Visits {@link ResultValueContext}s and returns a {@link ResolveValueRequest} object for each selection.
 * 
 * <p>
 * If aggregations/projections are encountered, they are automatically added to
 * {@link ExecutionRequestVisitorEnvironment#getExecutionRequest()}.
 * 
 * @author Bastian Gloeckle
 */
public class ResultValueVisitor extends DiqlBaseVisitor<List<ResolveValueRequest>> {

  private ExecutionRequestVisitorEnvironment env;

  private RepeatedColumnNameGenerator repeatedColNames;

  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  public ResultValueVisitor(ExecutionRequestVisitorEnvironment env, RepeatedColumnNameGenerator repeatedColNames,
      FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory) {
    this.env = env;
    this.repeatedColNames = repeatedColNames;
    this.functionBasedColumnNameBuilderFactory = functionBasedColumnNameBuilderFactory;
  }

  @Override
  public List<ResolveValueRequest> visitResultValue(ResultValueContext ctx) {
    ColumnOrValue anyValueResult =
        ctx.accept(new AnyValueVisitor(env, repeatedColNames, functionBasedColumnNameBuilderFactory)).getLeft();

    if (anyValueResult.getType().equals(Type.LITERAL)) {
      // TODO #19 support selecting literal values - currently use id() function
      throw new ParseException("Not implemented: selecting only literal values.");
    }

    ResolveValueRequest res = new ResolveValueRequest();
    res.setResolve(anyValueResult);

    return new ArrayList<>(Arrays.asList(new ResolveValueRequest[] { res }));
  }

  @Override
  protected List<ResolveValueRequest> aggregateResult(List<ResolveValueRequest> aggregate,
      List<ResolveValueRequest> nextResult) {
    if (aggregate == null)
      return nextResult;
    if (nextResult == null)
      return aggregate;
    aggregate.addAll(nextResult);
    return aggregate;
  }
}
