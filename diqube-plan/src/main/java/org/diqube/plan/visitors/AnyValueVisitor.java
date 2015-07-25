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

import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.AggregationFunctionNameContext;
import org.diqube.diql.antlr.DiqlParser.AnyValueContext;
import org.diqube.diql.antlr.DiqlParser.ColumnNameContext;
import org.diqube.diql.antlr.DiqlParser.DecimalLiteralValueContext;
import org.diqube.diql.antlr.DiqlParser.DoubleLiteralValueContext;
import org.diqube.diql.antlr.DiqlParser.FunctionContext;
import org.diqube.diql.antlr.DiqlParser.LiteralValueContext;
import org.diqube.diql.antlr.DiqlParser.ProjectionFunctionNameContext;
import org.diqube.diql.antlr.DiqlParser.StringLiteralValueContext;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.request.ExecutionRequest;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.plan.request.FunctionRequest.Type;
import org.diqube.plan.util.FunctionBasedColumnNameBuilder;
import org.diqube.util.ColumnOrValue;

/**
 * Parses and {@link AnyValueContext} including references to literal values, column names and aggregate and projection
 * functions and any hierarchy thereof.
 * 
 * <p>
 * Any projection/grouping is added correctly to the ExecutionRequest of the {@link ExecutionRequestVisitorEnvironment}
 * (method {@link ExecutionRequest#getProjectAndAggregate()}) that is specified in the constructor.
 * 
 * @author Bastian Gloeckle
 */
public class AnyValueVisitor extends DiqlBaseVisitor<ColumnOrValue> {

  private ExecutionRequestVisitorEnvironment env;
  private RepeatedColumnNameGenerator repeatedColName;

  public AnyValueVisitor(ExecutionRequestVisitorEnvironment env, RepeatedColumnNameGenerator repeatedColName) {
    this.env = env;
    this.repeatedColName = repeatedColName;
  }

  @Override
  public ColumnOrValue visitFunction(FunctionContext ctx) {
    // The output column of the function needs a name. We use a builder to build that.
    FunctionBasedColumnNameBuilder colNameBuilder = new FunctionBasedColumnNameBuilder();

    FunctionRequest projectionRequest = new FunctionRequest();

    boolean isAggregationFunction = false;

    // parse function name
    if (ctx.getChild(0) instanceof ProjectionFunctionNameContext) {
      ProjectionFunctionNameContext projCtx = (ProjectionFunctionNameContext) ctx.getChild(0);
      String functionName = projCtx.getText().toLowerCase();

      projectionRequest.setFunctionName(functionName);
      projectionRequest.setType(Type.PROJECTION);

      colNameBuilder.withFunctionName(functionName);
    } else if (ctx.getChild(0) instanceof AggregationFunctionNameContext) {
      AggregationFunctionNameContext projCtx = (AggregationFunctionNameContext) ctx.getChild(0);
      String functionName = projCtx.getText().toLowerCase();

      projectionRequest.setFunctionName(functionName);
      isAggregationFunction = true;

      colNameBuilder.withFunctionName(functionName);
    } else
      throw new ParseException("Could not parse function name.");

    env.getExecutionRequest().getProjectAndAggregate().add(projectionRequest);

    int anyValueChildCnt = 0;
    AnyValueContext anyValueCtx;
    // parse function parameters
    while ((anyValueCtx = ctx.getChild(AnyValueContext.class, anyValueChildCnt++)) != null) {
      ColumnOrValue childResult = anyValueCtx.accept(this);

      projectionRequest.getInputParameters().add(childResult);

      // add the childs parameter to the name of the column that will be created by executing this function.
      if (childResult.getType().equals(ColumnOrValue.Type.COLUMN))
        colNameBuilder.addParameterColumnName(childResult.getColumnName());
      else {
        if (isAggregationFunction)
          throw new ParseException("Aggregation functions (like " + projectionRequest.getFunctionName()
              + ") only accept column names as parameter.");
        Object childValue = childResult.getValue();
        if (childValue instanceof String)
          colNameBuilder.addParameterLiteralString((String) childValue);
        else if (childValue instanceof Long)
          colNameBuilder.addParameterLiteralLong((Long) childValue);
        else if (childValue instanceof Double)
          colNameBuilder.addParameterLiteralDouble((Double) childValue);
        else
          throw new ParseException("Function parameter did not provide valid literal value.");
      }
    }

    if (isAggregationFunction) {
      Type aggregationType;
      if (projectionRequest.getInputParameters().isEmpty())
        aggregationType = Type.AGGREGATION_ROW; // example "count" - parameterless agg functions are executed on rows!
      else if (projectionRequest.getInputParameters().stream()
          .anyMatch(c -> c.getColumnName().contains(repeatedColName.allEntriesIdentifyingSubstr())))
        aggregationType = Type.AGGREGATION_COL;
      else
        aggregationType = Type.AGGREGATION_ROW;

      projectionRequest.setType(aggregationType);
    }

    projectionRequest.setOutputColumn(colNameBuilder.build());

    return new ColumnOrValue(ColumnOrValue.Type.COLUMN, projectionRequest.getOutputColumn());
  }

  @Override
  public ColumnOrValue visitAnyValue(AnyValueContext anyValueCtx) {
    if (anyValueCtx.getChild(0) instanceof LiteralValueContext) {
      LiteralValueContext literalCtx = anyValueCtx.getChild(LiteralValueContext.class, 0);
      String valueText = literalCtx.getText();

      Object value;
      if (literalCtx.getChild(0) instanceof DecimalLiteralValueContext)
        value = Long.parseLong(valueText);
      else if (literalCtx.getChild(0) instanceof StringLiteralValueContext)
        value = parseStringValue(valueText);
      else if (literalCtx.getChild(0) instanceof DoubleLiteralValueContext)
        value = Double.parseDouble(valueText);
      else
        throw new ParseException("Could not parse literal value at " + literalCtx.toString());

      return new ColumnOrValue(ColumnOrValue.Type.LITERAL, value);
    } else if (anyValueCtx.getChild(0) instanceof ColumnNameContext) {
      String colName = anyValueCtx.getChild(ColumnNameContext.class, 0).getText();

      return new ColumnOrValue(ColumnOrValue.Type.COLUMN, colName);
    } else if (anyValueCtx.getChild(0) instanceof FunctionContext) {
      return visitFunction(anyValueCtx.getChild(FunctionContext.class, 0));
    }
    throw new ParseException("Could not parse AnyValueContext as there were no alternatives left");
  }

  private String parseStringValue(String diql) {
    // each string starts end ends with a single '
    String work = diql.substring(1, diql.length() - 1);

    // un-escape \'
    work = work.replaceAll("\\\\'", "'");

    return work;
  }

}
