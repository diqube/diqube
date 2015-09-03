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
import org.diqube.plan.util.FunctionBasedColumnNameBuilderFactory;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;

/**
 * Parses and {@link AnyValueContext} including references to literal values, column names and aggregate and projection
 * functions and any hierarchy thereof.
 * 
 * <p>
 * Any projection/grouping is added correctly to the ExecutionRequest of the {@link ExecutionRequestVisitorEnvironment}
 * (method {@link ExecutionRequest#getProjectAndAggregate()}) that is specified in the constructor.
 * 
 * <p>
 * The result of this visitor is a pair containing the resulting column or value and a boolean indicating if the result
 * column is an array column (happens when projecting on repeated fields, [*] syntax).
 * 
 * @author Bastian Gloeckle
 */
public class AnyValueVisitor extends DiqlBaseVisitor<Pair<ColumnOrValue, Boolean>> {

  private ExecutionRequestVisitorEnvironment env;
  private RepeatedColumnNameGenerator repeatedColName;
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  public AnyValueVisitor(ExecutionRequestVisitorEnvironment env, RepeatedColumnNameGenerator repeatedColName,
      FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory) {
    this.env = env;
    this.repeatedColName = repeatedColName;
    this.functionBasedColumnNameBuilderFactory = functionBasedColumnNameBuilderFactory;
  }

  @Override
  public Pair<ColumnOrValue, Boolean> visitFunction(FunctionContext ctx) {
    // The output column of the function needs a name. We use a builder to build that.
    FunctionBasedColumnNameBuilder colNameBuilder = functionBasedColumnNameBuilderFactory.create();

    FunctionRequest functionRequest = new FunctionRequest();

    // parse function name
    if (ctx.getChild(0) instanceof ProjectionFunctionNameContext) {
      ProjectionFunctionNameContext projCtx = (ProjectionFunctionNameContext) ctx.getChild(0);
      String functionName = projCtx.getText().toLowerCase();

      functionRequest.setFunctionName(functionName);
      functionRequest.setType(Type.PROJECTION);

      colNameBuilder.withFunctionName(functionName);
    } else if (ctx.getChild(0) instanceof AggregationFunctionNameContext) {
      AggregationFunctionNameContext projCtx = (AggregationFunctionNameContext) ctx.getChild(0);
      String functionName = projCtx.getText().toLowerCase();

      functionRequest.setFunctionName(functionName);
      functionRequest.setType(Type.AGGREGATION_ROW);

      colNameBuilder.withFunctionName(functionName);
    } else
      throw new ParseException("Could not parse function name.");

    env.getExecutionRequest().getProjectAndAggregate().add(functionRequest);

    boolean isArrayInput = false;

    int anyValueChildCnt = 0;
    AnyValueContext anyValueCtx;
    // parse function parameters
    while ((anyValueCtx = ctx.getChild(AnyValueContext.class, anyValueChildCnt++)) != null) {
      Pair<ColumnOrValue, Boolean> childResult = anyValueCtx.accept(this);

      functionRequest.getInputParameters().add(childResult.getLeft());

      if (childResult.getRight())
        isArrayInput = true;

      // add the childs parameter to the name of the column that will be created by executing this function.
      if (childResult.getLeft().getType().equals(ColumnOrValue.Type.COLUMN)) {
        String childColName = childResult.getLeft().getColumnName();
        colNameBuilder.addParameterColumnName(childColName);
      } else {
        Object childValue = childResult.getLeft().getValue();
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

    functionRequest.setOutputColumn(colNameBuilder.build());

    boolean isArrayOutput = false;
    if (isArrayInput) {
      if (functionRequest.getType().equals(Type.PROJECTION)) {
        // projecting on an array input value will result in an array output value, we're projecting over a repeated
        // field.
        functionRequest.setType(Type.REPEATED_PROJECTION);
        isArrayOutput = true;
        // make clear that we're outputting an array value.
        functionRequest
            .setOutputColumn(functionRequest.getOutputColumn() + repeatedColName.allEntriesIdentifyingSubstr());
      } else {
        // aggregation function. We're aggregating over an array, result will be a single value, but we are aggregating
        // over column values.
        functionRequest.setType(Type.AGGREGATION_COL);
      }
    }

    return new Pair<>(new ColumnOrValue(ColumnOrValue.Type.COLUMN, functionRequest.getOutputColumn()), isArrayOutput);
  }

  @Override
  public Pair<ColumnOrValue, Boolean> visitAnyValue(AnyValueContext anyValueCtx) {
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

      return new Pair<>(new ColumnOrValue(ColumnOrValue.Type.LITERAL, value), false);
    } else if (anyValueCtx.getChild(0) instanceof ColumnNameContext) {
      String colName = anyValueCtx.getChild(ColumnNameContext.class, 0).getText();

      boolean isArray = colName.contains(repeatedColName.allEntriesIdentifyingSubstr());

      return new Pair<>(new ColumnOrValue(ColumnOrValue.Type.COLUMN, colName), isArray);
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
