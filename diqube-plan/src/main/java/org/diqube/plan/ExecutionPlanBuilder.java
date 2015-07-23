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
package org.diqube.plan;

import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.diqube.diql.antlr.DiqlLexer;
import org.diqube.diql.antlr.DiqlParser;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.OrderedRowIdConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironmentFactory;
import org.diqube.execution.steps.FilterRequestedColumnsAndActiveRowIdsStep;
import org.diqube.execution.steps.HavingResultStep;
import org.diqube.execution.steps.OrderStep;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.exception.ValidationException;
import org.diqube.plan.optimizer.ExecutionRequestOptimizer;
import org.diqube.plan.request.ExecutionRequest;
import org.diqube.plan.visitors.SelectStmtVisitor;

/**
 * Build an {@link ExecutablePlan} from diql.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionPlanBuilder {
  private String diql;

  private ExecutionPlannerFactory executionPlannerFactory;

  private ColumnValueConsumer finalColumnValueConsumer;

  private OrderedRowIdConsumer finalOrderedRowIdConsumer;

  private ExecutionEnvironmentFactory executionEnvironmentFactory;

  private OverwritingRowIdConsumer havingResultsConsumer;

  public ExecutionPlanBuilder(ExecutionPlannerFactory executionPlannerFactory,
      ExecutionEnvironmentFactory executionEnvironmentFactory) {
    this.executionPlannerFactory = executionPlannerFactory;
    this.executionEnvironmentFactory = executionEnvironmentFactory;
  }

  public ExecutionPlanBuilder fromDiql(String diql) {
    this.diql = diql;
    return this;
  }

  public ExecutionPlanBuilder withFinalColumnValueConsumer(ColumnValueConsumer finalColumnValueConsumer) {
    this.finalColumnValueConsumer = finalColumnValueConsumer;
    return this;
  }

  public ExecutionPlanBuilder withFinalOrderedRowIdConsumer(OrderedRowIdConsumer finalOrderedRowIdConsumer) {
    this.finalOrderedRowIdConsumer = finalOrderedRowIdConsumer;
    return this;
  }

  public ExecutionPlanBuilder withHavingResultConsumer(OverwritingRowIdConsumer havingResultsConsumer) {
    this.havingResultsConsumer = havingResultsConsumer;
    return this;
  }

  /**
   * @return An {@link ExecutablePlan} that is executable on the query master right away.
   */
  public ExecutablePlan build() throws ParseException, ValidationException {
    DiqlStmtContext sqlStmt = parseWithAntlr();
    ExecutionRequest executionRequest = sqlStmt.accept(new SelectStmtVisitor());

    executionRequest = new ExecutionRequestOptimizer(executionRequest).optimize();

    Map<String, PlannerColumnInfo> colInfo =
        new PlannerColumnInfoBuilder().withExecutionRequest(executionRequest).build();

    ExecutionEnvironment queryMasterDefaultExecutionEnvironment =
        executionEnvironmentFactory.createQueryMasterExecutionEnvironment();

    new ExecutionPlanValidator().validate(executionRequest, colInfo);

    ExecutablePlan plan = executionPlannerFactory.createExecutionPlanner().plan(executionRequest, colInfo,
        queryMasterDefaultExecutionEnvironment);

    // wire manual consumers
    for (ExecutablePlanStep step : plan.getSteps()) {
      if (finalColumnValueConsumer != null && (step instanceof FilterRequestedColumnsAndActiveRowIdsStep))
        step.addOutputConsumer(finalColumnValueConsumer);

      if (finalOrderedRowIdConsumer != null && (step instanceof OrderStep))
        step.addOutputConsumer(finalOrderedRowIdConsumer);

      if (havingResultsConsumer != null && (step instanceof HavingResultStep))
        step.addOutputConsumer(havingResultsConsumer);
    }

    return plan;
  }

  private DiqlStmtContext parseWithAntlr() throws ParseException {
    // TODO #20 make ANTLR provide better error messages
    ANTLRInputStream input = new ANTLRInputStream(diql.toCharArray(), diql.length());
    DiqlLexer lexer = new DiqlLexer(input);
    lexer.addErrorListener(new BaseErrorListener() {

      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
          String msg, RecognitionException e) {
        throw new ParseException("Syntax error (" + line + ":" + charPositionInLine + "): " + msg);
      }

    });
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    DiqlParser parser = new DiqlParser(tokens);
    parser.setBuildParseTree(true);
    parser.addErrorListener(new BaseErrorListener() {

      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
          String msg, RecognitionException e) {
        throw new ParseException("Syntax error while parsing (" + line + ":" + charPositionInLine + "): " + msg);
      }

    });

    try {
      DiqlStmtContext sqlStmt = parser.diqlStmt();
      return sqlStmt;
    } catch (RecognitionException e) {
      throw new ParseException("Exception while parsing: " + e.getMessage(), e);
    }
  }

  public ColumnValueConsumer getFinalColumnValueConsumer() {
    return finalColumnValueConsumer;
  }

  public OrderedRowIdConsumer getFinalOrderedRowIdConsumer() {
    return finalOrderedRowIdConsumer;
  }
}
