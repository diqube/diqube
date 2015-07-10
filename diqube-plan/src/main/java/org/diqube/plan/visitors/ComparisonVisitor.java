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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.RuleNode;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.AnyValueContext;
import org.diqube.diql.antlr.DiqlParser.BinaryComparatorContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonAndContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonLeafContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonNotContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonOrContext;
import org.diqube.diql.antlr.DiqlParser.ComparisonRecursiveContext;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.Not;
import org.diqube.plan.request.ComparisonRequest.Operator;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.ColumnOrValue.Type;

/**
 * Visits comparisons, which may be in a WHERE clause or a HAVING clause and returns one {@link ComparisonRequest} that
 * represents the whole comparison block (that means the {@link ComparisonRequest} might point to other transitive
 * {@link ComparisonRequest} objects etc.).
 * 
 * <p>
 * If aggregations/projections are encountered, they are automatically added to
 * {@link ExecutionRequestVisitorEnvironment#getExecutionRequest()}.
 *
 * @author Bastian Gloeckle
 */
public class ComparisonVisitor extends DiqlBaseVisitor<ComparisonRequest> {

  /**
   * When switching the operators of a comparison, the operator needs to be adjusted. This map maps from original
   * operator to the operator that should be used when the operands are exchanged.
   */
  private static final Map<Operator, Operator> exchangeOperandOperators;

  private ExecutionRequestVisitorEnvironment env;
  private List<Class<? extends ParserRuleContext>> stopParsingForContexts;

  /**
   * @param stopParsingForContexts
   *          Context classes which will force this visitor to stop visiting any sub-tree of that context.
   */
  public ComparisonVisitor(ExecutionRequestVisitorEnvironment env,
      List<Class<? extends ParserRuleContext>> stopParsingForContexts) {
    this.env = env;
    this.stopParsingForContexts = stopParsingForContexts;
  }

  @Override
  public ComparisonRequest visitComparisonLeaf(ComparisonLeafContext comparisonCtx) {
    ComparisonRequest.Leaf res = new ComparisonRequest.Leaf();

    AnyValueContext firstAny = comparisonCtx.getChild(AnyValueContext.class, 0);
    AnyValueContext secondAny = comparisonCtx.getChild(AnyValueContext.class, 1);

    ColumnOrValue firstOperand = firstAny.accept(new AnyValueVisitor(env));
    ColumnOrValue secondOperand = secondAny.accept(new AnyValueVisitor(env));

    BinaryComparatorContext comparator = comparisonCtx.getChild(BinaryComparatorContext.class, 0);
    Operator operator = null;
    if (comparator.getText().equals("="))
      operator = Operator.EQ;
    else if (comparator.getText().equals(">="))
      operator = Operator.GT_EQ;
    else if (comparator.getText().equals(">"))
      operator = Operator.GT;
    else if (comparator.getText().equals("<="))
      operator = Operator.LT_EQ;
    else if (comparator.getText().equals("<"))
      operator = Operator.LT;

    // TODO check if both operands only select from literals and implement short-cut.
    if (firstOperand.getType().equals(Type.LITERAL) && secondOperand.getType().equals(Type.LITERAL))
      throw new ParseException("Comparisons are currently supported only with at least one side being a column.");

    if (firstOperand.getType().equals(Type.LITERAL)) {
      // exchange to make sure firstOperandPair contains a column, secondOperandPair contains the literal value
      ColumnOrValue tmp = firstOperand;
      firstOperand = secondOperand;
      secondOperand = tmp;
      operator = exchangeOperandOperators.get(operator);
    }

    res.setLeft(firstOperand);
    res.setRight(secondOperand);
    res.setOp(operator);

    return res;
  }

  @Override
  protected ComparisonRequest aggregateResult(ComparisonRequest aggregate, ComparisonRequest nextResult) {
    if (aggregate == null)
      return nextResult;
    return aggregate;
  }

  @Override
  public ComparisonRequest visitComparisonOr(ComparisonOrContext ctx) {
    return andOrOr(ctx, true);
  }

  @Override
  public ComparisonRequest visitComparisonAnd(ComparisonAndContext ctx) {
    return andOrOr(ctx, false);
  }

  @Override
  public ComparisonRequest visitComparisonNot(ComparisonNotContext ctx) {
    ComparisonRequest child = ctx.getChild(ComparisonContext.class, 0).accept(this);

    ComparisonRequest.Not res = new Not();
    res.setChild(child);

    return res;
  }

  private ComparisonRequest andOrOr(ComparisonContext ctx, boolean isOr) {
    ComparisonRequest leftChild = ctx.getChild(ComparisonContext.class, 0).accept(this);
    ComparisonRequest rightChild = ctx.getChild(ComparisonContext.class, 1).accept(this);

    ComparisonRequest.DelegateComparisonRequest res;

    if (isOr)
      res = new ComparisonRequest.Or();
    else
      res = new ComparisonRequest.And();

    res.setLeft(leftChild);
    res.setRight(rightChild);

    return res;
  }

  @Override
  public ComparisonRequest visitComparisonRecursive(ComparisonRecursiveContext ctx) {
    return ctx.getChild(ComparisonContext.class, 0).accept(this);
  }

  @Override
  protected boolean shouldVisitNextChild(RuleNode node, ComparisonRequest currentResult) {
    for (Class<? extends ParserRuleContext> stopClass : stopParsingForContexts) {
      if (stopClass.isInstance(node))
        return false;
    }
    return true;
  }

  static {
    exchangeOperandOperators = new HashMap<>();
    exchangeOperandOperators.put(Operator.EQ, Operator.EQ);
    exchangeOperandOperators.put(Operator.GT, Operator.LT);
    exchangeOperandOperators.put(Operator.GT_EQ, Operator.LT_EQ);
    exchangeOperandOperators.put(Operator.LT, Operator.GT);
    exchangeOperandOperators.put(Operator.LT_EQ, Operator.GT_EQ);
  }

}
