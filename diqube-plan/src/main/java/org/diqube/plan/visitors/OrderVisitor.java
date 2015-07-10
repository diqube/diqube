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

import org.antlr.v4.runtime.tree.TerminalNode;
import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.AnyValueContext;
import org.diqube.diql.antlr.DiqlParser.DecimalLiteralValueContext;
import org.diqube.diql.antlr.DiqlParser.LimitClauseContext;
import org.diqube.diql.antlr.DiqlParser.OrderClauseContext;
import org.diqube.diql.antlr.DiqlParser.OrderTermContext;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.request.OrderRequest;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.ColumnOrValue.Type;
import org.diqube.util.Pair;

/**
 * Visits 'ORDER BY' clause and returns a corresponding {@link OrderRequest}.
 * 
 * <p>
 * If aggregations/projections are encountered, they are automatically added to
 * {@link ExecutionRequestVisitorEnvironment#getExecutionRequest()}.
 * 
 * @author Bastian Gloeckle
 */
public class OrderVisitor extends DiqlBaseVisitor<OrderRequest> {

  public static final int IDX_ASC_OR_DESC = 0;
  public static final int IDX_LIMIT = 1;
  public static final int IDX_LIMIT_START = 2;
  public static final long SORT_ASC = 0;
  public static final long SORT_DESC = 1;

  private ExecutionRequestVisitorEnvironment env;

  public OrderVisitor(ExecutionRequestVisitorEnvironment env) {
    this.env = env;
  }

  @Override
  public OrderRequest visitOrderClause(OrderClauseContext orderClauseCtx) {
    OrderRequest res = new OrderRequest();

    // check if there is a limit clause
    LimitClauseContext limitClauseCtx = orderClauseCtx.getChild(LimitClauseContext.class, 0);
    if (limitClauseCtx != null) {
      long limit;
      try {
        limit = Long.parseLong(limitClauseCtx.getChild(DecimalLiteralValueContext.class, 0).getText());
      } catch (NumberFormatException e) {
        throw new ParseException("Could not parse limit value.");
      }
      res.setLimit(limit);
      DecimalLiteralValueContext startCtx = limitClauseCtx.getChild(DecimalLiteralValueContext.class, 1);
      if (startCtx != null) {
        long limitStart;
        try {
          limitStart = Long.parseLong(startCtx.getText());
        } catch (NumberFormatException e) {
          throw new ParseException("Could not parse limit start value.");
        }
        res.setLimitStart(limitStart);
      }
    }

    // check all columns that we should order by
    int termPos = 0;
    OrderTermContext termCtx = null;
    while ((termCtx = orderClauseCtx.getChild(OrderTermContext.class, termPos++)) != null) {
      AnyValueContext anyValueContext = termCtx.getChild(AnyValueContext.class, 0);
      ColumnOrValue anyValueResult = anyValueContext.accept(new AnyValueVisitor(env));

      if (anyValueResult.getType().equals(Type.LITERAL))
        throw new ParseException("Ordering by literal values is not supported. "
            + "Please use a column directly or a function based on a column to order by.");

      String colName = anyValueResult.getColumnName();
      boolean isAscending = true;

      // check last terminal node if it is 'desc'.
      for (int childPos = termCtx.getChildCount() - 1; childPos >= 0; childPos--)
        if (termCtx.getChild(childPos) instanceof TerminalNode) {
          String terminalText = ((TerminalNode) termCtx.getChild(childPos)).getText();
          if (terminalText != null && terminalText.toLowerCase().equals("desc"))
            isAscending = false;
          break;
        }

      res.getColumns().add(new Pair<String, Boolean>(colName, isAscending));
    }

    return res;
  }

  @Override
  protected OrderRequest aggregateResult(OrderRequest aggregate, OrderRequest nextResult) {
    if (aggregate == null)
      return nextResult;
    return aggregate;
  }
}
