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

import org.diqube.diql.antlr.DiqlBaseVisitor;
import org.diqube.diql.antlr.DiqlParser.AnyNameContext;
import org.diqube.diql.antlr.DiqlParser.TableNameContext;

/**
 * Visits the name of a table and returns it.
 *
 * @author Bastian Gloeckle
 */
public class TableNameVisitor extends DiqlBaseVisitor<String> {

  @Override
  public String visitTableName(TableNameContext ctx) {
    return ctx.getChild(AnyNameContext.class, 0).getText();
  }

  @Override
  protected String aggregateResult(String aggregate, String nextResult) {
    // this visitor may visit a TerminalNode after the tableName node. We do not want to overwrite the result value with
    // the value of the terminal node (== null).
    if (nextResult != null && aggregate == null)
      return nextResult;
    return aggregate;
  }

}