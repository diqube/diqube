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
package org.diqube.diql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.diqube.diql.antlr.DiqlLexer;
import org.diqube.diql.antlr.DiqlParser;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;

/**
 * Utility that can parse a diql string into ANTLR objects.
 *
 * @author Bastian Gloeckle
 */
public class DiqlParseUtil {

  public static DiqlStmtContext parseWithAntlr(String diql) throws ParseException {
    // TODO #20 make ANTLR provide better error messages, show error if not the while diql was parsed (e.g. WHERE after
    // ORDER).
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
}
