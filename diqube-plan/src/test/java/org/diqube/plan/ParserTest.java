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

import java.io.IOException;
import java.io.StringReader;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.diqube.diql.antlr.DiqlLexer;
import org.diqube.diql.antlr.DiqlParser;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;

/**
 *
 * @author Bastian Gloeckle
 */
public class ParserTest {
  public static void main(String[] args) throws IOException {
    ANTLRInputStream input = new ANTLRInputStream(new StringReader("select a from b group by a, b having b=5"));
    DiqlLexer lexer = new DiqlLexer(input);
    lexer.addErrorListener(new BaseErrorListener() {

      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
          String msg, RecognitionException e) {
        throw new RuntimeException("Syntax error at " + line + ":" + charPositionInLine + ": " + msg);
      }

    });
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    DiqlParser parser = new DiqlParser(tokens);
    parser.setBuildParseTree(true);
    parser.addErrorListener(new BaseErrorListener() {

      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
          String msg, RecognitionException e) {
        throw new RuntimeException("Syntax error while parsing: " + msg);
      }

    });
    DiqlStmtContext tree = parser.diqlStmt();
    System.out.println(tree.toStringTree(parser));
  }
}
