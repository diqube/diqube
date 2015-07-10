//
// diqube: Distributed Query Base.
//
// Copyright (C) 2015 Bastian Gloeckle
//
// This file is part of diqube.
//
// diqube is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

grammar Diql;

diqlStmt
 : selectStmt
 ;

selectStmt
 : K_SELECT resultValue ( ',' resultValue )*
   K_FROM tableName
   ( K_WHERE comparison )?
   ( groupByClause )?
   ( orderClause )?
 ;

// after the hash signs are 'labels' to make Context objects more readable and Listeners nicer

comparison 
 : '(' comparison ')'                                   # ComparisonRecursive
 | anyValue binaryComparator anyValue                   # ComparisonLeaf
 | comparison K_AND comparison                          # ComparisonAnd  
 | comparison K_OR comparison                           # ComparisonOr
 | K_NOT comparison                                     # ComparisonNot
 ;

function
 : projectionFunctionName '(' ( anyValue ( ',' anyValue )* )? ')'
 | aggregationFunctionName '(' ( anyValue ( ',' anyValue )* )? ')'
 ;

anyValue
 : function 
 | literalValue                                       
 | columnName                                         
// | anyValue ( '*' | '/' ) anyValue                    # Multiplication
// | anyValue ( '+' | '-' ) anyValue                    # Addition
// | '(' anyValue ')'                                    # AnyValueRecursive
 ;

orderTerm
 : anyValue ( K_ASC | K_DESC )?
 ;

orderClause
 : K_ORDER K_BY orderTerm ( ',' orderTerm )* ( limitClause )?
 ;
 
limitClause
 : K_LIMIT decimalLiteralValue ( ',' decimalLiteralValue )?
 ;
 
resultValue
 : anyValue
 ;

groupByClause
 : K_GROUP K_BY columnName ( ',' columnName )*
 // ( K_HAVING comparison )?
 ;

literalValue
 : doubleLiteralValue
 | decimalLiteralValue
 | stringLiteralValue
// | K_NULL
 ;

decimalLiteralValue
 : DECIMAL_LITERAL
 ;
 
stringLiteralValue
 : STRING_LITERAL
 ;
 
doubleLiteralValue
 : DOUBLE_LITERAL
 ;

//unaryOperator
// : '-'
// | '+'
// | '~'
// | K_NOT
// ;
 
//binaryOperator
// : '-'
// | '+'
// ;

binaryComparator
 : '='
 | '<'
 | '>'
 | '<='
 | '>='
 ;
 
//anyFunctionName
// : aggregationFunctionName
// | scalarFunctionName
// ;
 
aggregationFunctionName
 : F_COUNT
 | F_AVG
 ;

projectionFunctionName
 : F_ADD
 | F_ID
 | F_CONCAT
 ;

keyword
 : K_AND
 | K_AS
 | K_ASC
 | K_BY
 | K_DESC
 | K_FROM
 | K_GROUP
 | K_HAVING
 | K_LIMIT
 | K_NOT
 | K_NULL
 | K_OR
 | K_ORDER
 | K_SELECT
 | K_WHERE
 ;

tableName 
 : anyName                                         
 ;

columnName 
 : anyName
 ;
 
anyName
 : ID 
 ;

K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_BY : B Y;
K_DESC : D E S C;
K_FROM : F R O M;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_LIMIT : L I M I T;
K_NOT : N O T;
K_NULL : N U L L;
K_OR : O R;
K_ORDER : O R D E R;
K_SELECT : S E L E C T;
K_WHERE : W H E R E;

F_COUNT: C O U N T;
F_ADD: A D D;
F_ID: I D;
F_CONCAT: C O N C A T;
F_AVG: A V G;

ID
 : [a-zA-Z_] [a-zA-Z_0-9]* 
 ;

DOUBLE_LITERAL
 : DIGIT+ '.' DIGIT*
 | '.' DIGIT+
 ; 

DECIMAL_LITERAL
 : DIGIT+
 ;

STRING_LITERAL
 : '\'' .*? ~('\\') '\''
 ;

SINGLE_LINE_COMMENT
 : '--' .*? '\n' -> skip
 ;

MULTILINE_COMMENT
 : '/*' .*? '*/' -> skip
 ;

SPACES
 : [ \t\n\r] -> skip
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
