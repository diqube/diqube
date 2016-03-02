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
   // the "from" clause is optional, as the UI uses this grammar too for parsing partial diql statements. 
   // On diqube-server there has to be a "from" clause, of course.
   ( K_FROM tableName )?
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
 : projectionFunctionName ( anyValue ( ',' anyValue )* )? ')'
 | aggregationFunctionName ( anyValue ( ',' anyValue )* )? ')'
 ;

anyValue
 : columnName 
 | literalValue                                       
 | function                               
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
 : K_LIMIT positiveDecimalLiteralValue ( ',' positiveDecimalLiteralValue )?
 ;
 
resultValue
 : anyValue
 ;

groupByClause
 : K_GROUP K_BY anyValue ( ',' anyValue )* ( K_HAVING comparison )?
 ;

literalValue
 : doubleLiteralValue
 | decimalLiteralValue
 | stringLiteralValue
// | K_NULL
 ;

positiveDecimalLiteralValue
 : POSITIVE_DECIMAL_LITERAL
 ;

decimalLiteralValue
 : POSITIVE_DECIMAL_LITERAL
 | NEGATIVE_DECIMAL_LITERAL
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
 | F_ANY
 | F_MIN
 | F_MAX
 | F_CONCATGROUP
 | F_SUM
 | F_VAR
 | F_SD
 | F_RSD
 ;

projectionFunctionName
 : F_ADD
 | F_ID
 | F_CONCAT
 | F_ROUND
 | F_DIV
 | F_MUL
 | F_SUB
 | F_LOG
 | F_HOST
 | F_TOPLEVELDOMAIN
 | F_STRING
 | F_INT
 | F_LONG
 | F_DOUBLE
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
 : flattenedTableName
 | anyName                                         
 ;

flattenedTableName
 : F_FLATTEN anyName ',' columnName ')'
 ;

columnName 
 : anyName ( '[' ( 'length' | POSITIVE_DECIMAL_LITERAL | '*' ) ']' )?
 | columnName '.' columnName
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

F_COUNT: C O U N T '(';
F_ADD: A D D '(';
F_ID: I D '(';
F_CONCAT: C O N C A T '(';
F_AVG: A V G '(';
F_ROUND: R O U N D '(';
F_ANY: A N Y '(';
F_MIN: M I N '(';
F_MAX: M A X '(';
F_DIV: D I V '(';
F_MUL: M U L '(';
F_SUB: S U B '(';
F_CONCATGROUP: C O N C A T G R O U P '(';
F_SUM: S U M '(';
F_LOG: L O G '(';
F_HOST: H O S T '(';
F_TOPLEVELDOMAIN: T O P L E V E L D O M A I N '(';
F_STRING: S T R I N G '(';
F_INT: I N T '(';
F_LONG: L O N G '(';
F_DOUBLE: D O U B L E '(';
F_VAR: V A R '(';
F_SD: S D '(';
F_RSD: R S D '(';

F_FLATTEN: F L A T T E N '(';

ID
 : [a-zA-Z_] [a-zA-Z_0-9]* 
 ;

DOUBLE_LITERAL
 : '-' DOUBLE_LITERAL
 | DIGIT+ '.' DIGIT*
 | '.' DIGIT+
 ; 

POSITIVE_DECIMAL_LITERAL
 : DIGIT+
 ;

NEGATIVE_DECIMAL_LITERAL
 : '-' POSITIVE_DECIMAL_LITERAL
 ;

STRING_LITERAL
 : '\'\''
 | '\'' .*? ~('\\') '\''
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
