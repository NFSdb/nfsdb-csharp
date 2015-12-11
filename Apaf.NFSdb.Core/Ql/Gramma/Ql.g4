grammar Ql;

parse
 : select_stmt
 ;

error
 : UNEXPECTED_CHAR 
   { 
     throw new Apaf.NFSdb.Core.Ql.Gramma.NFSdbSyntaxException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text); 
   }
 ;

select_stmt
 : select_core ( compound_operator select_core )*
   ( K_ORDER K_BY ordering_term /*( ',' ordering_term )* */ )?
 ;

select_core
 : K_SELECT ( K_TOP expr ( ( K_OFFSET | ',' ) expr )? )? result_column ( ',' result_column )*
   ( K_FROM table_or_subquery )?
   ( K_WHERE where_expr )?
   ( K_GROUP K_BY expr ( ',' expr )* ( K_HAVING expr )? )?
 ;

type_name
 : name+ ( '(' signed_number ')'
         | '(' signed_number ',' signed_number ')' )?
 ;
 
where_expr
 : expr
 ;

/*
    NFSdb understands the following binary operators, in order from highest to
    lowest precedence:

    <    <=   >    >=
    =    ==   IN   NOT IN
    AND
    OR
*/
expr
 : literal_value                                              #LiteralExpr
 | BIND_PARAMETER  										      #ParamExpr
 | ( ( database_name '.' )? table_name '.' )? column_name     #ColumnNameExpr
 | unary_operator expr                                        #UnaryExpr
 | expr op=( '<' | '<=' | '>' | '>=' | '=' | '==' | '!=' | '<>' ) expr       #ComparisonExpr
 | expr op=K_AND expr                                            #LogicalAndExpr
 | expr op=K_OR expr                                             #LogicalOrExpr
 | '(' expr ')'                                               #ParensExpr
 | expr op=K_IN BIND_PARAMETER                                #InParamExpr
 | expr op=K_IN ( '(' expr ( ',' expr )*                
                      ')')                                    #InListExpr
 ;

qualified_table_name
 : table_name 
 ;

ordering_term
 : expr ( K_ASC | K_DESC )?
 ;

common_table_expression
 : table_name ( '(' column_name ( ',' column_name )* ')' )? K_AS '(' select_stmt ')'
 ;

table_or_subquery
 : ( database_name '.' )? table_name ( K_LATEST K_BY column_name )? ( K_AS? table_alias )?      
 ;
 
result_column
 : '*'
 | table_name '.' '*'
 | expr ( K_AS? column_alias )?
 ;
 
compound_operator
 : K_UNION
 | K_UNION K_ALL
 | K_INTERSECT
 | K_EXCEPT
 ;

cte_table_name
 : table_name ( '(' column_name ( ',' column_name )* ')' )?
 ;

signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

literal_value
 : NUMERIC_LITERAL        #NumericLiteral
 | STRING_LITERAL         #StringLiteral
 | K_NULL                 #NullLiteral
 ;

unary_operator
 : '-'
 | '+'
 | '~'
 | K_NOT
 ;

error_message
 : STRING_LITERAL
 ;

column_alias
 : IDENTIFIER
 | STRING_LITERAL
 ;

keyword
 : K_ALL
 | K_AND
 | K_AS
 | K_ASC
 | K_BETWEEN
 | K_BY
 | K_CASE
 | K_CAST
 | K_COLLATE
 | K_CROSS
 | K_CURRENT_DATE
 | K_CURRENT_TIME
 | K_CURRENT_TIMESTAMP
 | K_DESC
 | K_DISTINCT
 | K_ELSE
 | K_END
 | K_ESCAPE
 | K_EXCEPT
 | K_EXISTS
 | K_EXPLAIN
 | K_FOR
 | K_FROM
 | K_FULL
 | K_GROUP
 | K_HAVING
 | K_IN
 | K_INDEXED
 | K_INNER
 | K_INTERSECT
 | K_IS
 | K_ISNULL
 | K_JOIN
 | K_LEFT
 | K_LIKE
 | K_TOP
 | K_LATEST
 | K_MATCH
 | K_NATURAL
 | K_NOT
 | K_NOTNULL
 | K_NULL
 | K_OFFSET
 | K_ON
 | K_OR
 | K_ORDER
 | K_OUTER
 | K_REGEXP
 | K_SELECT
 | K_THEN
 | K_UNION
 | K_USING
 | K_VALUES
 | K_WHEN
 | K_WHERE
 ;

// TODO check all names below

name
 : any_name
 ;

function_name
 : any_name
 ;

database_name
 : any_name
 ;

table_name 
 : any_name
 ;

column_name 
 : any_name
 ;

collation_name 
 : any_name
 ;

table_alias 
 : any_name
 ;

any_name
 : IDENTIFIER 
 | keyword
 | STRING_LITERAL
 | '(' any_name ')'
 ;

SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
ASSIGN : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
TILDE : '~';
PIPE2 : '||';
DIV : '/';
MOD : '%';
LT2 : '<<';
GT2 : '>>';
AMP : '&';
PIPE : '|';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

// http://www.sqlite.org/lang_keywords.html
K_ALL : A L L;
K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_BETWEEN : B E T W E E N;
K_BY : B Y;
K_CASE : C A S E;
K_CAST : C A S T;
K_COLLATE : C O L L A T E;
K_CROSS : C R O S S;
K_CURRENT_DATE : C U R R E N T '_' D A T E;
K_CURRENT_TIME : C U R R E N T '_' T I M E;
K_CURRENT_TIMESTAMP : C U R R E N T '_' T I M E S T A M P;
K_DESC : D E S C;
K_DISTINCT : D I S T I N C T;
K_ELSE : E L S E;
K_END : E N D;
K_ESCAPE : E S C A P E;
K_EXCEPT : E X C E P T;
K_EXISTS : E X I S T S;
K_EXPLAIN : E X P L A I N;
K_FOR : F O R;
K_FROM : F R O M;
K_FULL : F U L L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_IN : I N;
K_INDEXED : I N D E X E D;
K_INNER : I N N E R;
K_INTERSECT : I N T E R S E C T;
K_IS : I S;
K_ISNULL : I S N U L L;
K_JOIN : J O I N;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_TOP : T O P;
K_LATEST : L A T E S T;
K_MATCH : M A T C H;
K_NATURAL : N A T U R A L;
K_NOT : N O T;
K_NOTNULL : N O T N U L L;
K_NULL : N U L L;
K_OFFSET : O F F S E T;
K_ON : O N;
K_OR : O R;
K_ORDER : O R D E R;
K_OUTER : O U T E R;
K_REGEXP : R E G E X P;
K_SELECT : S E L E C T;
K_THEN : T H E N;
K_UNION : U N I O N;
K_USING : U S I N G;
K_VALUES : V A L U E S;
K_WHEN : W H E N;
K_WHERE : W H E R E;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | '`' (~'`' | '``')* '`'
 | '[' ~']'* ']'
 | [a-zA-Z_] [a-zA-Z_0-9]* 
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

BIND_PARAMETER
 : [@] IDENTIFIER
 ;

STRING_LITERAL
 : '\'' op=( ~'\'' | '\'\'' )* '\''
 ;

BLOB_LITERAL
 : X STRING_LITERAL
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
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
