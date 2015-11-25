using System.Linq;
using System.Linq.Expressions;
using Antlr4.Runtime;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public class QlVisitor : QlBaseVisitor<QlExpression>
    {
        public override QlExpression VisitSelect_stmt(QlParser.Select_stmtContext context)
        {
            return Visit(context.GetChild(0));
        }

        public override QlExpression VisitSelect_core(QlParser.Select_coreContext context)
        {
            var expFrom = context.GetRuleContext<QlParser.Table_or_subqueryContext>(0);
            var expWhere = context.GetRuleContext<QlParser.Where_exprContext>(0);
            return new FilterExpression(Visit(expWhere), Visit(expFrom), context.Start.ToQlToken());
        }

        public override QlExpression VisitTable_or_subquery(QlParser.Table_or_subqueryContext context)
        {
            var table = Visit(context.GetRuleContext<QlParser.Table_nameContext>(0));
            var latestBy = context.GetRuleContext<QlParser.Column_nameContext>(0);
            if (latestBy == null)
            {
                return table;
            }
            return new LatestBySymbolExpression(latestBy.GetText(), table, context.Start.ToQlToken());
        }

        public override QlExpression VisitTable_name(QlParser.Table_nameContext context)
        {
            return new JournalNameExpression(context.GetText(), context.Start.ToQlToken());
        }

        public override QlExpression VisitComparisonExpr(QlParser.ComparisonExprContext context)
        {
            switch (context.op.Text)
            {
                case ">":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThan, GetRight(context), GetToken(context));
                case ">=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThanOrEqual, GetRight(context), GetToken(context));
                case "<":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThan, GetRight(context), GetToken(context));
                case "<=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThanOrEqual, GetRight(context), GetToken(context));
                case "=":
                case "==":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.Equal, GetRight(context), GetToken(context));
                case "<>":
                case "!=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.NotEqual, GetRight(context), GetToken(context));
                default:
                    context.AddErrorNode(context.op);
                    throw new NFSdbSyntaxException(string.Format("invalid comparison '{0}'", context.op.Text), 
                        context.start.Line, context.start.Column);
            }
        }

        private QlToken GetToken(ParserRuleContext context)
        {
            return new QlToken(context.GetText(), context.Start.Line, context.Start.StartIndex);
        }

        public override QlExpression VisitInListExpr(QlParser.InListExprContext context)
        {
            var columnName = context.GetRuleContext<QlParser.ColumnNameExprContext>(0);
            if (columnName != null)
            {
                var columnExpr = Visit(columnName);
                var literals = context.GetRuleContexts<QlParser.LiteralExprContext>();
                if (literals != null)
                {
                    var values = literals.Select(l => ((LiteralExpression) Visit(l)).Constant.Value).ToList();
                    return new SymbolContainsExpression(columnExpr, values, context.Start.ToQlToken());
                }
            }
            throw new NFSdbSyntaxException(string.Format("invalid expression '{0}'", context.GetText()),
                context.start.Line, context.start.StartIndex);

        }

        public override QlExpression VisitParamExpr(QlParser.ParamExprContext context)
        {
            var parameterText = context.GetText();
            if (string.IsNullOrEmpty(parameterText) || parameterText[0] != '@' ||
                parameterText.Length < 2)
            {
                throw new NFSdbSyntaxException(string.Format("invalid expression '{0}'", parameterText),
                    context.start.Line, context.start.StartIndex);
            }
            return new ParameterNameExpression(parameterText.Substring(1), context.start.ToQlToken());
        }

        public override QlExpression VisitLogicalAndExpr(QlParser.LogicalAndExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.And, GetRight(context), context.Start.ToQlToken());
        }
        
        public override QlExpression VisitLogicalOrExpr(QlParser.LogicalOrExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.Or, GetRight(context), context.Start.ToQlToken());
        }

        public override QlExpression VisitColumnNameExpr(QlParser.ColumnNameExprContext context)
        {
            return new ColumnNameExpression(context.GetText(), context.Start.ToQlToken());
        }

        public override QlExpression VisitStringLiteral(QlParser.StringLiteralContext context)
        {
            string text = context.GetText();
            if (text.Length < 2 || text[0] != '\'' || text[text.Length - 1] != '\'')
            {
                throw new NFSdbSyntaxException(string.Format("invalid string literal '{0}'", text), 
                    context.start.Line, context.start.Column);
            }
            return new LiteralExpression(Expression.Constant(text.Substring(1, text.Length - 2)), context.Start.ToQlToken());
        }

        public override QlExpression VisitNumericLiteral(QlParser.NumericLiteralContext context)
        {
            int value;
            string text = context.GetText();
            if (!int.TryParse(text, out value))
            {
                throw new NFSdbSyntaxException(string.Format("invalid numeric literal '{0}'",text),
                    context.start.Line, context.start.Column);
            }
            return new LiteralExpression(Expression.Constant(value), context.Start.ToQlToken());
        }

        private Expression GetRight(ParserRuleContext context)
        {
            return Visit(context.GetRuleContext<QlParser.ExprContext>(1));
        }

        private Expression GetLeft(ParserRuleContext context)
        {
            return Visit(context.GetRuleContext<QlParser.ExprContext>(0));
        }
    }
}