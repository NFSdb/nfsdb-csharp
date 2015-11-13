using System.Linq;
using System.Linq.Expressions;
using Antlr4.Runtime;
using Apaf.NFSdb.Core.Exceptions;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public class QlVisitor : QlBaseVisitor<Expression>
    {
        public override Expression VisitSelect_stmt(QlParser.Select_stmtContext context)
        {
            return Visit(context.GetChild(0));
        }

        public override Expression VisitSelect_core(QlParser.Select_coreContext context)
        {
            var expFrom = context.GetRuleContext<QlParser.Table_or_subqueryContext>(0);
            var expWhere = context.GetRuleContext<QlParser.Where_exprContext>(0);
            return new FilterExpression(Visit(expWhere), Visit(expFrom));
        }

        public override Expression VisitTable_or_subquery(QlParser.Table_or_subqueryContext context)
        {
            var table = Visit(context.GetRuleContext<QlParser.Table_nameContext>(0));
            var latestBy = context.GetRuleContext<QlParser.Column_nameContext>(0);
            if (latestBy == null)
            {
                return table;
            }
            return new LatestBySymbolExpression(latestBy.GetText(), table);
        }

        public override Expression VisitTable_name(QlParser.Table_nameContext context)
        {
            return new JournalNameExpression(context.GetText());
        }

        public override Expression VisitComparisonExpr(QlParser.ComparisonExprContext context)
        {
            switch (context.op.Text)
            {
                case ">":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThan, GetRight(context));
                case ">=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThanOrEqual, GetRight(context));
                case "<":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThan, GetRight(context));
                case "<=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThanOrEqual, GetRight(context));
                case "=":
                case "==":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.Equal, GetRight(context));
                case "<>":
                case "!=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.NotEqual, GetRight(context));
                default:
                    context.AddErrorNode(context.op);
                    throw new NFSdbSyntaxException(string.Format("invalid comparison '{0}'", context.op.Text), 
                        context.start.Line, context.start.Column);
            }
        }

        public override Expression VisitInListExpr(QlParser.InListExprContext context)
        {
            var columnName = context.GetRuleContext<QlParser.ColumnNameExprContext>(0);
            if (columnName != null)
            {
                var columnExpr = Visit(columnName);
                var literals = context.GetRuleContexts<QlParser.LiteralExprContext>();
                if (literals != null)
                {
                    var values = literals.Select(l => ((ConstantExpression) Visit(l)).Value).ToList();
                    return new SymbolContainsExpression(columnExpr, values);
                }
            }
            throw new NFSdbSyntaxException(string.Format("invalid expression '{0}'", context.GetText()),
                context.start.Line, context.start.StartIndex);

        }

        public override Expression VisitLogicalAndExpr(QlParser.LogicalAndExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.And, GetRight(context));
        }
        
        public override Expression VisitLogicalOrExpr(QlParser.LogicalOrExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.Or, GetRight(context));
        }

        public override Expression VisitColumnNameExpr(QlParser.ColumnNameExprContext context)
        {
            return new ColumnNameExpression(context.GetText());
        }

        public override Expression VisitStringLiteral(QlParser.StringLiteralContext context)
        {
            string text = context.GetText();
            if (text.Length < 2 || text[0] != '\'' || text[text.Length - 1] != '\'')
            {
                throw new NFSdbSyntaxException(string.Format("invalid string literal '{0}'", text), 
                    context.start.Line, context.start.Column);
            }
            return Expression.Constant(text.Substring(1, text.Length - 2));
        }

        public override Expression VisitNumericLiteral(QlParser.NumericLiteralContext context)
        {
            int value;
            string text = context.GetText();
            if (!int.TryParse(text, out value))
            {
                throw new NFSdbSyntaxException(string.Format("invalid numeric literal '{0}'",text),
                    context.start.Line, context.start.Column);
            }
            return Expression.Constant(value);
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