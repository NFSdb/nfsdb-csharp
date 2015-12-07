using System;
using System.Linq;
using System.Linq.Expressions;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Apaf.NFSdb.Core.Queries.Queryable;
using Apaf.NFSdb.Core.Queries.Queryable.Expressions;

namespace Apaf.NFSdb.Core.Ql.Gramma
{
    public class QlVisitor : QlBaseVisitor<QlExpression>
    {
        private const string KEYWORD_TOP = "TOP";
        private const string KEYWORD_SKIP = "OFFSET";
        private const string KEYWORD_ORDER_BY = "ORDER";
        private const string KEYWORD_DESC = "DESC";

        public override QlExpression VisitSelect_stmt(QlParser.Select_stmtContext context)
        {
            QlExpression result = null;
            int orderByIter = 0;
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                var selectContext = child as QlParser.Select_coreContext;
                if (selectContext != null)
                {
                    result = Visit(selectContext);
                }
                var terminal = child as TerminalNodeImpl;
                if (terminal != null && terminal.GetText().StartsWith(KEYWORD_ORDER_BY, StringComparison.OrdinalIgnoreCase))
                {
                    if (i < context.ChildCount - 1)
                    {
                        var orderByExpr = context.GetChild<QlParser.Ordering_termContext>(orderByIter++);
                        result = VisitOrderBy(result, orderByExpr);
                    }
                }
            }
            return result;
        }

        public override QlExpression VisitSelect_core(QlParser.Select_coreContext context)
        {
            QlExpression take = null;
            QlToken takeToken = null;
            QlExpression skip = null;
            QlToken skipToken = null;
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                var terminal = child as TerminalNodeImpl;
                if (terminal != null)
                {
                    if (string.Equals(terminal.GetText(), KEYWORD_TOP, StringComparison.OrdinalIgnoreCase))
                    {
                        if (i < context.ChildCount - 1)
                        {
                            takeToken = context.ToQlToken();
                            take = Visit(context.GetChild(++i));
                        }
                    }
                    if (string.Equals(terminal.GetText(), KEYWORD_SKIP, StringComparison.OrdinalIgnoreCase))
                    {
                        if (i < context.ChildCount - 1)
                        {
                            skipToken = context.ToQlToken();
                            skip = Visit(context.GetChild(++i));
                        }
                    }
                }
            }

            var expFrom = context.GetRuleContext<QlParser.Table_or_subqueryContext>(0);
            var expWhere = context.GetRuleContext<QlParser.Where_exprContext>(0);
            var filter = new FilterExpression(Visit(expWhere), Visit(expFrom), context.ToQlToken());
            QlExpression result = filter;
            if (skip != null)
            {
                result = new SliceExpression(result, EJournalExpressionType.Skip, skip, skipToken);
            }
            if (take != null)
            {
                result = new SliceExpression(result, EJournalExpressionType.Take, take, takeToken);
            }
            return result;
        }

        public override QlExpression VisitTable_or_subquery(QlParser.Table_or_subqueryContext context)
        {
            var table = Visit(context.GetRuleContext<QlParser.Table_nameContext>(0));
            var latestBy = context.GetRuleContext<QlParser.Column_nameContext>(0);
            if (latestBy == null)
            {
                return table;
            }
            return new LatestBySymbolExpression(latestBy.GetText(), table, context.ToQlToken());
        }

        public override QlExpression VisitTable_name(QlParser.Table_nameContext context)
        {
            return new JournalNameExpression(context.GetText(), context.ToQlToken());
        }

        public override QlExpression VisitComparisonExpr(QlParser.ComparisonExprContext context)
        {
            switch (context.op.Text)
            {
                case ">":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThan, GetRight(context), context.ToQlToken());
                case ">=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.GreaterThanOrEqual, GetRight(context), context.ToQlToken());
                case "<":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThan, GetRight(context), context.ToQlToken());
                case "<=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.LessThanOrEqual, GetRight(context), context.ToQlToken());
                case "=":
                case "==":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.Equal, GetRight(context), context.ToQlToken());
                case "<>":
                case "!=":
                    return new ComparisonExpression(GetLeft(context), ExpressionType.NotEqual, GetRight(context), context.ToQlToken());
                default:
                    context.AddErrorNode(context.op);
                    throw new NFSdbSyntaxException(string.Format("invalid comparison '{0}'", context.op.Text), 
                        context.start.Line, context.start.Column);
            }
        }

        public override QlExpression VisitInParamExpr(QlParser.InParamExprContext context)
        {
            var columnName = context.GetRuleContext<QlParser.ColumnNameExprContext>(0);
            if (columnName != null)
            {
                var columnExpr = Visit(columnName);
                var param = context.GetChild(2);
                var paramText = param.GetText();
                return new SymbolContainsExpression(columnExpr, GetParameterExpression(context, paramText));
            }
            throw new NFSdbSyntaxException(string.Format("Column name cannot be parsed from 'IN' expression '{0}'", context.GetText()),
                context.start.Line, context.start.StartIndex);
        }

        public override QlExpression VisitInListExpr(QlParser.InListExprContext context)
        {
            var columnName = context.GetRuleContext<QlParser.ColumnNameExprContext>(0);
            if (columnName != null)
            {
                var columnExpr = Visit(columnName);
                var literalContexts = context.GetRuleContexts<QlParser.LiteralExprContext>();
                var paramContexts = context.GetRuleContexts<QlParser.ParamExprContext>();
                var litrals = literalContexts.Select(l => (LiteralExpression)Visit(l));
                var paramms = paramContexts.Select(l => (ParameterNameExpression)Visit(l)).ToList();

                if (paramms.Any())
                {
                    var listExpr = new ValueListExpression(paramms.Cast<Expression>().Concat(litrals).ToList(), 
                        context.ToQlToken());
                    return new SymbolContainsExpression(columnExpr, listExpr, context.ToQlToken());
                }
                else
                {
                    var values = litrals.Select(l => l.Constant.Value).ToList();
                    return new SymbolContainsExpression(columnExpr, Expression.Constant(values), context.ToQlToken());
                }
            }
            throw new NFSdbSyntaxException(string.Format("invalid expression '{0}'", context.GetText()),
                context.start.Line, context.start.StartIndex);

        }

        public override QlExpression VisitParamExpr(QlParser.ParamExprContext context)
        {
            var parameterText = context.GetText();
            return GetParameterExpression(context, parameterText);
        }

        private static QlExpression GetParameterExpression(ParserRuleContext context, string parameterText)
        {
            if (string.IsNullOrEmpty(parameterText) || parameterText[0] != '@' ||
                parameterText.Length < 2)
            {
                throw new NFSdbSyntaxException(string.Format("invalid expression '{0}'", parameterText),
                    context.start.Line, context.start.StartIndex);
            }
            return new ParameterNameExpression(parameterText.Substring(1), context.ToQlToken());
        }

        public override QlExpression VisitLogicalAndExpr(QlParser.LogicalAndExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.And, GetRight(context), context.ToQlToken());
        }
        
        public override QlExpression VisitLogicalOrExpr(QlParser.LogicalOrExprContext context)
        {
            return new ComparisonExpression(GetLeft(context), ExpressionType.Or, GetRight(context), context.ToQlToken());
        }

        public override QlExpression VisitColumnNameExpr(QlParser.ColumnNameExprContext context)
        {
            return new ColumnNameExpression(context.GetText(), context.ToQlToken());
        }

        public override QlExpression VisitStringLiteral(QlParser.StringLiteralContext context)
        {
            string text = context.GetText();
            if (text.Length < 2 || text[0] != '\'' || text[text.Length - 1] != '\'')
            {
                throw new NFSdbSyntaxException(string.Format("invalid string literal '{0}'", text), 
                    context.start.Line, context.start.Column);
            }
            return new LiteralExpression(Expression.Constant(text.Substring(1, text.Length - 2)), context.ToQlToken());
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
            return new LiteralExpression(Expression.Constant(value), context.ToQlToken());
        }

        private Expression GetRight(ParserRuleContext context)
        {
            return Visit(context.GetRuleContext<QlParser.ExprContext>(1));
        }

        private Expression GetLeft(ParserRuleContext context)
        {
            return Visit(context.GetRuleContext<QlParser.ExprContext>(0));
        }

        private QlExpression VisitOrderBy(QlExpression result, QlParser.Ordering_termContext orderByExpr)
        {
            var column = orderByExpr.GetChild<QlParser.ExprContext>(0);
            var direction = orderByExpr.GetChild<TerminalNodeImpl>(0);
            if (direction != null && string.Equals(direction.GetText(), KEYWORD_DESC, StringComparison.OrdinalIgnoreCase))
            {
                return new OrderExpression(result, EJournalExpressionType.OrderByDescending, Visit(column), orderByExpr.ToQlToken());
            }
            return new OrderExpression(result, EJournalExpressionType.OrderBy, Visit(column), orderByExpr.ToQlToken());
        }
    }
}