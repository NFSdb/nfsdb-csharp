﻿using System.Linq.Expressions;

namespace Apaf.NFSdb.Core.Queries.Queryable.Expressions
{
    public class ColumnNameExpression: Expression
    {
        public ColumnNameExpression(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }

        public EJournalExpressionType Operation
        {
            get { return EJournalExpressionType.Column; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType) Operation; }
        }

        public override string ToString()
        {
            return Name;
        }
    }
}