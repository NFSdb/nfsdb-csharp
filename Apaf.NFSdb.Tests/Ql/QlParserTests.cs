﻿using Apaf.NFSdb.Core.Ql.Gramma;
using System.IO;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using NUnit.Framework;

namespace Apaf.NFSdb.Tests.Ql
{
    [TestFixture]
    public class QlParserTests
    {
        [TestCase("SELECT Timestamp FROM Journal Where Id = 1", Result = "Timestamp From Journal Where (Id Equal 1)")]
        [TestCase("SELECT * FROM Journal Where Id = 1", Result = "From Journal Where (Id Equal 1)")]
        [TestCase("SELECT * FROM Journal Where Timestamp > 1", Result = "From Journal Where (Timestamp GreaterThan 1)")]
        [TestCase("SELECT * FROM Journal Where Id = '09a'", Result = "From Journal Where (Id Equal \"09a\")")]
        [TestCase("SELECT * FROM Journal Where Id = '09a'", Result = "From Journal Where (Id Equal \"09a\")")]
        [TestCase("SELECT * FROM Journal Where Id = '09a' or Timestamp > 1", Result = "From Journal Where ((Id Equal \"09a\") Or (Timestamp GreaterThan 1))")]
        [TestCase("SELECT * FROM Journal Where Id = '09a' and Timestamp > 1", Result = "From Journal Where ((Id Equal \"09a\") And (Timestamp GreaterThan 1))")]
        [TestCase("SELECT * FROM Journal Where Timestamp IN (1, 2, 3)", Result = "From Journal Where Timestamp IN (1, 2, 3)")]
        [TestCase("SELECT * FROM Journal Where Timestamp IN ('1a', '2b')", Result = "From Journal Where Timestamp IN (1a, 2b)")]
        [TestCase("SELECT * FROM Journal Latest By Id Where Timestamp = 1", Result = "From Journal Latest By Id Where (Timestamp Equal 1)")]
        [TestCase("SELECT * FROM Journal Where Id =  @id", Result = "From Journal Where (Id Equal @id)")]
        [TestCase("SELECT * FROM Journal Where Id =  @id Order by Timestamp", Result = "From Journal Where (Id Equal @id) ORDER BY Timestamp")]
        [TestCase("SELECT * FROM Journal Where Id =  @id Order by Timestamp asc", Result = "From Journal Where (Id Equal @id) ORDER BY Timestamp")]
        [TestCase("SELECT * FROM Journal Where Id =  @id Order by Timestamp desc", Result = "From Journal Where (Id Equal @id) ORDER BY Timestamp DESC")]
        [TestCase("SELECT * FROM Journal Where Id = Null", Result = "From Journal Where (Id Equal null)")]
        [TestCase("SELECT * FROM Journal Where Id IS Null", Result = "From Journal Where (Id Equal null)")]
        [TestCase("SELECT * FROM Journal Where Id IS NOT Null", Result = "From Journal Where (Id NotEqual null)")]
        [TestCase("SELECT TOP @top AskSize FROM Journal Where Id IS NOT Null", Result = "Take @top AskSize From Journal Where (Id NotEqual null)")]
        public string Should_parse(string query)
        {
            var input = new AntlrInputStream(query);
            var lexer = new QlLexer(input);
            var commonTokenStream = new CommonTokenStream(lexer);
            var parser = new QlParser(commonTokenStream);

            parser.AddErrorListener(new QlErrorListener());
            parser.BuildParseTree = true;
            IParseTree tree = parser.parse();

            var lis = new QlVisitor();
            return lis.Visit(tree).ToString();
        }

        public Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}