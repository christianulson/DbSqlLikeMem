namespace DbSqlLikeMem.Firebird.Test.Parser;

/// <summary>
/// EN: Covers Firebird-specific parser feature behavior.
/// PT-br: Cobre o comportamento de recursos de parser específicos do Firebird.
/// </summary>
public sealed class FirebirdDialectFeatureParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that blank SQL input keeps the existing parser parameter-validation message.
    /// PT-br: Verifica se SQL em branco mantém a mensagem existente de validacao de parametro do parser.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [InlineData(0)]
    [InlineData(4)]
    public void Parse_BlankSql_ShouldProvideParameterValidationMessage(int version)
    {
        var dialect = new FirebirdDialect(version);
        var db = new FirebirdDbMock(version);

        var ex = Assert.Throws<ArgumentException>(() => SqlQueryParser.Parse(" ", db, dialect));

        Assert.Contains("sql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses the supported CREATE FUNCTION and DROP FUNCTION subset.
    /// PT-br: Garante que o Firebird interprete o subset suportado de CREATE FUNCTION e DROP FUNCTION.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseScalarFunctionDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END",
            db, dialect));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal("INT", create.Definition.ReturnTypeSql, ignoreCase: true);
        Assert.Collection(create.Definition.Parameters,
            p => Assert.Equal("baseValue", p.Name, ignoreCase: true),
            p => Assert.Equal("incrementValue", p.Name, ignoreCase: true));

        var drop = Assert.IsType<SqlDropFunctionQuery>(SqlQueryParser.Parse(
            "DROP FUNCTION IF EXISTS fn_users",
            db, dialect));

        Assert.True(drop.IfExists);
        Assert.Equal("fn_users", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird rejects CREATE OR REPLACE FUNCTION because the mock gate stays disabled.
    /// PT-br: Garante que o Firebird rejeite CREATE OR REPLACE FUNCTION porque o gate do mock permanece desabilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCreateOrReplaceScalarFunctionDdlSubset_ShouldReject()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
            "CREATE OR REPLACE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END",
            db, dialect));
        Assert.Contains("CREATE OR REPLACE FUNCTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses scalar function parameter defaults in the supported DDL subset.
    /// PT-br: Garante que o Firebird interprete padroes de parametro de funcao escalar no subset de DDL suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseScalarFunctionParameterDefaultSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT DEFAULT 2) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END",
            db, dialect));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(2, create.Definition.Parameters.Count);
        Assert.True(create.Definition.Parameters[0].Required);
        Assert.False(create.Definition.Parameters[1].Required);
        Assert.Equal("incrementValue", create.Definition.Parameters[1].Name, ignoreCase: true);
        Assert.Equal(2, Convert.ToInt32(create.Definition.Parameters[1].DefaultValue, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird keeps the pipe operator as string concatenation in scalar expressions.
    /// PT-br: Garante que o Firebird mantenha o operador pipe como concatenacao de strings em expressoes escalares.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseScalarPipeConcat_ShouldReturnConcatExpression()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var expr = SqlExpressionParser.ParseScalar("CURRENT_USER || '|' || CURRENT_ROLE", db, dialect);

        var concat = Assert.IsType<BinaryExpr>(expr);
        Assert.Equal(SqlBinaryOp.Concat, concat.Op);
    }

    /// <summary>
    /// EN: Ensures Firebird rejects scalar function defaults that are followed by required parameters.
    /// PT-br: Garante que o Firebird rejeite defaults de funcao escalar seguidos por parametros obrigatorios.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseScalarFunctionDefaultParameterBeforeRequiredSubset_ShouldReject()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(baseValue INT DEFAULT 1, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END",
            db, dialect));

        Assert.Contains("default values must be trailing", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses CREATE OR ALTER PROCEDURE in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete CREATE OR ALTER PROCEDURE no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCreateOrAlterProcedureDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateProcedureQuery>(SqlQueryParser.Parse(
            "CREATE OR ALTER PROCEDURE sp_echo(IN tenantId INT) BEGIN END",
            db, dialect));

        Assert.True(create.OrReplace);
        Assert.Equal("sp_echo", create.Table?.Name, ignoreCase: true);
        Assert.Single(create.Definition.RequiredIn);
        Assert.Empty(create.Definition.OptionalIn);
        Assert.Empty(create.Definition.OutParams);
        Assert.Equal("tenantId", create.Definition.RequiredIn[0].Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses procedure parameter defaults in the supported DDL subset.
    /// PT-br: Garante que o Firebird interprete padroes de parametro de procedure no subset de DDL suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseProcedureParameterDefaultSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateProcedureQuery>(SqlQueryParser.Parse(
            "CREATE OR ALTER PROCEDURE sp_echo(IN tenantId INT = 1) BEGIN END",
            db, dialect));

        Assert.True(create.OrReplace);
        Assert.Equal("sp_echo", create.Table?.Name, ignoreCase: true);
        Assert.Empty(create.Definition.RequiredIn);
        Assert.Single(create.Definition.OptionalIn);
        Assert.Empty(create.Definition.OutParams);
        Assert.False(create.Definition.OptionalIn[0].Required);
        Assert.Equal("tenantId", create.Definition.OptionalIn[0].Name, ignoreCase: true);
        Assert.Equal(1, Convert.ToInt32(create.Definition.OptionalIn[0].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird rejects procedure defaults that are followed by required input parameters.
    /// PT-br: Garante que o Firebird rejeite defaults de procedure seguidos por parametros de entrada obrigatorios.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseProcedureDefaultParameterBeforeRequiredSubset_ShouldReject()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE OR ALTER PROCEDURE sp_echo(IN tenantId INT = 1, IN suffix INT) BEGIN END",
            db, dialect));

        Assert.Contains("default values must be trailing", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses CREATE OR ALTER TRIGGER in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete CREATE OR ALTER TRIGGER no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCreateOrAlterTriggerDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateTriggerQuery>(SqlQueryParser.Parse(
            "CREATE OR ALTER TRIGGER trg_users_ai AFTER INSERT ON users BEGIN END",
            db, dialect));

        Assert.True(create.OrReplace);
        Assert.Equal("trg_users_ai", create.TriggerName, ignoreCase: true);
        Assert.Equal("users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(TableTriggerEvent.AfterInsert, create.Event);
    }

    /// <summary>
    /// EN: Ensures Firebird parses RECREATE PROCEDURE in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete RECREATE PROCEDURE no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseRecreateProcedureDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateProcedureQuery>(SqlQueryParser.Parse(
            "RECREATE PROCEDURE sp_echo(IN tenantId INT, IN suffix INT) BEGIN END",
            db, dialect));

        Assert.True(create.OrReplace);
        Assert.Equal("sp_echo", create.Table?.Name, ignoreCase: true);
        Assert.Collection(create.Definition.RequiredIn,
            _ => { },
            _ => { });
        Assert.Equal("tenantId", create.Definition.RequiredIn[0].Name, ignoreCase: true);
        Assert.Equal("suffix", create.Definition.RequiredIn[1].Name, ignoreCase: true);
        Assert.Empty(create.Definition.OptionalIn);
        Assert.Empty(create.Definition.OutParams);
    }

    /// <summary>
    /// EN: Ensures Firebird parses RECREATE TRIGGER in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete RECREATE TRIGGER no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseRecreateTriggerDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateTriggerQuery>(SqlQueryParser.Parse(
            "RECREATE TRIGGER trg_users_ai BEFORE INSERT ON users BEGIN END",
            db, dialect));

        Assert.True(create.OrReplace);
        Assert.Equal("trg_users_ai", create.TriggerName, ignoreCase: true);
        Assert.Equal("users", create.Table?.Name, ignoreCase: true);
        Assert.True(create.IsBefore);
        Assert.Equal(TableTriggerEvent.BeforeInsert, create.Event);
    }

    /// <summary>
    /// EN: Ensures Firebird parses DROP PROCEDURE in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete DROP PROCEDURE no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseDropProcedureDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var drop = Assert.IsType<SqlDropProcedureQuery>(SqlQueryParser.Parse(
            "DROP PROCEDURE IF EXISTS sp_echo",
            db, dialect));

        Assert.True(drop.IfExists);
        Assert.Equal("sp_echo", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses DROP TRIGGER in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete DROP TRIGGER no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseDropTriggerDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var drop = Assert.IsType<SqlDropTriggerQuery>(SqlQueryParser.Parse(
            "DROP TRIGGER IF EXISTS trg_users_ai",
            db, dialect));

        Assert.True(drop.IfExists);
        Assert.Equal("trg_users_ai", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses ALTER SEQUENCE RESTART in the supported subset.
    /// PT-br: Garante que o Firebird interprete ALTER SEQUENCE RESTART no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseAlterSequenceDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var alter = Assert.IsType<SqlAlterSequenceQuery>(SqlQueryParser.Parse(
            "ALTER SEQUENCE seq_users RESTART WITH 10",
            db, dialect));

        Assert.Equal("seq_users", alter.Table?.Name, ignoreCase: true);
        Assert.Equal(10, alter.RestartWith);
    }

    /// <summary>
    /// EN: Ensures Firebird parses ALTER SEQUENCE RESTART without WITH in the supported subset.
    /// PT-br: Garante que o Firebird interprete ALTER SEQUENCE RESTART sem WITH no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseAlterSequenceRestartSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var alter = Assert.IsType<SqlAlterSequenceQuery>(SqlQueryParser.Parse(
            "ALTER SEQUENCE seq_users RESTART",
            db, dialect));

        Assert.Equal("seq_users", alter.Table?.Name, ignoreCase: true);
        Assert.Null(alter.RestartWith);
    }

    /// <summary>
    /// EN: Ensures Firebird parses CREATE GENERATOR as a sequence creation alias in the supported subset.
    /// PT-br: Garante que o Firebird interprete CREATE GENERATOR como alias de criacao de sequence no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCreateGeneratorAliasSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var create = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.Parse(
            "CREATE GENERATOR seq_users",
            db, dialect));

        Assert.Equal("seq_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(1, create.StartValue);
        Assert.Equal(1, create.IncrementBy);
    }

    /// <summary>
    /// EN: Ensures Firebird rejects MATERIALIZED and NOT MATERIALIZED CTE hints.
    /// PT-br: Garante que o Firebird rejeite hints MATERIALIZED e NOT MATERIALIZED em CTE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [InlineData("WITH x AS MATERIALIZED (SELECT 1 FROM RDB$DATABASE) SELECT 1 FROM x", "WITH ... AS MATERIALIZED")]
    [InlineData("WITH x AS NOT MATERIALIZED (SELECT 1 FROM RDB$DATABASE) SELECT 1 FROM x", "WITH ... AS NOT MATERIALIZED")]
    public void ParseWithCte_MaterializedHint_ShouldReject(string sql, string expectedMessage)
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, dialect));

        Assert.Contains(expectedMessage, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird rejects OWNED BY for sequence DDL even when CREATE SEQUENCE is available.
    /// PT-br: Garante que o Firebird rejeite OWNED BY em DDL de sequence mesmo quando CREATE SEQUENCE estiver disponivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCreateSequence_OwnedBy_ShouldThrowNotSupported()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
            "CREATE SEQUENCE seq_users START WITH 1 INCREMENT BY 1 OWNED BY public.users.id",
            db, dialect));

        Assert.Contains("OWNED BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses DROP GENERATOR as a sequence drop alias in the supported subset.
    /// PT-br: Garante que o Firebird interprete DROP GENERATOR como alias de exclusao de sequence no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseDropGeneratorAliasSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var drop = Assert.IsType<SqlDropSequenceQuery>(SqlQueryParser.Parse(
            "DROP GENERATOR IF EXISTS seq_users",
            db, dialect));

        Assert.True(drop.IfExists);
        Assert.Equal("seq_users", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses ALTER PROCEDURE as a replacing procedure definition.
    /// PT-br: Garante que o Firebird interprete ALTER PROCEDURE como definicao de procedure substituivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseAlterProcedureDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var alter = Assert.IsType<SqlCreateProcedureQuery>(SqlQueryParser.Parse(
            "ALTER PROCEDURE sp_echo(IN tenantId INT, IN suffix INT) BEGIN END",
            db, dialect));

        Assert.True(alter.OrReplace);
        Assert.Equal("sp_echo", alter.Table?.Name, ignoreCase: true);
        Assert.Collection(alter.Definition.RequiredIn,
            p => Assert.Equal("tenantId", p.Name, ignoreCase: true),
            p => Assert.Equal("suffix", p.Name, ignoreCase: true));
    }

    /// <summary>
    /// EN: Ensures Firebird parses ALTER TRIGGER as a replacing trigger definition.
    /// PT-br: Garante que o Firebird interprete ALTER TRIGGER como definicao de trigger substituivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseAlterTriggerDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var alter = Assert.IsType<SqlCreateTriggerQuery>(SqlQueryParser.Parse(
            "ALTER TRIGGER trg_users_ai AFTER INSERT ON users BEGIN END",
            db, dialect));

        Assert.True(alter.OrReplace);
        Assert.Equal("trg_users_ai", alter.TriggerName, ignoreCase: true);
        Assert.Equal("users", alter.Table?.Name, ignoreCase: true);
        Assert.False(alter.IsBefore);
        Assert.Equal(TableTriggerEvent.AfterInsert, alter.Event);
    }

    /// <summary>
    /// EN: Ensures Firebird parses EXECUTE BLOCK in the supported provider-real subset.
    /// PT-br: Garante que o Firebird interprete EXECUTE BLOCK no subset realista suportado pelo provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseExecuteBlockDdlSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var block = Assert.IsType<SqlExecuteBlockQuery>(SqlQueryParser.Parse(
            """
            EXECUTE BLOCK AS
            BEGIN
                INSERT INTO users (id, name) VALUES (1, 'A');
            END
            """,
            db, dialect));

        Assert.Contains("INSERT INTO users", block.BodySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUES (1, 'A')", block.BodySql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses EXECUTE BLOCK with parameters and RETURNS clauses in the supported subset.
    /// PT-br: Garante que o Firebird interprete EXECUTE BLOCK com parametros e RETURNS no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseExecuteBlockWithParametersAndReturnsSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var block = Assert.IsType<SqlExecuteBlockQuery>(SqlQueryParser.Parse(
            """
            EXECUTE BLOCK (tenantId INT = 1)
            RETURNS (outValue INT)
            AS
            BEGIN
                outValue = tenantId;
            END
            """,
            db, dialect));

        Assert.Single(block.InputParameters);
        Assert.Equal("tenantId", block.InputParameters[0].Name, ignoreCase: true);
        Assert.False(block.InputParameters[0].Required);
        Assert.Equal(1, Convert.ToInt32(block.InputParameters[0].Value, CultureInfo.InvariantCulture));
        Assert.Single(block.ReturnParameters);
        Assert.Equal("outValue", block.ReturnParameters[0].Name, ignoreCase: true);
        Assert.Contains("outValue = tenantId", block.BodySql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses EXECUTE BLOCK bodies that contain SUSPEND statements in the supported subset.
    /// PT-br: Garante que o Firebird interprete corpos de EXECUTE BLOCK que contenham SUSPEND no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseExecuteBlockWithSuspendSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var block = Assert.IsType<SqlExecuteBlockQuery>(SqlQueryParser.Parse(
            """
            EXECUTE BLOCK AS
            BEGIN
                SUSPEND;
                INSERT INTO users (id, name) VALUES (1, 'A');
            END
            """,
            db, dialect));

        Assert.DoesNotContain("SUSPEND", block.BodySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INSERT INTO users", block.BodySql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses EXECUTE BLOCK bodies that stop at EXIT in the supported subset.
    /// PT-br: Garante que o Firebird interprete corpos de EXECUTE BLOCK que param em EXIT no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseExecuteBlockWithExitSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var block = Assert.IsType<SqlExecuteBlockQuery>(SqlQueryParser.Parse(
            """
            EXECUTE BLOCK AS
            BEGIN
                INSERT INTO users (id, name) VALUES (1, 'A');
                EXIT;
                INSERT INTO users (id, name) VALUES (2, 'B');
            END
            """,
            db, dialect));

        Assert.Contains("INSERT INTO users (id, name) VALUES (1, 'A')", block.BodySql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("VALUES (2, 'B')", block.BodySql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses EXECUTE STATEMENT inside EXECUTE BLOCK in the supported subset.
    /// PT-br: Garante que o Firebird interprete EXECUTE STATEMENT dentro de EXECUTE BLOCK no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseExecuteBlockWithExecuteStatementSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var block = Assert.IsType<SqlExecuteBlockQuery>(SqlQueryParser.Parse(
            """
            EXECUTE BLOCK AS
            BEGIN
                EXECUTE STATEMENT 'INSERT INTO users (id, name) VALUES (1, ''A'')';
            END
            """,
            db, dialect));

        Assert.Contains("EXECUTE STATEMENT", block.BodySql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses FIRST and SKIP row limiting in the supported subset.
    /// PT-br: Garante que o Firebird interprete FIRST e SKIP para limite de linhas no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelectFirstSkipRowLimitSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var select = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            """
            SELECT FIRST 2 SKIP 1 Id
            FROM users
            ORDER BY Id
            """,
            db, dialect));

        var limit = Assert.IsType<SqlLimitOffset>(select.RowLimit);
        Assert.IsType<LiteralExpr>(limit.Count).Value.Should().Be(2);
        Assert.IsType<LiteralExpr>(limit.Offset).Value.Should().Be(1);
    }

    /// <summary>
    /// EN: Ensures Firebird parses ROWS range limiting in the supported subset.
    /// PT-br: Garante que o Firebird interprete limite de faixa ROWS no subset suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelectRowsRangeRowLimitSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var select = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            """
            SELECT Id
            FROM users
            ORDER BY Id
            ROWS 2 TO 4
            """,
            db, dialect));

        var limit = Assert.IsType<SqlLimitOffset>(select.RowLimit);
        Assert.NotNull(limit.Count);
        Assert.NotNull(limit.Offset);
    }

    /// <summary>
    /// EN: Ensures Firebird parses ORDER BY NULLS FIRST and NULLS LAST in the supported subset.
    /// PT-br: Garante que o Firebird interprete ORDER BY NULLS FIRST e NULLS LAST no subset suportado.
    /// </summary>
    [Theory]
    [InlineData("SELECT Id FROM users ORDER BY Name NULLS FIRST", true)]
    [InlineData("SELECT Id FROM users ORDER BY Name NULLS LAST", false)]
    [Trait("Category", "Parser")]
    public void ParseSelectOrderByNullsModifierSubset_ShouldParse(string sql, bool expectedNullsFirst)
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var select = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, db, dialect));

        var orderBy = Assert.Single(select.OrderBy);
        orderBy.NullsFirst.Should().Be(expectedNullsFirst);
    }

    /// <summary>
    /// EN: Ensures Firebird parses the supported OVERLAY string function syntax.
    /// PT-br: Garante que o Firebird interprete a sintaxe suportada da funcao de string OVERLAY.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseOverlayStringFunctionSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var select = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT OVERLAY('abcdef' PLACING 'XYZ' FROM 3 FOR 2) FROM RDB$DATABASE",
            db, dialect));

        Assert.Single(select.SelectItems);
        Assert.Contains("OVERLAY", select.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird parses the DATEADD temporal syntax that uses TO and a leading amount.
    /// PT-br: Garante que o Firebird interprete a sintaxe temporal DATEADD que usa TO e uma quantidade inicial.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseDateAddTemporalFunctionSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(
            "DATEADD(1 DAY TO CURRENT_TIMESTAMP)",
            db, dialect));

        Assert.Equal("DATEADD", expr.Name, ignoreCase: true);
        Assert.Equal(3, expr.Args.Count);
        Assert.Equal("DAY", Assert.IsType<RawSqlExpr>(expr.Args[0]).Sql, ignoreCase: true);
        Assert.Equal(1, Convert.ToInt32(Assert.IsType<LiteralExpr>(expr.Args[1]).Value, CultureInfo.InvariantCulture));
        Assert.Equal("CURRENT_TIMESTAMP", Assert.IsType<IdentifierExpr>(expr.Args[2]).Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses the CRYPT_HASH function with the USING hash algorithm syntax.
    /// PT-br: Garante que o Firebird interprete a funcao CRYPT_HASH com a sintaxe de algoritmo USING.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseCryptHashFunctionSubset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(
            "CRYPT_HASH('Firebird' USING SHA256)",
            db, dialect));

        Assert.Equal("CRYPT_HASH", expr.Name, ignoreCase: true);
        Assert.Equal(2, expr.Args.Count);
        Assert.Equal("SHA256", Assert.IsType<RawSqlExpr>(expr.Args[1]).Sql, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Firebird parses the HASH function with the supported CRC32 algorithm syntax.
    /// PT-br: Garante que o Firebird interprete a funcao HASH com a sintaxe suportada do algoritmo CRC32.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseHashFunctionUsingCrc32Subset_ShouldParse()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(
            "HASH('Firebird' USING CRC32)",
            db, dialect));

        Assert.Equal("HASH", expr.Name, ignoreCase: true);
        Assert.Equal(2, expr.Args.Count);
        Assert.Equal("CRC32", Assert.IsType<RawSqlExpr>(expr.Args[1]).Sql, ignoreCase: true);
    }
}
