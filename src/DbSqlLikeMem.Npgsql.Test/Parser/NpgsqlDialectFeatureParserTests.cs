namespace DbSqlLikeMem.Npgsql.Test.Parser;

/// <summary>
/// EN: Covers PostgreSQL/Npgsql-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos de PostgreSQL/Npgsql.
/// </summary>
public sealed class NpgsqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING is parsed as duplicate-key handling.
    /// PT: Garante que ON CONFLICT DO NOTHING seja interpretado como tratamento de chave duplicada.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothing_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Empty(ins.OnDupAssigns);
        Assert.True(ins.IsOnConflictDoNothing);
    }

    /// <summary>
    /// EN: Ensures MATERIALIZED CTE syntax is accepted.
    /// PT: Garante que a sintaxe de CTE MATERIALIZED seja aceita.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseWithCte_AsMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE is parsed correctly.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE seja interpretado corretamente.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdate_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Equal("name", ins.OnDupAssigns[0].Col);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE, update WHERE, and RETURNING are parsed together.
    /// PT: Garante que WHERE no alvo do conflito, WHERE do update e RETURNING sejam interpretados em conjunto.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_Returning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Contains("users.id", ins.OnConflictUpdateWhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ins.OnConflictUpdateWhereExpr);
    }

    /// <summary>
    /// EN: Ensures empty ON CONFLICT target list is rejected with actionable message.
    /// PT: Garante que lista vazia no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_EmptyTarget_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT () DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (, id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id,) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target interrupted by semicolon is rejected with actionable message.
    /// PT: Garante que alvo de ON CONFLICT interrompido por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET with empty assignment list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET com lista vazia de atribuições seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetEmptyAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment without expression is rejected with actionable message.
    /// PT: Garante que atribuição sem expressão em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetAssignmentWithoutExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires an expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em ON CONFLICT DO UPDATE SET sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetMissingCommaBetweenAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET accepts semicolon statement boundary after assignment list.
    /// PT: Garante que ON CONFLICT DO UPDATE SET aceite fronteira de statement por ponto e vírgula após lista de atribuições.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetWithSemicolonBoundary_ShouldParse(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Single(parsed.OnDupAssigns);
        Assert.Equal("name", parsed.OnDupAssigns[0].Col);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT without DO branch is rejected with actionable message.
    /// PT: Garante que ON CONFLICT sem ramo DO seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_WithoutDoBranch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires DO NOTHING or DO UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO with invalid continuation is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO com continuação inválida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoInvalidContinuation_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO SKIP";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must be followed by NOTHING or UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE without SET is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE sem SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateWithoutSet_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires SET assignments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE without predicate is rejected with actionable message.
    /// PT: Garante que WHERE no alvo de ON CONFLICT sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que WHERE no alvo de ON CONFLICT finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE; DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without constraint name is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome da constraint seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutName_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires a constraint name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT ... RETURNING captures projection payload in AST for PostgreSQL dialect.
    /// PT: Garante que INSERT ... RETURNING capture o payload de projeção na AST para o dialeto PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name AS user_name";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("user_name", parsed.Returning[1].Alias);
    }


    /// <summary>
    /// EN: Ensures INSERT with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que INSERT com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after INSERT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1),";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuples without comma separator are rejected with actionable message.
    /// PT: Garante que tuplas em INSERT VALUES sem vírgula separadora sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTuplesWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1) (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("separate row tuples with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES , (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id,) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT empty column list is rejected with actionable message.
    /// PT: Garante que lista de colunas vazia no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_EmptyColumnList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users () VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("at least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (,id) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT column list unclosed before semicolon is rejected with actionable message.
    /// PT: Garante que lista de colunas do INSERT não fechada antes de ponto e vírgula seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES empty row tuple is rejected with actionable message.
    /// PT: Garante que linha vazia em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesEmptyRowTuple_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES ()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rejects empty expression between commas inside tuple.
    /// PT: Garante que INSERT VALUES rejeite expressão vazia entre vírgulas dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleMissingExpressionBetweenCommas_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,,2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES rejects trailing comma inside tuple.
    /// PT: Garante que INSERT VALUES rejeite vírgula final dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuple with unclosed parenthesis is rejected with actionable message.
    /// PT: Garante que tupla em INSERT VALUES com parêntese não fechado seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleUnclosedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 2";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES row expression count matches target column count.
    /// PT: Garante que a quantidade de expressões em INSERT VALUES corresponda à quantidade de colunas alvo.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesColumnCountMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("column count", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rows with inconsistent expression counts are rejected with actionable message.
    /// PT: Garante que linhas de INSERT VALUES com cardinalidade inconsistente de expressões sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesRowArityMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 'a'), (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("row 2", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT ... RETURNING is parsed without consuming RETURNING as SELECT tail.
    /// PT: Garante que INSERT ... SELECT ... RETURNING seja interpretado sem consumir RETURNING como cauda do SELECT.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_SelectReturning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) SELECT id, name FROM users RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT ... WHERE ... RETURNING preserves WHERE boundary and captures RETURNING projection.
    /// PT: Garante que INSERT ... SELECT ... WHERE ... RETURNING preserve o limite do WHERE e capture a projeção de RETURNING.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_SelectWhereReturning_ShouldPreserveWhereBoundary(int version)
    {
        const string sql = "INSERT INTO users (id, name) SELECT id, name FROM users WHERE id IN (1, 2) RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.NotNull(parsed.InsertSelect!.Where);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING keeps WHERE boundary and captures returning projection.
    /// PT: Garante que UPDATE ... RETURNING preserve o limite do WHERE e capture a projeção de retorno.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id, name";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }


    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING without WHERE keeps SET boundary and captures RETURNING projection.
    /// PT: Garante que UPDATE ... RETURNING sem WHERE preserve o limite do SET e capture a projeção de RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningWithoutWhere_ShouldCaptureReturningItems(int version)
    {
        const string sql = "UPDATE users SET name = 'b' RETURNING id";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Null(parsed.WhereRaw);
        Assert.Single(parsed.Set);
        Assert.Equal("'b'", parsed.Set[0].ValueRaw);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b', RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que UPDATE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING with qualified wildcard preserves projection item in AST.
    /// PT: Garante que UPDATE ... RETURNING com wildcard qualificado preserve o item de projeção na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningQualifiedWildcard_ShouldCaptureReturningItem(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING users.*";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Single(parsed.Returning);
        Assert.Equal("users.*", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING captures projection payload in AST for PostgreSQL dialect.
    /// PT: Garante que DELETE ... RETURNING capture o payload de projeção na AST para o dialeto PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var parsed = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }


    /// <summary>
    /// EN: Ensures DELETE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que DELETE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures empty RETURNING clause is rejected with actionable message.
    /// PT: Garante que cláusula RETURNING vazia seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_EmptyReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial no RETURNING seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final no RETURNING seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING supports nested expressions with commas and semicolon statement boundary.
    /// PT: Garante que RETURNING suporte expressões aninhadas com vírgulas e limite de statement por ponto e vírgula.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningNestedExpressionsWithSemicolon_ShouldCaptureItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING COALESCE((SELECT max(id) FROM users), 0) AS next_id, concat(name, ',x') AS decorated;";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("COALESCE((SELECT max(id) FROM users), 0)", parsed.Returning[0].Raw);
        Assert.Equal("next_id", parsed.Returning[0].Alias);
        Assert.Equal("concat(name, ',x')", parsed.Returning[1].Raw);
        Assert.Equal("decorated", parsed.Returning[1].Alias);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING com parênteses desbalanceados seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningUnbalancedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints are rejected for Npgsql.
    /// PT: Garante que hints de tabela do SQL Server sejam rejeitados para Npgsql.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
    }

    /// <summary>
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var limitOffset = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
            dialect));
        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));
        var fetchFirst = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id FETCH FIRST 2 ROWS ONLY",
            dialect));

        var normalizedLimit = Assert.IsType<SqlLimitOffset>(limitOffset.RowLimit);
        var normalizedOffsetFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);
        var normalizedFetchFirst = Assert.IsType<SqlLimitOffset>(fetchFirst.RowLimit);

        Assert.Equal(normalizedLimit, normalizedOffsetFetch);
        Assert.Equal(2, normalizedFetchFirst.Count);
        Assert.Null(normalizedFetchFirst.Offset);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for Npgsql.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para Npgsql.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Use hints compatíveis", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures unsupported quoted aliases are rejected with actionable parser diagnostics for this dialect.
    /// PT: Garante que aliases com quoting não suportado sejam rejeitados com diagnóstico acionável do parser para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", new NpgsqlDialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Npgsql accepts double-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o Npgsql aceite aliases com aspas duplas e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithDoubleQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User Name\" FROM users",
            new NpgsqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures Npgsql unescapes doubled double-quotes inside quoted aliases when normalizing AST alias text.
    /// PT: Garante que o Npgsql faça unescape de aspas duplas duplicadas dentro de aliases quoted ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithEscapedDoubleQuotedAlias_ShouldNormalizeEscapedQuote(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            new NpgsqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User\"Name", item.Alias);
    }


    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies DELETE without FROM returns an actionable error message.
    /// PT: Verifica que DELETE sem FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new NpgsqlDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE target alias before FROM returns an actionable error message.
    /// PT: Verifica que alias alvo de DELETE antes de FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new NpgsqlDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new NpgsqlDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE is rejected for Npgsql dialect with an actionable not-supported message.
    /// PT: Garante que MERGE seja rejeitado para o dialeto Npgsql com mensagem acionável de não suportado.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseMerge_UnsupportedDialect_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < NpgsqlDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("não suportado", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));
        Assert.IsType<SqlMergeQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new NpgsqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new NpgsqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by PostgreSQL version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do PostgreSQL e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expected = version >= NpgsqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("LAG"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against PostgreSQL dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto PostgreSQL por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(supported, dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(supported, dialect);
        Assert.IsType<WindowFunctionExpr>(expr);
        Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(unsupported, dialect));
    }


    /// <summary>
    /// EN: Ensures window functions that require ordering reject OVER clauses without ORDER BY.
    /// PT: Garante que funções de janela que exigem ordenação rejeitem cláusulas OVER sem ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", dialect));

        Assert.Contains("requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates window function argument arity for supported functions.
    /// PT: Garante que o parser valide a aridade dos argumentos de funções de janela suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", dialect));
            return;
        }

        var exRowNumber = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", dialect));
        Assert.Contains("does not accept arguments", exRowNumber.Message, StringComparison.OrdinalIgnoreCase);

        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE() OVER (ORDER BY id)", dialect));
        Assert.Contains("exactly 1 argument", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, 1, 0, 99) OVER (ORDER BY id)", dialect));
        Assert.Contains("between 1 and 3 arguments", exLag.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates literal semantic ranges for window function arguments.
    /// PT: Garante que o parser valide intervalos semânticos literais para argumentos de funções de janela.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new NpgsqlDialect(version);
        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", dialect));
            return;
        }


        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", dialect));
        Assert.Contains("positive bucket count", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, -1, 0) OVER (ORDER BY id)", dialect));
        Assert.Contains("non-negative offset", exLag.Message, StringComparison.OrdinalIgnoreCase);

        var exNthValue = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTH_VALUE(id, 0) OVER (ORDER BY id)", dialect));
        Assert.Contains("greater than zero", exNthValue.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ORDER BY requirement for window functions is exposed through dialect runtime hook.
    /// PT: Garante que o requisito de ORDER BY para funções de janela seja exposto pelo hook de runtime do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expected = version >= NpgsqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("LAG"));

        Assert.False(dialect.RequiresOrderByInWindowFunction("COUNT"));
    }


    /// <summary>
    /// EN: Ensures window function argument arity metadata is exposed through dialect hook.
    /// PT: Garante que os metadados de aridade de argumentos de função de janela sejam expostos pelo hook do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.False(dialect.TryGetWindowFunctionArgumentArity("ROW_NUMBER", out _, out _));
            return;
        }

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("ROW_NUMBER", out var rnMin, out var rnMax));
        Assert.Equal(0, rnMin);
        Assert.Equal(0, rnMax);

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("LAG", out var lagMin, out var lagMax));
        Assert.Equal(1, lagMin);
        Assert.Equal(3, lagMax);

        Assert.False(dialect.TryGetWindowFunctionArgumentArity("COUNT", out _, out _));
    }


    /// <summary>
    /// EN: Ensures ROWS window frame clauses parse when supported and RANGE remains gated.
    /// PT: Garante que cláusulas ROWS de frame de janela sejam interpretadas quando suportadas e que RANGE continue bloqueado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));
            return;
        }

        var rowsExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(rowsExpr);

        var rangeExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(rangeExpr);

        var groupsExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(groupsExpr);
    }



    /// <summary>
    /// EN: Ensures invalid window frame bound ordering is rejected by parser semantic validation.
    /// PT: Garante que ordenação inválida de limites de frame de janela seja rejeitada pela validação semântica do parser.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Npgsql parser accepts ordered-set WITHIN GROUP for STRING_AGG.
    /// PT: Garante que o parser Npgsql aceite ordered-set WITHIN GROUP para STRING_AGG.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("STRING_AGG", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy![0].Desc);
    }

    /// <summary>
    /// EN: Ensures Npgsql parser blocks non-native ordered-set aggregate names with WITHIN GROUP.
    /// PT: Garante que o parser Npgsql bloqueie nomes não nativos de agregação ordered-set com WITHIN GROUP.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_ListAggWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP clause fails with actionable ORDER BY message.
    /// PT: Garante que cláusula WITHIN GROUP malformada falhe com mensagem acionável de ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_StringAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures trailing commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas finais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByTrailingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ORDER BY lists in WITHIN GROUP are rejected with actionable message.
    /// PT: Garante que listas ORDER BY vazias em WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByEmptyList_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas iniciais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByLeadingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures missing commas between WITHIN GROUP ORDER BY expressions are rejected with actionable message.
    /// PT: Garante que ausência de vírgula entre expressões de ORDER BY no WITHIN GROUP seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
