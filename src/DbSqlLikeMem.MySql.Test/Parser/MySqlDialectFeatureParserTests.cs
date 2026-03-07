namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers MySQL-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do MySQL.
/// </summary>
public sealed class MySqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT syntax is rejected for MySQL.
    /// PT: Garante que a sintaxe ON CONFLICT do PostgreSQL seja rejeitada no MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflict_ShouldRespectDialectRule(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO NOTHING ... RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO NOTHING ... RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoNothingWithReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO NOTHING ... RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO NOTHING ... RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoNothingWithInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO NOTHING ... RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO NOTHING ... RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoNothingWithUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO NOTHING ... RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO NOTHING ... RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoNothingWithEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothing_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE without predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE sem predicado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereWithoutPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE terminated by semicolon-only remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE terminado apenas por ponto e vírgula continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereOnlySemicolon_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE; DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE with malformed predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE malformado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereInvalidPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id = DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING + RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with unexpected continuation token remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com token de continuação inesperado do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingUnexpectedContinuationToken_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with FROM clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula FROM do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingWithFromClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with USING clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula USING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingWithUsingClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with SET clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula SET do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingWithSetClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with UPDATE clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula UPDATE do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingWithUpdateClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO NOTHING with additional WHERE clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula WHERE adicional do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereDoNothingWithWhereClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereDoNothing_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereDoNothingReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereDoNothingInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereDoNothingUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereDoNothingEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with FROM clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula FROM do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithFromClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with USING clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula USING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithUsingClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with SET clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula SET do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithSetClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with UPDATE clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula UPDATE do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithUpdateClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with additional WHERE clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula WHERE adicional do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithWhereClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO NOTHING with unexpected continuation token remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com token de continuação inesperado do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoNothingWithUnexpectedContinuationToken_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE with FROM clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula FROM do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWithFromClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE with USING clause remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula USING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWithUsingClause_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by FROM remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por FROM do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetFromWithoutAssignments_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by USING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por USING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetUsingWithoutAssignments_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT without DO branch remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem ramo DO do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutDoBranch_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT without constraint name remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome da constraint do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutName_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT without name at end-of-statement remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome no fim do statement continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutNameAtEndOfStatement_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO with invalid continuation remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO com continuação inválida do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoInvalidContinuation_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO SKIP";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE without SET remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE sem SET do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWithoutSet_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET without assignments remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET sem atribuições do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetWithoutAssignments_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET with leading comma remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula inicial do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetLeadingComma_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET with trailing comma remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula final do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetTrailingComma_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET assignments without comma separator remain rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuições sem separador por vírgula do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetAssignmentsWithoutCommaSeparator_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET with repeated SET keyword remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com palavra-chave SET repetida do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetRepeatedSetKeyword_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET assignment without equals remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuição sem sinal de igual do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetAssignmentWithoutEquals_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE SET with malformed assignment expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com expressão de atribuição malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetInvalidAssignmentExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = (EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE ... RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE ... RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE ... RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE ... RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE ... RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE ... RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE ... RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE ... RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE WHERE with semicolon-only predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE contendo apenas ponto e vírgula continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereOnlySemicolon_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE WHERE with semicolon-only predicate remains rejected for MySQL with actionable guidance even without RETURNING.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE contendo apenas ponto e vírgula continue rejeitado no MySQL com orientação acionável mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereOnlySemicolonWithoutReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE WHERE without predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE sem predicado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereWithoutPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE WHERE with malformed predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE malformado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereInvalidPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão malformada em RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT DO UPDATE with valid WHERE and empty RETURNING list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e lista vazia em RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintUpdateWhereReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintUpdateWhereInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintUpdateWhereUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintUpdateWhereEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE remains rejected for MySQL with actionable guidance even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE do PostgreSQL continue rejeitado no MySQL com orientação acionável mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintUpdateWhereWithoutReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereUpdateWhereReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereUpdateWhereInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereUpdateWhereUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereUpdateWhereEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT target WHERE + DO UPDATE WHERE remains rejected for MySQL with actionable guidance even without RETURNING.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE do PostgreSQL continue rejeitado no MySQL com orientação acionável mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictTargetWhereUpdateWhereWithoutReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE without predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE sem predicado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereWithoutPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE terminated by semicolon-only remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE terminado apenas por ponto e vírgula continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereOnlySemicolon_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE; DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT target WHERE with malformed predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE malformado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereInvalidPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE WHERE with semicolon-only predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE contendo apenas ponto e vírgula continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereOnlySemicolon_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated by semicolon-only remains rejected for MySQL with actionable guidance even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE terminado apenas por ponto e vírgula continue rejeitado no MySQL com orientação acionável mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereOnlySemicolonWithoutReturning_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE WHERE without predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE sem predicado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereWithoutPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE WHERE with malformed predicate remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE malformado continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereInvalidPredicate_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão malformada em RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereInvalidReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereUnbalancedReturningExpression_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and empty RETURNING list remains rejected for MySQL with actionable guidance.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e lista vazia em RETURNING do PostgreSQL continue rejeitado no MySQL com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereEmptyReturningList_ShouldRespectDialectRule(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures valid ON DUPLICATE KEY UPDATE assignments materialize parsed scalar expressions in AST.
    /// PT: Garante que atribuições válidas de ON DUPLICATE KEY UPDATE materializem expressões escalares parseadas na AST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateValidAssignments_ShouldMaterializeParsedExpressions(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name), updated_at = NOW()";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Equal(2, parsed.OnDupAssignsParsed.Count);
        Assert.All(parsed.OnDupAssignsParsed, a => Assert.NotNull(a.ValueExpr));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for MySQL INSERT statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em INSERT no MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no INSERT do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING clause remains rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que a cláusula RETURNING continue rejeitada em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression remains rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que expressão malformada em RETURNING continue rejeitada em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list remains rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que lista vazia em RETURNING continue rejeitada em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures unbalanced parentheses in RETURNING remain rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que parênteses desbalanceados em RETURNING continuem rejeitados em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading comma in RETURNING remains rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que vírgula inicial em RETURNING continue rejeitada em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithLeadingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures trailing comma in RETURNING remains rejected for MySQL INSERT with ON DUPLICATE KEY UPDATE.
    /// PT: Garante que vírgula final em RETURNING continue rejeitada em INSERT MySQL com ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithTrailingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for MySQL UPDATE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em UPDATE no MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no UPDATE do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for MySQL DELETE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em DELETE no MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no DELETE do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no INSERT do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no INSERT do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no INSERT do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no INSERT do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in MySQL INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no INSERT do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no UPDATE do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no UPDATE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no UPDATE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no UPDATE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in MySQL UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no UPDATE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_SetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET without assignments and followed by WHERE is rejected with actionable token context.
    /// PT: Garante que UPDATE SET sem atribuições e seguido por WHERE seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_SetWithoutAssignmentsBeforeWhere_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_SetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET SET name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET leading comma is rejected with actionable token context.
    /// PT: Garante que vírgula inicial em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_SetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET , name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("unexpected comma before assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET trailing comma is rejected with actionable token context.
    /// PT: Garante que vírgula final em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_SetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b', WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("trailing comma without assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE without predicate is rejected with actionable token context.
    /// PT: Garante que UPDATE com WHERE sem predicado seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE terminated only by semicolon is rejected with actionable token context.
    /// PT: Garante que UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUpdate_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no DELETE do MySQL continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no DELETE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no DELETE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no DELETE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in MySQL DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no DELETE do MySQL continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE without predicate is rejected with actionable token context.
    /// PT: Garante que DELETE com WHERE sem predicado seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE terminated only by semicolon is rejected with actionable token context.
    /// PT: Garante que DELETE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseDelete_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em ON DUPLICATE KEY UPDATE sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateAssignmentsWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) updated_at = NOW()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("must separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignment with malformed expression is rejected with actionable message.
    /// PT: Garante que atribuição em ON DUPLICATE KEY UPDATE com expressão malformada seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = (VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em ON DUPLICATE KEY UPDATE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name),";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and followed by RETURNING is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e seguido por RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with empty RETURNING list is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com lista vazia em RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and terminated by semicolon is rejected with actionable token context.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e finalizado por ponto e vírgula seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with unbalanced RETURNING expression is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com expressão RETURNING desbalanceada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with WHERE clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula WHERE seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithWhereClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support a WHERE clause", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with FROM clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithFromClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with USING clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithoutAssignmentsWithUsingClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE SET name = VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("must not include SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em ON DUPLICATE KEY UPDATE sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em ON DUPLICATE KEY UPDATE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE , name = VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with WHERE clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula WHERE seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithWhereClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support a WHERE clause", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with table-source clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula de table-source seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithFromClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with USING clause is rejected with actionable message.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnDuplicateWithUsingClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES reports row/position for malformed expression in later rows.
    /// PT: Garante que INSERT VALUES reporte linha/posição para expressão malformada em linhas posteriores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_ValuesSecondRowInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a'), (2 +, 'b')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("row 2 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE support follows the configured MySQL version.
    /// PT: Garante que o suporte a with recursive siga a versão configurada do MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithRecursive_ShouldRespectVersion(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte";

        if (version < MySqlDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }



    /// <summary>
    /// EN: Verifies unsupported WITH RECURSIVE versions return actionable MySQL guidance.
    /// PT: Verifica que versões sem suporte a WITH RECURSIVE retornam orientação acionável para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion(VersionLowerThan = MySqlDialect.WithCteMinVersion)]
    public void ParseSelect_WithRecursive_UnsupportedVersion_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte", new MySqlDialect(version)));

        Assert.Contains("WITH/CTE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index/keyword hints are parsed.
    /// PT: Garante que hints de índice/palavras-chave do MySQL sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users AS u USE INDEX (idx_users_id) IGNORE KEY FOR ORDER BY (idx_users_name)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.NotNull(parsed.Table);
        Assert.Equal(2, parsed.Table!.MySqlIndexHints?.Count ?? 0);
    }




    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR ORDER BY is captured in AST.
    /// PT: Garante que o escopo FOR ORDER BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForOrderBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u IGNORE INDEX FOR ORDER BY (idx_users_name) ORDER BY u.name";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Ignore, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.OrderBy, hint.Scope);
        Assert.Equal(["idx_users_name"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR GROUP BY is captured in AST.
    /// PT: Garante que o escopo FOR GROUP BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForGroupBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR GROUP BY (idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.GroupBy, hint.Scope);
        Assert.Equal(["idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures advanced MySQL index hints with PRIMARY and FOR JOIN are parsed.
    /// PT: Garante que hints avançados de índice MySQL com PRIMARY e FOR JOIN sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithAdvancedIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR JOIN (PRIMARY, idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Join, hint.Scope);
        Assert.Equal(["PRIMARY", "idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures empty MySQL index hint list is rejected.
    /// PT: Garante que lista vazia em hint de índice MySQL seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintList_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX ()";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("lista de índices vazia", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint list containing empty item is rejected.
    /// PT: Garante que lista de hints MySQL contendo item vazio seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintItem_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id, )";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("item vazio", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint names with dollar and escaped backtick quoted names are parsed.
    /// PT: Garante que nomes de índice MySQL com cifrão e nomes quoted com escape de backtick sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithExtendedValidIndexHintNames_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx$users, `idx``quoted`)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Use, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Any, hint.Scope);
        Assert.Equal(["idx$users", "idx`quoted"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures OFFSET/FETCH compatibility syntax is accepted for MySQL parser.
    /// PT: Garante que a sintaxe de compatibilidade OFFSET/FETCH seja aceita pelo parser MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }


    /// <summary>
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new MySqlDialect(version);

        var limitOffset = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
            dialect));
        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));

        var normalizedLimit = Assert.IsType<SqlLimitOffset>(limitOffset.RowLimit);
        var normalizedFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);

        Assert.Equal(normalizedLimit, normalizedFetch);
        Assert.Equal(2, normalizedFetch.Count);
        Assert.Equal(1, normalizedFetch.Offset);
    }



    /// <summary>
    /// EN: Verifies FETCH FIRST syntax returns actionable MySQL pagination guidance.
    /// PT: Verifica que sintaxe FETCH FIRST retorna orientação acionável de paginação para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_FetchFirst_ShouldProvidePaginationHint(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users ORDER BY id FETCH FIRST 5 ROWS ONLY", new MySqlDialect(version)));

        Assert.Contains("FETCH FIRST/NEXT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIMIT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mysql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new MySqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }




    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new MySqlDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for MySQL.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("USE/IGNORE/FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures unsupported quoted aliases are rejected with actionable parser diagnostics for this dialect.
    /// PT: Garante que aliases com quoting não suportado sejam rejeitados com diagnóstico acionável do parser para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithBracketQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name [User Name] FROM users", new MySqlDialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'['", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL accepts backtick-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o MySQL aceite aliases com crase e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name `User Name` FROM users",
            new MySqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures MySQL unescapes doubled backticks inside backtick-quoted aliases when normalizing AST alias text.
    /// PT: Garante que o MySQL faça unescape de crases duplicadas dentro de aliases com crase ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEscapedBacktickQuotedAlias_ShouldNormalizeEscapedBacktick(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name `User``Name` FROM users",
            new MySqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User`Name", item.Alias);
    }



    /// <summary>
    /// EN: Verifies MERGE in MySQL returns actionable replacement guidance.
    /// PT: Verifica que MERGE no MySQL retorna orientação acionável de substituição.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseMerge_UnsupportedDialect_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by MySQL version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do MySQL e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = new MySqlDialect(version);

        var expected = version >= MySqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("RANK"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against MySQL dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto MySQL por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new MySqlDialect(version);
        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new MySqlDialect(version);

        var expected = version >= MySqlDialect.WindowFunctionsMinVersion;
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
    [MemberDataMySqlVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SELECT parsing with string aggregate WITHIN GROUP is blocked by MySQL dialect gate.
    /// PT: Garante que parsing de SELECT com agregação textual WITHIN GROUP seja bloqueado pelo gate de dialeto MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_StringAggregateWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SELECT parsing accepts MySQL native GROUP_CONCAT ordering syntax.
    /// PT: Garante que o parsing de SELECT aceite a sintaxe nativa de ordenacao do GROUP_CONCAT no MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_StringAggregateOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new MySqlDialect(version);

        var parsed = Assert.IsType<SqlSelectQuery>(
            SqlQueryParser.Parse("SELECT GROUP_CONCAT(amount ORDER BY amount DESC SEPARATOR '|') AS joined FROM orders", dialect));

        Assert.Single(parsed.SelectItems);
        Assert.Contains("GROUP_CONCAT", parsed.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures call-only temporal identifier without parentheses is rejected with actionable guidance.
    /// PT: Garante que identificador temporal apenas-invocável sem parênteses seja rejeitado com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_CallOnlyTemporalIdentifierWithoutParentheses_ShouldThrowClearError(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NOW", dialect));

        Assert.Contains("NOW", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures token-only temporal identifier called with parentheses is rejected with actionable guidance.
    /// PT: Garante que identificador temporal no formato token chamado com parênteses seja rejeitado com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_TokenOnlyTemporalIdentifierCalledWithParentheses_ShouldThrowClearError(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("CURRENT_DATE()", dialect));

        Assert.Contains("CURRENT_DATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL parser accepts native ORDER BY and SEPARATOR inside GROUP_CONCAT.
    /// PT: Garante que o parser MySQL aceite ORDER BY e SEPARATOR nativos dentro de GROUP_CONCAT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_GroupConcatOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new MySqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount ORDER BY amount DESC, id ASC SEPARATOR '|')", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("GROUP_CONCAT", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, call.Args.Count);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Equal(2, call.WithinGroupOrderBy!.Count);
        Assert.True(call.WithinGroupOrderBy[0].Desc);
        Assert.False(call.WithinGroupOrderBy[1].Desc);
    }

    /// <summary>
    /// EN: Ensures MySQL parser preserves DISTINCT when native ORDER BY is used inside GROUP_CONCAT.
    /// PT: Garante que o parser MySQL preserve DISTINCT quando ORDER BY nativo e usado dentro de GROUP_CONCAT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_GroupConcatDistinctOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new MySqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("GROUP_CONCAT(DISTINCT amount ORDER BY amount DESC SEPARATOR '|')", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.True(call.Distinct);
        Assert.Equal(2, call.Args.Count);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy[0].Desc);
    }

    /// <summary>
    /// EN: Ensures malformed native SEPARATOR usage in GROUP_CONCAT fails with actionable message.
    /// PT: Garante que uso nativo malformado de SEPARATOR em GROUP_CONCAT falhe com mensagem acionavel.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_GroupConcatSeparatorWithoutExpression_ShouldThrowActionableError(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount ORDER BY amount DESC SEPARATOR)", dialect));

        Assert.Contains("separator keyword requires an expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITHIN GROUP ordered-set syntax remains unsupported for MySQL aggregates.
    /// PT: Garante que a sintaxe ordered-set WITHIN GROUP continue não suportada para agregações MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP syntax in MySQL still fails as not-supported (dialect gate precedence).
    /// PT: Garante que sintaxe malformada de WITHIN GROUP no MySQL continue falhando como não suportada (precedência do gate de dialeto).
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroupMalformed_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed trailing comma in WITHIN GROUP remains blocked by dialect gate.
    /// PT: Garante que vírgula final malformada no WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroupTrailingComma_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ORDER BY list in WITHIN GROUP remains blocked by dialect gate.
    /// PT: Garante que lista ORDER BY vazia em WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByEmptyList_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading commas in WITHIN GROUP ORDER BY remain blocked by dialect gate.
    /// PT: Garante que vírgulas iniciais no ORDER BY do WITHIN GROUP continuem bloqueadas pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByLeadingComma_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures missing commas in malformed WITHIN GROUP ORDER BY remain blocked by dialect gate.
    /// PT: Garante que ausência de vírgula em ORDER BY malformado no WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowNotSupported(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL parser accepts MATCH(...) AGAINST(...) and maps to internal MATCH_AGAINST call form.
    /// PT: Garante que o parser MySQL aceite MATCH(...) AGAINST(...) e mapeie para forma interna MATCH_AGAINST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_MatchAgainst_ShouldParseAsInternalCall(int version)
    {
        var dialect = new MySqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("MATCH(title, body) AGAINST ('hello world')", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("MATCH_AGAINST", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.True(call.Args.Count >= 2);
        Assert.IsType<RowExpr>(call.Args[0]);
        var queryLiteral = Assert.IsType<LiteralExpr>(call.Args[1]);
        Assert.Equal("hello world", queryLiteral.Value);
    }

    /// <summary>
    /// EN: Ensures MySQL parser accepts AGAINST mode tail (for example IN BOOLEAN MODE).
    /// PT: Garante que o parser MySQL aceite sufixo de modo no AGAINST (por exemplo IN BOOLEAN MODE).
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_MatchAgainstWithBooleanMode_ShouldParseAndCaptureMode(int version)
    {
        var dialect = new MySqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("MATCH(title) AGAINST ('+mysql -oracle' IN BOOLEAN MODE)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("MATCH_AGAINST", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.True(call.Args.Count >= 3);

        var mode = Assert.IsType<RawSqlExpr>(call.Args[2]);
        Assert.Contains("BOOLEAN MODE", mode.Sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures parser accepts NATURAL LANGUAGE + QUERY EXPANSION mode in AGAINST clause.
    /// PT: Garante que o parser aceite modo NATURAL LANGUAGE + QUERY EXPANSION na cláusula AGAINST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_MatchAgainstWithNaturalLanguageAndQueryExpansion_ShouldParse(int version)
    {
        var dialect = new MySqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar(
            "MATCH(title) AGAINST ('database indexing' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
            dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("MATCH_AGAINST", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.True(call.Args.Count >= 3);
        var mode = Assert.IsType<RawSqlExpr>(call.Args[2]);
        Assert.Contains("NATURAL LANGUAGE MODE WITH QUERY EXPANSION", mode.Sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures parser rejects unsupported AGAINST mode combinations with actionable message.
    /// PT: Garante que o parser rejeite combinações de modo AGAINST não suportadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_MatchAgainstWithInvalidMode_ShouldThrowActionableError(int version)
    {
        var dialect = new MySqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("MATCH(title) AGAINST ('john' IN BOOLEAN)", dialect));

        Assert.Contains("Unsupported AGAINST mode", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
