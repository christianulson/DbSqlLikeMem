namespace DbSqlLikeMem;

internal sealed class SqlQueryParser
{
    private enum ReturningClauseTarget
    {
        Insert,
        Update,
        Delete
    }

    private readonly IReadOnlyList<SqlToken> _toks;
    private readonly ISqlDialect _dialect;
    private readonly IDataParameterCollection? _parameters;
    private readonly AutoSqlSyntaxFeatures _autoSyntaxFeatures;
    private int _i;
    // INSERT ... SELECT pode ter um sufixo de UPSERT após o SELECT (MySQL ON DUPLICATE..., Postgres ON CONFLICT ...)
    private bool _allowInsertSelectSuffixBoundary;

    private static readonly SqlQueryAstCache _astCache = SqlQueryAstCache.CreateFromEnvironment();
    private static readonly SqlQueryParsePreludeCache _preludeCache = SqlQueryParsePreludeCache.CreateFromEnvironment();


    /// <summary>
    /// EN: Implements SqlQueryParser.
    /// PT: Implementa SqlQueryParser.
    /// </summary>
    public SqlQueryParser(string sql, ISqlDialect dialect)
        : this(sql, dialect, null)
    {
    }

    /// <summary>
    /// EN: Implements SqlQueryParser.
    /// PT: Implementa SqlQueryParser.
    /// </summary>
    public SqlQueryParser(string sql, ISqlDialect dialect, IDataParameterCollection? parameters)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _dialect = dialect;
        _parameters = parameters;
        var prelude = GetPrelude(sql, dialect);
        _toks = prelude.Tokens;
        _autoSyntaxFeatures = prelude.AutoSyntaxFeatures;
        _i = 0;
    }

    private SqlQueryParser(
        IReadOnlyList<SqlToken> toks,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        AutoSqlSyntaxFeatures autoSyntaxFeatures)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(toks, nameof(toks));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _toks = toks;
        _dialect = dialect;
        _parameters = parameters;
        _autoSyntaxFeatures = autoSyntaxFeatures;
        _i = 0;
    }

    /// <summary>
    /// EN: Parses one SQL statement into an AST root using default parser options and no parameter collection.
    /// PT: Faz o parsing de um statement SQL para a raiz da AST usando opções padrão do parser e sem coleção de parâmetros.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase Parse(string sql, ISqlDialect dialect)
        => Parse(sql, dialect, null);

    /// <summary>
    /// EN: Parses one SQL statement using the automatic dialect compatibility mode.
    /// PT: Faz o parsing de um statement SQL usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase ParseAuto(string sql)
        => Parse(sql, new AutoSqlDialect(), null);

    /// <summary>
    /// EN: Parses one SQL statement using the automatic dialect compatibility mode and optional parameters.
    /// PT: Faz o parsing de um statement SQL usando o modo de compatibilidade automatica de dialeto e parametros opcionais.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized numeric values. PT: Parametros de comando opcionais usados por caminhos do parser que resolvem valores numericos parametrizados.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase ParseAuto(string sql, IDataParameterCollection? parameters)
        => Parse(sql, new AutoSqlDialect(), parameters);

    /// <summary>
    /// EN: Parses one SQL statement into an AST root using dialect capabilities and optional command parameters.
    /// PT: Faz o parsing de um statement SQL para a raiz da AST usando capacidades do dialeto e parâmetros de comando opcionais.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized numeric values. PT: Parâmetros de comando opcionais usados por caminhos do parser que resolvem valores numéricos parametrizados.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase Parse(string sql, ISqlDialect dialect, IDataParameterCollection? parameters)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        var metrics = DbMetrics.Current;
        long startedAt = 0;
        if (metrics is not null)
        {
            metrics.IncrementPerformancePhaseHit(DbPerformanceMetricKeys.SqlParse);
            startedAt = Stopwatch.GetTimestamp();
        }

        try
        {


            // Fast feature gate before cache lookup to avoid serving incompatible ASTs for version-gated commands.
            var (preludeTokens, autoSyntaxFeatures) = GetPrelude(sql, dialect);
            var first = preludeTokens.Count > 0 ? preludeTokens[0] : default;
            if (IsWord(first, SqlConst.MERGE) && !dialect.SupportsMerge)
                throw SqlUnsupported.ForMerge(dialect);

            // ALWAYS use cache if available. Para evitar dependências de valores de parâmetros no AST (que quebraria o cache),
            // cláusulas como LIMIT/OFFSET agora armazenam SqlExpr (ParameterExpr) em vez de resolver para int durante o parse.
            var cacheKey = SqlQueryAstCache.BuildKey(sql, dialect.Name, dialect.Version);
            if (_astCache.TryGet(cacheKey, out var cached))
            {
                EnsureDialectSupport(cached, dialect);
                return cached with { RawSql = sql };
            }

            // DDL statements are cheap to parse and benefit from deterministic no-cache behavior in tests.
            if (IsWord(first, SqlConst.CREATE) || IsWord(first, SqlConst.ALTER) || IsWord(first, SqlConst.DROP))
            {
                var uncached = ParseUncached(preludeTokens, dialect, null, autoSyntaxFeatures);
                EnsureDialectSupport(uncached, dialect);
                return uncached with { RawSql = sql };
            }

            var parsed = ParseUncached(preludeTokens, dialect, parameters, autoSyntaxFeatures);
            EnsureDialectSupport(parsed, dialect);
            _astCache.Set(cacheKey, parsed);

            // Para estratégias que precisam do SQL original (ex: UPDATE/DELETE ... JOIN (SELECT ...))
            return parsed with { RawSql = sql };
        }
        finally
        {
            if (startedAt != 0)
                metrics!.IncrementPerformancePhaseElapsedTicks(
                    DbPerformanceMetricKeys.SqlParse,
                    StopwatchCompatible.GetElapsedTicks(startedAt));
        }
    }

    private static SqlQueryParsePreludeCache.Prelude GetPrelude(string sql, ISqlDialect dialect)
    {
        var cacheKey = SqlQueryParsePreludeCache.BuildKey(sql, dialect.Name, dialect.Version);
        if (_preludeCache.TryGet(cacheKey, out var cached))
            return cached;

        var tokens = new SqlTokenizer(sql, dialect).Tokenize();
        var autoSyntaxFeatures = dialect is AutoSqlDialect
            ? SqlSyntaxDetector.Detect(sql, tokens)
            : AutoSqlSyntaxFeatures.None;

        var prelude = new SqlQueryParsePreludeCache.Prelude(tokens, autoSyntaxFeatures);
        _preludeCache.Set(cacheKey, prelude);
        return prelude;
    }

    private static void EnsureDialectSupport(SqlQueryBase parsed, ISqlDialect dialect)
    {
        switch (parsed)
        {
            case SqlMergeQuery when !dialect.SupportsMerge:
                throw SqlUnsupported.ForMerge(dialect);
            case SqlSelectQuery select:
                EnsureSelectDialectSupport(select, dialect);
                break;
            case SqlUnionQuery union:
                foreach (var part in union.Parts)
                    EnsureSelectDialectSupport(part, dialect);
                EnsureRowLimitDialectSupport(union.RowLimit, dialect);
                break;
            case SqlInsertQuery insert when insert.InsertSelect is not null:
                EnsureSelectDialectSupport(insert.InsertSelect, dialect);
                break;
            case SqlUpdateQuery update when update.UpdateFromSelect is not null:
                EnsureSelectDialectSupport(update.UpdateFromSelect, dialect);
                break;
            case SqlDeleteQuery delete when delete.DeleteFromSelect is not null:
                EnsureSelectDialectSupport(delete.DeleteFromSelect, dialect);
                break;
        }
    }

    private static void EnsureSelectDialectSupport(SqlSelectQuery select, ISqlDialect dialect)
    {
        if (select.Ctes.Count > 0 && !dialect.SupportsWithCte)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.WITH_CTE);

        EnsureRowLimitDialectSupport(select.RowLimit, dialect);

        if (select.ForJson is not null && !dialect.SupportsForJsonClause)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.FOR_JSON);

        if (select.Table?.Derived is not null)
            EnsureSelectDialectSupport(select.Table.Derived, dialect);

        foreach (var join in select.Joins)
        {
            if (join.Table.Derived is not null)
                EnsureSelectDialectSupport(join.Table.Derived, dialect);
        }
    }

    private static void EnsureRowLimitDialectSupport(SqlRowLimit? rowLimit, ISqlDialect dialect)
    {
        if (rowLimit is SqlFetch fetch)
        {
            if (fetch.Offset != null)
            {
                if (!dialect.SupportsOffsetFetch)
                    throw SqlUnsupported.ForPagination(dialect, SqlConst.OFFSET_FETCH);
                return;
            }

            if (!dialect.SupportsFetchFirst)
                throw SqlUnsupported.ForPagination(dialect, SqlConst.FETCH_FIRST_NEXT);
        }
    }

    private static SqlQueryBase ParseUncached(
        IReadOnlyList<SqlToken> tokens,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        AutoSqlSyntaxFeatures autoSyntaxFeatures)
    {
        var q = new SqlQueryParser(tokens, dialect, parameters, autoSyntaxFeatures);
        var first = q.Peek();

        SqlQueryBase? result;
        if (IsWord(first, SqlConst.SELECT) || IsWord(first, SqlConst.WITH))
            result = q.ParseSelectOrUnionQuery();
        else if (IsWord(first, SqlConst.INSERT))
            result = q.ParseInsert();
        else if (IsWord(first, SqlConst.REPLACE))
            result = q.ParseReplace();
        else if (IsWord(first, SqlConst.UPDATE))
            result = q.ParseUpdate();
        else if (IsWord(first, SqlConst.DELETE))
            result = q.ParseDelete();
        else if (IsWord(first, SqlConst.CREATE))
            result = q.ParseCreate();
        else if (IsWord(first, SqlConst.ALTER))
            result = q.ParseAlter();
        else if (IsWord(first, SqlConst.DROP))
            result = q.ParseDrop();
        else if (IsWord(first, SqlConst.MERGE))
        {
            // Para MySQL, MERGE simplesmente não existe (é sintaxe inválida para o dialeto).
            // Os testes de corpus esperam ThrowInvalid aqui, não NotSupported.
            if (!dialect.SupportsMerge)
                throw SqlUnsupported.ForMerge(dialect);

            result = q.ParseMerge();
        }
        else
            throw SqlUnsupported.ForUnknownTopLevelStatement(dialect, first.Text);

        return result;
    }

    /// <summary>
    /// EN: Parses a SQL batch and yields AST roots for each top-level statement using default parser options.
    /// PT: Faz o parsing de um lote SQL e retorna raízes de AST para cada statement top-level usando opções padrão do parser.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="dialect">EN: Dialect used to split and parse each statement. PT: Dialeto usado para separar e parsear cada statement.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequência de raízes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMulti(
        string sql,
        ISqlDialect dialect)
        => ParseMulti(sql, dialect, null);

    /// <summary>
    /// EN: Parses a SQL batch using the automatic dialect compatibility mode.
    /// PT: Faz o parsing de um lote SQL usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequencia de raizes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMultiAuto(string sql)
        => ParseMulti(sql, new AutoSqlDialect(), null);

    /// <summary>
    /// EN: Parses a SQL batch using the automatic dialect compatibility mode and optional parameters.
    /// PT: Faz o parsing de um lote SQL usando o modo de compatibilidade automatica de dialeto e parametros opcionais.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="parameters">EN: Optional parameters forwarded to each statement parse. PT: Parametros opcionais repassados para o parse de cada statement.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequencia de raizes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMultiAuto(
        string sql,
        IDataParameterCollection? parameters)
        => ParseMulti(sql, new AutoSqlDialect(), parameters);

    /// <summary>
    /// EN: Parses a SQL batch and yields AST roots for each top-level statement split by semicolon boundaries.
    /// PT: Faz o parsing de um lote SQL e retorna raízes de AST para cada statement top-level separado por fronteiras de ponto e vírgula.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="dialect">EN: Dialect used to split and parse each statement. PT: Dialeto usado para separar e parsear cada statement.</param>
    /// <param name="parameters">EN: Optional parameters forwarded to each statement parse. PT: Parâmetros opcionais repassados para o parse de cada statement.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequência de raízes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMulti(
        string sql,
        ISqlDialect dialect,
        IDataParameterCollection? parameters)
    {
        // O split top-level ainda é útil para separar statements por ';'
        foreach (var s in SplitStatementsTopLevel(sql, dialect))
        {
            if (string.IsNullOrWhiteSpace(s)) continue;
            yield return Parse(s, dialect, parameters);
        }
    }

    /// <summary>
    /// EN: Splits a SQL batch into top-level statements preserving dialect string/comment rules.
    /// PT: Separa um lote SQL em statements de topo preservando regras de string/comentário do dialeto.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="dialect">EN: Dialect used for statement boundaries. PT: Dialeto usado para limites de statement.</param>
    /// <returns>EN: Top-level SQL statements. PT: Statements SQL de topo.</returns>
    public static IEnumerable<string> SplitStatements(
        string sql,
        ISqlDialect dialect)
        => SplitStatementsTopLevel(sql, dialect);

    /// <summary>
    /// EN: Splits a SQL batch into top-level statements using the automatic dialect compatibility mode.
    /// PT: Separa um lote SQL em statements de topo usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <returns>EN: Top-level SQL statements. PT: Statements SQL de topo.</returns>
    public static IEnumerable<string> SplitStatementsAuto(string sql)
        => SplitStatementsTopLevel(sql, new AutoSqlDialect());

    // Mantido para compatibilidade com lógica de Union
    /// <summary>
    /// EN: Represents a normalized UNION parsing result including parts, ALL flags, final ORDER BY and row-limit tail.
    /// PT: Representa um resultado normalizado de parsing de UNION incluindo partes, flags ALL, ORDER BY final e cauda de limite de linhas.
    /// </summary>
    public sealed record UnionChain(
        IReadOnlyList<SqlSelectQuery> Parts,
        IReadOnlyList<bool> AllFlags,
        IReadOnlyList<SqlOrderByItem> OrderBy,
        SqlRowLimit? RowLimit
    );

    /// <summary>
    /// EN: Parses SQL into a normalized UNION chain contract used by callers that expect UNION metadata even for single SELECT.
    /// PT: Faz o parsing de SQL para um contrato normalizado de cadeia UNION usado por chamadores que esperam metadados de UNION mesmo para SELECT único.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="dialect">EN: Dialect used for parsing. PT: Dialeto usado no parsing.</param>
    /// <returns>EN: Normalized UNION chain representation. PT: Representação normalizada de cadeia UNION.</returns>
    public static UnionChain ParseUnionChain(string sql, ISqlDialect dialect)
    {
        var parsed = Parse(sql, dialect);
        if (parsed is SqlUnionQuery uq)
            return new UnionChain(uq.Parts, uq.AllFlags, uq.OrderBy, uq.RowLimit);

        if (parsed is SqlSelectQuery sq)
            return new UnionChain([sq], [], [], null);

        throw new InvalidOperationException("UNION chain deve conter apenas SELECT.");
    }

    /// <summary>
    /// EN: Parses SQL into a normalized UNION chain using the automatic dialect compatibility mode.
    /// PT: Faz o parsing de SQL para uma cadeia UNION normalizada usando o modo de compatibilidade automatica de dialeto.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <returns>EN: Normalized UNION chain representation. PT: Representacao normalizada de cadeia UNION.</returns>
    public static UnionChain ParseUnionChainAuto(string sql)
        => ParseUnionChain(sql, new AutoSqlDialect());

    // ------------------------------------------------------------
    // NOVAS IMPLEMENTAÃ‡Ã•ES DE INSERT / UPDATE / DELETE VIA TOKENS
    // ------------------------------------------------------------

    private SqlInsertQuery ParseInsert()
        => ParseInsertLike(false);

    private SqlInsertQuery ParseReplace()
        => ParseInsertLike(true);

    private SqlInsertQuery ParseInsertLike(bool isReplace)
    {
        Consume(); // INSERT / REPLACE
        var insertIgnore = ConsumeOptionalInsertModifiers(isReplace);
        if (IsWord(Peek(), SqlConst.INTO)) Consume();

        var table = ParseTableSource(
            consumeHints: false,
            allowFunctionSource: false,
            aliasStopWords: [SqlConst.VALUE, SqlConst.PARTITION]); // Tabela

        ConsumeOptionalPartitionClause();

        // REPLACE ... SET col1 = expr, col2 = expr
        var valuesRaw = new List<List<string>>();
        var valuesExpr = new List<List<SqlExpr?>>();
        List<string> cols;
        bool hasExplicitColumnList;
        SqlSelectQuery? insertSelect = null;
        if (IsWord(Peek(), SqlConst.SET))
        {
            Consume(); // SET
            var assignments = ParseReplaceSetAssignments();
            cols = [.. assignments.Select(a => a.Column)];
            valuesRaw.Add([.. assignments.Select(a => a.ValueRaw)]);
            valuesExpr.Add([.. assignments.Select(a => a.ValueExpr)]);
            hasExplicitColumnList = false;
        }
        else
        {
            // Colunas opcionais: (col1, col2)
            hasExplicitColumnList = IsSymbol(Peek(), "(");
            cols = ParseCols();
        }

        // VALUES / VALUE ou SELECT?
        if (valuesRaw.Count == 0 && (IsWord(Peek(), SqlConst.VALUES) || IsWord(Peek(), SqlConst.VALUE)))
        {
            Consume(); // VALUES / VALUE
            ParseInsertValuesRows(valuesRaw, valuesExpr);
        }
        else if (valuesRaw.Count == 0 && (IsWord(Peek(), SqlConst.SELECT) || IsWord(Peek(), SqlConst.WITH)))
        {
            _allowInsertSelectSuffixBoundary = _dialect.SupportsOnDuplicateKeyUpdate
                || _dialect.SupportsOnConflictClause
                || _dialect.SupportsReturning
                || _dialect.AllowsParserInsertSelectUpsertSuffix;
            insertSelect = ParseSelectQuery();
            _allowInsertSelectSuffixBoundary = false;
        }

        // Must be VALUES(...), VALUE(...), or SELECT...
        if (valuesRaw.Count == 0 && insertSelect is null)
            throw new InvalidOperationException("Invalid INSERT statement: expected VALUES, VALUE, or SELECT.");

        // INSERT INTO t () VALUES () -> default row (aceito)
        // INSERT INTO t () VALUES (1) / INSERT INTO t () SELECT ... -> inválido
        if (hasExplicitColumnList && cols.Count == 0)
        {
            if (insertSelect is not null)
                throw new InvalidOperationException("INSERT column list requires at least one column; empty list cannot be used with SELECT source.");

            if (valuesRaw.Any(row => row.Count != 0))
                throw new InvalidOperationException("INSERT column list requires at least one column, or use VALUES () for default-row semantics.");
        }

        ValidateInsertValuesColumnArity(cols, valuesRaw);

        // ON DUPLICATE KEY UPDATE
        var onDup = isReplace ? null : ParseOnDuplicated();
        var returning = ParseOptionalReturningItems(ReturningClauseTarget.Insert);

        EnsureStatementEnd(SqlConst.INSERT);

        return new SqlInsertQuery
        {
            Table = table,
            Columns = cols,
            ValuesRaw = valuesRaw,
            ValuesExpr = valuesExpr,
            InsertSelect = insertSelect,
            Returning = returning,
            IsReplace = isReplace,
            HasOnDuplicateKeyUpdate = (onDup != null),
            OnDupAssigns = onDup?.Assignments.Select(a => (a.Column, a.ValueRaw)).ToList() ?? [],
            OnDupAssignsParsed = onDup?.Assignments.ToList() ?? [],
            IsOnConflictDoNothing = onDup?.IsDoNothing == true || (insertIgnore && onDup is null),
            OnConflictUpdateWhereRaw = onDup?.UpdateWhereRaw,
            OnConflictUpdateWhereExpr = onDup?.UpdateWhereExpr
        };
    }

    private bool ConsumeOptionalInsertModifiers(bool isReplace)
    {
        var sawIgnore = false;
        while (true)
        {
            if (IsWord(Peek(), SqlConst.LOW_PRIORITY)
                || IsWord(Peek(), SqlConst.DELAYED)
                || (!isReplace && IsWord(Peek(), SqlConst.HIGH_PRIORITY))
                || (!isReplace && IsWord(Peek(), SqlConst.IGNORE)))
            {
                if (!isReplace && IsWord(Peek(), SqlConst.IGNORE))
                    sawIgnore = true;
                Consume();
                continue;
            }

            break;
        }

        return sawIgnore;
    }

    private void ConsumeOptionalPartitionClause()
    {
        if (!IsWord(Peek(), SqlConst.PARTITION))
            return;

        Consume(); // PARTITION
        if (!IsSymbol(Peek(), "("))
            throw new InvalidOperationException("INSERT PARTITION clause requires a partition list.");

        _ = ReadBalancedParenRawTokens();
    }

    private void ParseInsertValuesRows(List<List<string>> valuesRaw, List<List<SqlExpr?>> valuesExpr)
    {
        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
            {
                if (valuesRaw.Count == 0)
                    throw new InvalidOperationException("INSERT VALUES requires at least one row.");

                return;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException("INSERT VALUES has an unexpected comma before row (found ',').");

            if (!IsSymbol(Peek(), "("))
            {
                if (valuesRaw.Count == 0)
                    throw new InvalidOperationException("Invalid INSERT statement: expected VALUES row tuple.");

                throw new InvalidOperationException("INSERT VALUES must separate row tuples with commas.");
            }

            var rawBlock = ReadBalancedParenRawTokens();
            var rowValuesRaw = SplitRawByComma(rawBlock);

            if (rowValuesRaw.Any(v => string.IsNullOrWhiteSpace(v)))
                throw new InvalidOperationException("INSERT VALUES row has an empty expression between commas.");

            var rowValues = rowValuesRaw;
            var rowNumber = valuesRaw.Count + 1;

            valuesRaw.Add([.. rowValues.Select(NormalizeInsertValueRaw)]);
            valuesExpr.Add(ParseInsertValuesRowExpressions(rowValues, rowNumber));

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
                    throw new InvalidOperationException("INSERT VALUES has a trailing comma without row tuple.");

                continue;
            }

            if (IsSymbol(Peek(), "("))
                throw new InvalidOperationException("INSERT VALUES must separate row tuples with commas.");

            return;
        }
    }

    private static void ValidateInsertValuesColumnArity(IReadOnlyList<string> cols, IReadOnlyList<List<string>> valuesRaw)
    {
        if (valuesRaw.Count == 0)
            return;

        var expectedRowArity = valuesRaw[0].Count;
        for (var rowIndex = 1; rowIndex < valuesRaw.Count; rowIndex++)
        {
            var row = valuesRaw[rowIndex];
            if (row.Count == expectedRowArity)
                continue;

            throw new InvalidOperationException(
                $"INSERT VALUES row {rowIndex + 1} expression count ({row.Count}) does not match row 1 expression count ({expectedRowArity}).");
        }

        if (cols.Count == 0)
            return;

        for (var rowIndex = 0; rowIndex < valuesRaw.Count; rowIndex++)
        {
            var row = valuesRaw[rowIndex];
            if (row.Count == 0)
                throw new InvalidOperationException("INSERT VALUES row requires at least one expression.");

            if (row.Count == cols.Count)
                continue;

            throw new InvalidOperationException(
                $"INSERT column count ({cols.Count}) does not match VALUES row {rowIndex + 1} expression count ({row.Count}).");
        }
    }

    private List<string> ParseCols()
    {
        if (!IsSymbol(Peek(), "("))
            return [];

        var cols = new List<string>();
        Consume(); // (

        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
                throw new InvalidOperationException("INSERT column list was not closed correctly.");

            if (IsSymbol(Peek(), ")"))
            {
                Consume();
                return cols;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException("INSERT column list has an unexpected comma before column.");

            cols.Add(ExpectIdentifier());

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsSymbol(Peek(), ")"))
                    throw new InvalidOperationException("INSERT column list has a trailing comma without column.");

                continue;
            }

            if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
                throw new InvalidOperationException("INSERT column list was not closed correctly.");

            if (!IsSymbol(Peek(), ")"))
                throw new InvalidOperationException("INSERT column list must separate columns with commas.");
        }
    }

    private List<SqlExpr?> ParseInsertValuesRowExpressions(IReadOnlyList<string> rowValues, int rowNumber)
    {
        var parsed = new List<SqlExpr?>(rowValues.Count);

        for (var exprIndex = 0; exprIndex < rowValues.Count; exprIndex++)
        {
            var raw = rowValues[exprIndex];
            try
            {
                parsed.Add(SqlExpressionParser.ParseScalar(raw, _dialect));
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException(
                    $"INSERT VALUES row {rowNumber} expression {exprIndex + 1} is invalid.",
                    ex);
            }
        }

        return parsed;
    }

    private SqlOnDuplicateKeyUpdate? ParseOnDuplicated()
    {
        if (!IsWord(Peek(), SqlConst.ON))
            return null;

        var next = Peek(1);

        // MySQL: ON DUPLICATE KEY UPDATE
        if (IsWord(next, SqlConst.DUPLICATE))
        {
            if (!_dialect.SupportsOnDuplicateKeyUpdate && !_dialect.AllowsParserInsertSelectUpsertSuffix)
            {
                var gateException = SqlUnsupported.ForOnDuplicateKeyUpdateClause(_dialect);
                throw gateException;
            }

            Consume(); // ON
            ExpectWord(SqlConst.DUPLICATE);
            ExpectWord("KEY");
            ExpectWord(SqlConst.UPDATE);

            if (IsWord(Peek(), SqlConst.WHERE))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support a WHERE clause (found '{Peek().Text}').");

            if (IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support table-source clauses after assignments (found '{Peek().Text}').");

            var assigns = ParseAssignmentsList().AsReadOnly();

            if (IsWord(Peek(), SqlConst.WHERE))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support a WHERE clause (found '{Peek().Text}').");

            if (IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support table-source clauses after assignments (found '{Peek().Text}').");

            return new SqlOnDuplicateKeyUpdate(assigns);
        }

        // PostgreSQL: ON CONFLICT (...) DO UPDATE SET ...  |  ON CONFLICT DO NOTHING
        if (IsWord(next, "CONFLICT"))
        {
            if (!_dialect.SupportsOnConflictClause && !_dialect.AllowsParserInsertSelectUpsertSuffix)
            {
                var gateException = SqlUnsupported.ForOnConflictClause(_dialect);
                throw gateException;
            }

            Consume(); // ON
            ExpectWord("CONFLICT");

            // Target opcional (PostgreSQL):
            // - (col1, col2, ...)
            // - ON CONSTRAINT constraint_name
            // - [target] WHERE predicate
            ParsePostgreSqlOnConflictTarget();

            if (!IsWord(Peek(), SqlConst.DO))
                throw new InvalidOperationException(
                    $"ON CONFLICT requires DO NOTHING or DO UPDATE SET (found '{DescribeFoundToken(Peek())}').");

            Consume(); // DO

            if (IsWord(Peek(), SqlConst.NOTHING))
            {
                Consume();

                var afterDoNothing = Peek();
                if (!IsEnd(afterDoNothing) && !IsSymbol(afterDoNothing, ";") && !IsWord(afterDoNothing, SqlConst.RETURNING))
                    throw new InvalidOperationException(
                        $"ON CONFLICT DO NOTHING does not support additional clauses before RETURNING (found '{afterDoNothing.Text}').");

                return new SqlOnDuplicateKeyUpdate([], IsDoNothing: true);
            }

            if (!IsWord(Peek(), SqlConst.UPDATE))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO must be followed by NOTHING or UPDATE SET (found '{DescribeFoundToken(Peek())}').");

            Consume(); // UPDATE

            if (!IsWord(Peek(), SqlConst.SET))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE requires SET assignments (found '{DescribeFoundToken(Peek())}').");

            Consume(); // SET

            if (IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE does not support table-source clauses after assignments (found '{Peek().Text}').");

            var assigns = ParseOnConflictUpdateAssignments();
            string? updateWhereRaw = null;
            SqlExpr? updateWhereExpr = null;

            if (IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE does not support table-source clauses after assignments (found '{Peek().Text}').");

            // PostgreSQL permite: DO UPDATE SET ... WHERE <predicate>.
            // O predicado é validado e materializado na AST para uso no executor.
            if (IsWord(Peek(), SqlConst.WHERE))
            {
                Consume();
                (updateWhereRaw, updateWhereExpr) = ParseOnConflictWherePredicate(
                    ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING),
                    "ON CONFLICT DO UPDATE WHERE",
                    Peek());
            }

            return new SqlOnDuplicateKeyUpdate(assigns, UpdateWhereRaw: updateWhereRaw, UpdateWhereExpr: updateWhereExpr);
        }

        return null;
    }

    private void ParsePostgreSqlOnConflictTarget()
    {
        // ON CONFLICT ON CONSTRAINT constraint_name
        if (IsWord(Peek(), SqlConst.ON) && IsWord(Peek(1), "CONSTRAINT"))
        {
            Consume(); // ON
            Consume(); // CONSTRAINT

            var constraint = Peek();
            if (constraint.Kind != SqlTokenKind.Identifier || IsMissingOnConflictConstraintNameToken(constraint))
                throw new InvalidOperationException(
                    $"ON CONFLICT ON CONSTRAINT requires a constraint name (found '{DescribeFoundToken(constraint)}').");

            Consume(); // constraint name

            if (IsWord(Peek(), SqlConst.WHERE))
            {
                Consume();
                _ = ParseOnConflictWherePredicate(
                    ReadClauseTextUntilTopLevelStop(SqlConst.DO),
                    "ON CONFLICT target WHERE",
                    Peek());
            }
            return;
        }

        // ON CONFLICT (index_expr [, ...]) [WHERE predicate]
        if (IsSymbol(Peek(), "("))
        {
            Consume(); // (
            ParseOnConflictTargetItems();

            if (IsWord(Peek(), SqlConst.WHERE))
            {
                Consume();
                _ = ParseOnConflictWherePredicate(
                    ReadClauseTextUntilTopLevelStop(SqlConst.DO),
                    "ON CONFLICT target WHERE",
                    Peek());
            }
        }
    }

    private (string Raw, SqlExpr Expr) ParseOnConflictWherePredicate(string raw, string clauseLabel, SqlToken foundToken)
    {
        var normalized = NormalizeClauseText(raw);
        if (string.IsNullOrWhiteSpace(normalized))
            throw new InvalidOperationException(
                $"{clauseLabel} requires a predicate (found '{DescribeFoundToken(foundToken)}').");

        try
        {
            var expr = SqlExpressionParser.ParseWhere(normalized, _dialect);
            return (normalized, expr);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException($"{clauseLabel} predicate is invalid.", ex);
        }
        catch (NotSupportedException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"{clauseLabel} predicate is invalid.", ex);
        }
    }

    private void ParseOnConflictTargetItems()
    {
        var items = 0;

        while (true)
        {
            if (IsEnd(Peek()))
                throw new InvalidOperationException(
                    $"ON CONFLICT target was not closed correctly (found '{DescribeFoundToken(Peek())}').");

            if (IsSymbol(Peek(), ")"))
            {
                if (items == 0)
                    throw new InvalidOperationException(
                        $"ON CONFLICT target requires at least one expression (found '{DescribeFoundToken(Peek())}').");

                Consume();
                return;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"ON CONFLICT target has an unexpected comma before expression (found '{DescribeFoundToken(Peek())}').");

            var raw = ReadRawExpressionUntilCommaOrRightParen().Trim();
            if (string.IsNullOrWhiteSpace(raw))
                throw new InvalidOperationException("ON CONFLICT target requires at least one expression.");

            try
            {
                _ = SqlExpressionParser.ParseScalar(raw, _dialect);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("ON CONFLICT target expression is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("ON CONFLICT target expression is invalid.", ex);
            }
            items++;

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsSymbol(Peek(), ")"))
                    throw new InvalidOperationException(
                        $"ON CONFLICT target has a trailing comma without expression (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            if (!IsSymbol(Peek(), ")"))
                throw new InvalidOperationException(
                    $"ON CONFLICT target must separate expressions with commas (found '{DescribeFoundToken(Peek())}').");
        }
    }

    private static bool IsMissingOnConflictConstraintNameToken(SqlToken token)
    {
        if (token.Kind == SqlTokenKind.EndOfFile)
            return true;

        if (token.Kind == SqlTokenKind.Symbol && token.Text == ";")
            return true;

        return token.Text.Equals(SqlConst.DO, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.NOTHING, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.UPDATE, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.SET, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.WHERE, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.RETURNING, StringComparison.OrdinalIgnoreCase);
    }


    private List<SqlAssignment> ParseOnConflictUpdateAssignments()
    {
        var list = new List<SqlAssignment>();

        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
            {
                if (list.Count == 0)
                    throw new InvalidOperationException(
                        $"ON CONFLICT DO UPDATE SET requires at least one assignment (found '{DescribeFoundToken(Peek())}').");

                return list;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE SET has an unexpected comma before assignment (found '{DescribeFoundToken(Peek())}').");

            if (IsWord(Peek(), SqlConst.SET))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE SET must not repeat SET keyword (found '{DescribeFoundToken(Peek())}').");

            var col = ExpectIdentifierWithDots();
            ExpectAssignmentEquals("ON CONFLICT DO UPDATE SET", col);

            var exprRaw = ReadClauseTextUntilTopLevelStop(",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";").Trim();
            if (string.IsNullOrWhiteSpace(exprRaw))
                throw new InvalidOperationException($"ON CONFLICT DO UPDATE SET assignment for '{col}' requires an expression.");

            SqlExpr expr;
            try
            {
                expr = SqlExpressionParser.ParseScalar(exprRaw, _dialect);
            }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("ON CONFLICT DO UPDATE SET must separate assignments with commas.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"ON CONFLICT DO UPDATE SET assignment for '{col}' has an invalid expression.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"ON CONFLICT DO UPDATE SET assignment for '{col}' has an invalid expression.", ex);
            }
            list.Add(new SqlAssignment(col, exprRaw, expr));

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
                    throw new InvalidOperationException(
                        $"ON CONFLICT DO UPDATE SET has a trailing comma without assignment (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
                return list;

            throw new InvalidOperationException("ON CONFLICT DO UPDATE SET must separate assignments with commas.");
        }
    }

    private SqlUpdateQuery ParseUpdate()
    {
        Consume(); // UPDATE
        var firstTablePart = ExpectIdentifier();
        string? tableDbName = null;
        var tableNameOnly = firstTablePart;
        if (IsSymbol(Peek(), ".") || Peek().Text == ".")
        {
            Consume();
            tableDbName = tableNameOnly;
            tableNameOnly = ExpectIdentifier();
        }

        var table = new SqlTableSource(
            tableDbName,
            tableNameOnly,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);

        if (Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
        {
            var maybeAlias = Peek();
            if (!IsWord(maybeAlias, SqlConst.SET)
                && !IsWord(maybeAlias, SqlConst.FROM)
                && !IsWord(maybeAlias, SqlConst.WHERE)
                && !IsWord(maybeAlias, SqlConst.RETURNING)
                && !IsJoinStart(maybeAlias)
                && !IsEnd(maybeAlias)
                && !IsSymbol(maybeAlias, ";"))
            {
                table = table with { Alias = Consume().Text };
            }
        }

        // MySQL: UPDATE <table> [alias] JOIN (...) ... SET ...
        var hasJoin = false;

        // Se vier JOIN antes do SET, pulamos os tokens do JOIN aqui (as estratégias smart usam RawSql)
        if (IsJoinStart(Peek()))
        {
            if (!_dialect.SupportsUpdateJoinFromSubquerySyntax)
                throw SqlUnsupported.ForDialect(_dialect, "UPDATE ... JOIN (subquery)");

            hasJoin = true;
            SkipUntilTopLevelWord(SqlConst.SET);
        }

        ExpectWord(SqlConst.SET);

        var assignsList = ParseUpdateAssignmentsList();
        var setList = assignsList.ConvertAll(a => (a.Column, a.ValueRaw));

        // SQL Server/PostgreSQL: UPDATE <alias> SET ... FROM ... [WHERE ...]
        if (IsWord(Peek(), SqlConst.FROM))
        {
            hasJoin = true;
            Consume(); // FROM
            if (HasTopLevelWordInRemaining(SqlConst.WHERE))
                SkipUntilTopLevelWord(SqlConst.WHERE);
            else
                while (!IsEnd(Peek()))
                    Consume();
        }

        // Fallback: algumas variações de DELETE ... JOIN podem deixar o JOIN
        // pendente após o cabeçalho. Consumimos a cláusula aqui para não
        // falhar no EnsureStatementEnd, preservando parse de WHERE/RETURNING.
        if (IsJoinStart(Peek()))
        {
            hasJoin = true;
            if (HasTopLevelWordInRemaining(SqlConst.WHERE))
                SkipUntilTopLevelWord(SqlConst.WHERE);
            else if (HasTopLevelWordInRemaining(SqlConst.RETURNING))
                SkipUntilTopLevelWord(SqlConst.RETURNING);
            else
                while (!IsEnd(Peek()))
                    Consume();
        }

        // Guardrail: se algum formato de DELETE ... JOIN deixou tokens de JOIN
        // pendentes no topo, consumimos até WHERE/RETURNING/fim para evitar
        // erro de "Unexpected token after DELETE".
        if (IsJoinStart(Peek()))
        {
            hasJoin = true;
            if (HasTopLevelWordInRemaining(SqlConst.WHERE))
                SkipUntilTopLevelWord(SqlConst.WHERE);
            else if (HasTopLevelWordInRemaining(SqlConst.RETURNING))
                SkipUntilTopLevelWord(SqlConst.RETURNING);
            else
                while (!IsEnd(Peek()))
                    Consume();
        }

        string? whereRaw = null;
        if (IsWord(Peek(), SqlConst.WHERE))
        {
            Consume(); // WHERE
            whereRaw = NormalizeClauseText(ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING));
            if (string.IsNullOrWhiteSpace(whereRaw))
                throw new InvalidOperationException(
                    $"UPDATE WHERE requires a predicate (found '{DescribeFoundToken(Peek())}').");
        }
        var returning = ParseOptionalReturningItems(ReturningClauseTarget.Update);

        var setParsed = assignsList;
        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = SqlExpressionParser.ParseWhere(whereRaw!, _dialect); }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("Unexpected token after UPDATE in WHERE clause.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("UPDATE WHERE predicate is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("UPDATE WHERE predicate is invalid.", ex);
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        EnsureStatementEnd(SqlConst.UPDATE);

        return new SqlUpdateQuery
        {
            Table = table,
            Set = setList,
            SetParsed = setParsed,
            WhereRaw = whereRaw,
            Where = whereExpr,
            Returning = returning,
            // Só o "!= null" importa para acionar a estratégia smart; o SQL bruto está em RawSql.
            UpdateFromSelect = hasJoin
                ? new SqlSelectQuery([], false, [], [], null, [], null, [], null)
                : null
        };
    }

    private SqlDeleteQuery ParseDelete()
    {
        Consume(); // DELETE

        // MySQL suporta:
        // 1) DELETE FROM t WHERE ...
        // 2) DELETE a FROM t a JOIN (...) s ON ...
        // Para (2) precisamos guardar a tabela real (t), não o alias (a).

        SqlTableSource table;
        bool hasJoin = false;

        if (IsWord(Peek(), SqlConst.FROM))
        {
            // DELETE FROM t WHERE ...
            Consume();
            table = ParseTableSource(allowFunctionSource: false);

            if (IsWord(Peek(), SqlConst.USING))
            {
                hasJoin = true;
                Consume(); // USING
                if (HasTopLevelWordInRemaining(SqlConst.WHERE))
                    SkipUntilTopLevelWord(SqlConst.WHERE);
                else
                    while (!IsEnd(Peek()))
                        Consume();
            }
        }
        else
        {
            // DELETE sem FROM (alguns dialetos permitem, outros não).
            // a) DELETE t WHERE ...
            // b) DELETE a FROM t a JOIN (...) s ON ...
            // Para (b) precisamos guardar a tabela real (t), não o alias (a).
            var allowsTargetAlias = (_dialect.SupportsDeleteTargetAlias || _dialect.AllowsParserDeleteWithoutFromCompatibility)
                && Peek().Kind == SqlTokenKind.Identifier
                && IsWord(Peek(1), SqlConst.FROM);
            if (!_dialect.SupportsDeleteWithoutFrom && !_dialect.AllowsParserDeleteWithoutFromCompatibility && !allowsTargetAlias)
                throw SqlUnsupported.ForDeleteWithoutFrom(_dialect);

            var first = ParseTableSource(allowFunctionSource: false); // pode ser tabela ou alvo

            if (IsWord(Peek(), SqlConst.FROM))
            {
                if (!_dialect.SupportsDeleteTargetAlias && !_dialect.AllowsParserDeleteWithoutFromCompatibility)
                    throw SqlUnsupported.ForDeleteTargetAliasFrom(_dialect);

                // DELETE <alias> FROM <table> <alias> JOIN ...
                Consume(); // FROM
                table = ParseTableSource(allowFunctionSource: false); // ex: users

                // alias pós-tabela (ex: users u)
                if (Peek().Kind == SqlTokenKind.Identifier && !IsWord(Peek(), SqlConst.WHERE) && !IsJoinStart(Peek()))
                    Consume();

                if (IsJoinStart(Peek()))
                {
                    hasJoin = true;
                    // A estratégia smart faz o parsing completo a partir do RawSql.
                    // Aqui só precisamos consumir a cláusula JOIN para evitar token sobrando no fim.
                    if (HasTopLevelWordInRemaining(SqlConst.WHERE))
                        SkipUntilTopLevelWord(SqlConst.WHERE);
                    else if (HasTopLevelWordInRemaining(SqlConst.RETURNING))
                        SkipUntilTopLevelWord(SqlConst.RETURNING);
                    else
                        while (!IsEnd(Peek()))
                            Consume();
                }
            }
            else
            {
                // DELETE <table> WHERE ...
                table = first;

                // alias opcional (DELETE users u WHERE ...) - tolerado
                if (Peek().Kind == SqlTokenKind.Identifier &&
                    !IsWord(Peek(), SqlConst.WHERE) &&
                    !IsWord(Peek(), SqlConst.ORDER) &&
                    !IsWord(Peek(), SqlConst.LIMIT) &&
                    !IsJoinStart(Peek()))
                {
                    Consume();
                }
            }
        }

        string? whereRaw = null;

        if (IsWord(Peek(), SqlConst.WHERE))
        {
            Consume();
            whereRaw = NormalizeClauseText(ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING));
            if (string.IsNullOrWhiteSpace(whereRaw))
                throw new InvalidOperationException(
                    $"DELETE WHERE requires a predicate (found '{DescribeFoundToken(Peek())}').");
        }
        var returning = ParseOptionalReturningItems(ReturningClauseTarget.Delete);

        if (hasJoin && returning.Count > 0 && !_dialect.SupportsDeleteReturningWithJoin)
            throw new InvalidOperationException("RETURNING cannot be used with multi-table DELETE statements in this dialect.");

        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = SqlExpressionParser.ParseWhere(whereRaw!, _dialect); }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("Unexpected token after DELETE in WHERE clause.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("DELETE WHERE predicate is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("DELETE WHERE predicate is invalid.", ex);
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        EnsureStatementEnd(SqlConst.DELETE);

        return new SqlDeleteQuery
        {
            Table = table,
            WhereRaw = whereRaw,
            Where = whereExpr,
            Returning = returning,
            DeleteFromSelect = hasJoin
                ? new SqlSelectQuery([], false, [], [], null, [], null, [], null)
                : null
        };
    }

    private SqlMergeQuery ParseMerge()
    {
        Consume(); // MERGE
        if (IsWord(Peek(), SqlConst.INTO)) Consume();

        // target table + alias (ex: stats target)
        var target = ParseTableSource(allowFunctionSource: false);

        if (!HasTopLevelWordInRemaining(SqlConst.USING))
            throw new InvalidOperationException("MERGE requer cláusula USING. Ex.: MERGE INTO <target> USING <source> ON ...");

        if (!HasTopLevelWordInRemaining(SqlConst.ON))
            throw new InvalidOperationException("MERGE requer cláusula ON. Ex.: MERGE INTO <target> USING <source> ON <condição>");

        if (!HasTopLevelMergeWhenClause())
            throw new InvalidOperationException("MERGE requer ao menos uma cláusula WHEN (MATCHED/NOT MATCHED).");

        // O resto do MERGE é grande demais pra agora.
        // Só avançamos tokens até o fim pra não deixar lixo se você evoluir o parser.
        while (Peek().Kind != SqlTokenKind.EndOfFile)
            Consume();

        return new SqlMergeQuery
        {
            Table = target
        };
    }

    private bool HasTopLevelWordInRemaining(string word)
    {
        var depth = 0;
        for (var idx = _i; idx < _toks.Count; idx++)
        {
            var t = _toks[idx];
            if (t.Kind == SqlTokenKind.EndOfFile)
                break;

            if (IsSymbol(t, "(")) { depth++; continue; }
            if (IsSymbol(t, ")")) { depth = Math.Max(0, depth - 1); continue; }

            if (depth == 0 && IsWord(t, word))
                return true;
        }

        return false;
    }

    private bool HasTopLevelMergeWhenClause()
    {
        var depth = 0;
        for (var idx = _i; idx < _toks.Count; idx++)
        {
            var t = _toks[idx];
            if (t.Kind == SqlTokenKind.EndOfFile)
                break;

            if (IsSymbol(t, "("))
            {
                depth++;
                continue;
            }

            if (IsSymbol(t, ")"))
            {
                depth = Math.Max(0, depth - 1);
                continue;
            }

            if (depth != 0 || !IsWord(t, SqlConst.WHEN))
                continue;

            var next = PeekTokenFrom(idx + 1);
            if (IsWord(next, SqlConst.MATCHED))
                return true;

            if (IsWord(next, SqlConst.NOT) && IsWord(PeekTokenFrom(idx + 2), SqlConst.MATCHED))
                return true;
        }

        return false;
    }

    private SqlToken PeekTokenFrom(int index)
        => (index >= 0 && index < _toks.Count) ? _toks[index] : SqlToken.EOF;

    // ------------------------------------------------------------
    // SELECT (Lógica já existente, mantida e integrada)
    // ------------------------------------------------------------

    /// <summary>
    /// EN: Implements ParseSelectOrUnionQuery.
    /// PT: Implementa ParseSelectOrUnionQuery.
    /// </summary>
    private SqlQueryBase ParseSelectOrUnionQuery()
    {
        var first = ParseSelectQuery(allowOrderByAndLimit: false);

        if (!IsWord(Peek(), SqlConst.UNION))
        {
            var orderBy = TryParseOrderBy();
            var rowLimit = TryParseRowLimitTail(orderBy.Count > 0);
            var forJson = TryParseForJsonClause();
            rowLimit ??= first.RowLimit;
            TryConsumeQueryHintOption();
            ExpectEndOrUnionBoundary();

            return first with
            {
                OrderBy = orderBy,
                RowLimit = rowLimit,
                ForJson = forJson
            };
        }

        var parts = new List<SqlSelectQuery> { first };
        var allFlags = new List<bool>();

        while (IsWord(Peek(), SqlConst.UNION))
        {
            Consume();
            var isAll = false;
            if (IsWord(Peek(), SqlConst.ALL))
            {
                Consume();
                isAll = true;
            }

            allFlags.Add(isAll);
            parts.Add(ParseSelectQuery(allowCtes: false, allowOrderByAndLimit: false));
        }

        var unionOrderBy = TryParseOrderBy();
        var unionRowLimit = TryParseRowLimitTail(unionOrderBy.Count > 0);
        TryConsumeQueryHintOption();
        ExpectEndOrUnionBoundary();

        return new SqlUnionQuery(parts, allFlags, unionOrderBy, unionRowLimit);
    }

    /// <summary>
    /// EN: Parses a SELECT query with optional control over CTE parsing and ORDER BY/pagination tail parsing.
    /// PT: Faz o parsing de uma query SELECT com controle opcional de parsing de CTE e cauda ORDER BY/paginação.
    /// </summary>
    /// <param name="allowCtes">EN: When true, WITH/CTE clauses are parsed before SELECT. PT: Quando verdadeiro, cláusulas WITH/CTE são parseadas antes do SELECT.</param>
    /// <param name="allowOrderByAndLimit">EN: When true, ORDER BY and row-limit tails are parsed. PT: Quando verdadeiro, caudas ORDER BY e limite de linhas são parseadas.</param>
    /// <returns>EN: Parsed SELECT AST node. PT: Nó AST de SELECT parseado.</returns>
    public SqlSelectQuery ParseSelectQuery(bool allowCtes = true, bool allowOrderByAndLimit = true)
    {
        var ctes = allowCtes ? TryParseCtes() : [];

        ExpectWord(SqlConst.SELECT);
        if (IsWord(Peek(), SqlConst.SELECT))
            throw new InvalidOperationException("invalid: duplicated SELECT keyword");
        var distinct = TryParseDistinct();
        var top = TryParseTop();
        TryParseSelectModifiers();
        var selectItems = ParseSelectItemsWithValidation();
        var table = ParseFromOrDual();
        var joins = ParseJoins(table);
        while (IsSymbol(Peek(), ","))
        {
            Consume();
            var commaTable = TryParseTableTransforms(ParseTableSource());
            joins.Add(new SqlJoin(
                commaTable.Derived is not null || commaTable.DerivedUnion is not null || commaTable.TableFunction is not null
                    ? SqlJoinType.CrossApply
                    : SqlJoinType.Cross,
                commaTable,
                new LiteralExpr(true)));
        }
        var where = TryParseWhereExpr();
        var groupBy = TryParseGroupBy();
        var having = TryParseHavingExpr();
        var orderBy = allowOrderByAndLimit ? TryParseOrderBy() : [];
        var rowLimit = allowOrderByAndLimit ? TryParseRowLimitTail(orderBy.Count > 0) : null;
        var forJson = allowOrderByAndLimit ? TryParseForJsonClause() : null;
        if (allowOrderByAndLimit)
            TryConsumeQueryHintOption();
        if (top is not null)
            rowLimit ??= top;

        if (allowOrderByAndLimit)
        {
            ExpectEndOrUnionBoundary();
        }
        else
        {
            var t = Peek();
            if (!IsEnd(t)
                && !IsWord(t, SqlConst.UNION)
                && !IsWord(t, SqlConst.ORDER)
                && !IsWord(t, SqlConst.LIMIT)
                && !IsWord(t, SqlConst.OFFSET)
                && !IsWord(t, SqlConst.FETCH)
                && !IsWord(t, SqlConst.FOR)
                && !IsWord(t, SqlConst.OPTION)
                && !IsSymbol(t, ";"))
            {
                throw new InvalidOperationException($"Token inesperado após SELECT: {t.Kind} '{t.Text}'");
            }
        }

        var query = new SqlSelectQuery(
            Ctes: ctes,
            Distinct: distinct,
            SelectItems: selectItems,
            Joins: joins,
            Where: where,
            OrderBy: orderBy,
            RowLimit: rowLimit,
            GroupBy: groupBy,
            Having: having,
            ForJson: forJson
        )
        {
            Table = table
        };

        if (_dialect is AutoSqlDialect)
        {
            query = DialectNormalizer.NormalizeAutoSelect(
                query,
                _autoSyntaxFeatures,
                ResolveParameterInt);
        }

        return query;
    }

    private void TryParseSelectModifiers()
    {
        while (IsWord(Peek(), SqlConst.SQL_CALC_FOUND_ROWS))
        {
            if (!_dialect.SupportsSqlCalcFoundRowsModifier)
                throw SqlUnsupported.ForDialect(_dialect, "SELECT modifier SQL_CALC_FOUND_ROWS");

            Consume();
        }
    }

    // ------------------------------------------------------------
    // Helpers de Token (Generalizados)
    // ------------------------------------------------------------

    private string ExpectIdentifierWithDots()
    {
        var sb = new StringBuilder();
        sb.Append(ExpectIdentifier());
        while (IsSymbol(Peek(), "."))
        {
            Consume(); // .
            sb.Append('.').Append(ExpectIdentifier());
        }
        return sb.ToString();
    }

    private void ExpectAssignmentEquals(string clauseLabel, string column)
    {
        try
        {
            ExpectSymbol("=");
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                $"{clauseLabel} assignment for '{column}' requires '=' between column and expression.",
                ex);
        }
    }

    private static string DescribeFoundToken(SqlToken token)
        => IsEnd(token) ? "<end-of-statement>" : token.Text;

    private static string DescribeFoundTokenFromRaw(string raw)
    {
        var trimmed = raw.TrimStart();
        if (string.IsNullOrWhiteSpace(trimmed))
            return "<end-of-statement>";

        var tokenEnd = 0;
        while (tokenEnd < trimmed.Length && !char.IsWhiteSpace(trimmed[tokenEnd]))
            tokenEnd++;

        return trimmed[..tokenEnd];
    }

    private List<SqlAssignment> ParseUpdateAssignmentsList()
    {
        var list = new List<SqlAssignment>();

        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.RETURNING))
            {
                if (list.Count == 0)
                    throw new InvalidOperationException(
                        $"UPDATE SET requires at least one assignment (found '{DescribeFoundToken(Peek())}').");

                return list;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"UPDATE SET has an unexpected comma before assignment (found '{DescribeFoundToken(Peek())}').");

            if (IsWord(Peek(), SqlConst.SET))
                throw new InvalidOperationException(
                    $"UPDATE SET must not repeat SET keyword (found '{DescribeFoundToken(Peek())}').");

            var col = ExpectIdentifierWithDots();
            ExpectAssignmentEquals("UPDATE SET", col);

            var exprRaw = ReadClauseTextUntilTopLevelStop(",", SqlConst.WHERE, SqlConst.FROM, SqlConst.RETURNING, ";").Trim();
            if (string.IsNullOrWhiteSpace(exprRaw))
                throw new InvalidOperationException($"UPDATE SET assignment for '{col}' requires an expression.");

            SqlExpr expr;
            try
            {
                expr = SqlExpressionParser.ParseScalar(exprRaw, _dialect);
            }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("UPDATE SET must separate assignments with commas.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"UPDATE SET assignment for '{col}' has an invalid expression.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"UPDATE SET assignment for '{col}' has an invalid expression.", ex);
            }
            list.Add(new SqlAssignment(col, exprRaw, expr));

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.RETURNING))
                    throw new InvalidOperationException(
                        $"UPDATE SET has a trailing comma without assignment (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.RETURNING))
                return list;

            throw new InvalidOperationException("UPDATE SET must separate assignments with commas.");
        }
    }

    private List<SqlAssignment> ParseAssignmentsList()
    {
        var list = new List<SqlAssignment>();
        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
            {
                if (list.Count == 0)
                    throw new InvalidOperationException(
                        $"ON DUPLICATE KEY UPDATE requires at least one assignment (found '{DescribeFoundToken(Peek())}').");

                return list;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE has an unexpected comma before assignment (found '{DescribeFoundToken(Peek())}').");

            if (IsWord(Peek(), SqlConst.SET))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE must not include SET keyword; provide assignments directly (found '{DescribeFoundToken(Peek())}').");

            var col = ExpectIdentifierWithDots();
            ExpectAssignmentEquals("ON DUPLICATE KEY UPDATE", col);

            var exprRaw = ReadClauseTextUntilTopLevelStop(",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";").Trim();
            if (string.IsNullOrWhiteSpace(exprRaw))
                throw new InvalidOperationException($"ON DUPLICATE KEY UPDATE assignment for '{col}' requires an expression.");

            SqlExpr expr;
            try
            {
                expr = SqlExpressionParser.ParseScalar(exprRaw, _dialect);
            }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("ON DUPLICATE KEY UPDATE must separate assignments with commas.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"ON DUPLICATE KEY UPDATE assignment for '{col}' has an invalid expression.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"ON DUPLICATE KEY UPDATE assignment for '{col}' has an invalid expression.", ex);
            }
            list.Add(new SqlAssignment(col, NormalizeInsertValueRaw(exprRaw), expr));

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
                    throw new InvalidOperationException(
                        $"ON DUPLICATE KEY UPDATE has a trailing comma without assignment (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING))
                return list;

            throw new InvalidOperationException("ON DUPLICATE KEY UPDATE must separate assignments with commas.");
        }
    }

    private List<SqlAssignment> ParseReplaceSetAssignments()
    {
        var list = new List<SqlAssignment>();
        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING) || IsWord(Peek(), SqlConst.ON))
            {
                if (list.Count == 0)
                    throw new InvalidOperationException(
                        $"REPLACE SET requires at least one assignment (found '{DescribeFoundToken(Peek())}').");

                return list;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"REPLACE SET has an unexpected comma before assignment (found '{DescribeFoundToken(Peek())}').");

            if (IsWord(Peek(), SqlConst.SET))
                throw new InvalidOperationException(
                    $"REPLACE SET must not include SET keyword twice (found '{DescribeFoundToken(Peek())}').");

            var col = ExpectIdentifierWithDots();
            ExpectAssignmentEquals("REPLACE SET", col);

            var exprRaw = ReadClauseTextUntilTopLevelStop(",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";").Trim();
            if (string.IsNullOrWhiteSpace(exprRaw))
                throw new InvalidOperationException($"REPLACE SET assignment for '{col}' requires an expression.");

            SqlExpr expr;
            try
            {
                expr = SqlExpressionParser.ParseScalar(exprRaw, _dialect);
            }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("REPLACE SET must separate assignments with commas.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"REPLACE SET assignment for '{col}' has an invalid expression.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"REPLACE SET assignment for '{col}' has an invalid expression.", ex);
            }
            list.Add(new SqlAssignment(col, NormalizeInsertValueRaw(exprRaw), expr));

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING) || IsWord(Peek(), SqlConst.ON))
                    throw new InvalidOperationException(
                        $"REPLACE SET has a trailing comma without assignment (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            if (IsEnd(Peek()) || IsSymbol(Peek(), ";") || IsWord(Peek(), SqlConst.WHERE) || IsWord(Peek(), SqlConst.FROM) || IsWord(Peek(), SqlConst.USING) || IsWord(Peek(), SqlConst.RETURNING) || IsWord(Peek(), SqlConst.ON))
                return list;

            throw new InvalidOperationException("REPLACE SET must separate assignments with commas.");
        }
    }

    private static List<string> SplitRawByComma(string rawBlock)
    {
        // Separa "1, 'abc', func(x)" em itens, respeitando:
        // - profundidade de parênteses
        // - strings quoted (single/double)
        // Isso evita quebrar JSON/texto que contém vírgula dentro da string.
        var res = new List<string>();
        if (rawBlock.Length == 0)
            return res;

        int depth = 0;
        int start = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        for (int i = 0; i < rawBlock.Length; i++)
        {
            var ch = rawBlock[i];

            if (inSingleQuote)
            {
                // MySQL-style escape
                if (ch == '\\' && i + 1 < rawBlock.Length)
                {
                    i++;
                    continue;
                }

                // ANSI doubled quote: '' inside a single-quoted literal
                if (ch == '\'' && i + 1 < rawBlock.Length && rawBlock[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inSingleQuote = false;

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '\\' && i + 1 < rawBlock.Length)
                {
                    i++;
                    continue;
                }

                if (ch == '"' && i + 1 < rawBlock.Length && rawBlock[i + 1] == '"')
                {
                    i++;
                    continue;
                }

                if (ch == '"')
                    inDoubleQuote = false;

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth == 0 && ch == ',')
            {
                res.Add(rawBlock[start..i].Trim());
                start = i + 1;
            }
        }
        if (start <= rawBlock.Length)
            res.Add(rawBlock[start..].Trim());
        return res;
    }

    private string NormalizeInsertValueRaw(string raw)
    {
        raw = raw.Trim();
        if (string.IsNullOrWhiteSpace(raw))
            return raw;

        var tokens = new SqlTokenizer(raw, _dialect).Tokenize();
        if (tokens.Count == 2
            && tokens[0].Kind == SqlTokenKind.String
            && tokens[1].Kind == SqlTokenKind.EndOfFile)
        {
            return tokens[0].Text;
        }

        return raw;
    }

    private string ReadBalancedParenRawTokens()
    {
        ExpectSymbol("(");
        int depth = 1;
        var buf = new List<SqlToken>();
        while (!IsEnd(Peek()))
        {
            var t = Consume();
            if (IsSymbol(t, "(")) depth++;
            else if (IsSymbol(t, ")"))
            {
                depth--;
                if (depth == 0) break;
            }
            buf.Add(t);
        }

        if (depth != 0)
            throw new InvalidOperationException("INSERT VALUES row tuple was not closed correctly.");

        return TokensToSql(buf);
    }

    private SqlQueryBase ParseCreate()
    {
        ExpectWord(SqlConst.CREATE);

        // CREATE OR REPLACE ...
        var orReplace = false;
        if (IsWord(Peek(), SqlConst.OR))
        {
            Consume();
            ExpectWord(SqlConst.REPLACE);
            orReplace = true;
        }

        // CREATE VIEW ...
        if (IsWord(Peek(), SqlConst.VIEW))
            return ParseCreateView(orReplace);

        var uniqueIndex = false;
        if (IsWord(Peek(), SqlConst.UNIQUE))
        {
            Consume();
            uniqueIndex = true;
        }

        if (IsWord(Peek(), SqlConst.INDEX))
            return ParseCreateIndex(orReplace, uniqueIndex);

        if (IsWord(Peek(), SqlConst.SEQUENCE))
            return ParseCreateSequence(orReplace);

        if (IsWord(Peek(), SqlConst.FUNCTION))
            return ParseCreateFunction(orReplace);

        if (IsWord(Peek(), SqlConst.PROCEDURE))
            return ParseCreateProcedure(orReplace);

        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW, FUNCTION and PROCEDURE statements.");

        var isTemporary = false;
        var tempScope = TemporaryTableScope.None;
        if (IsWord(Peek(), SqlConst.GLOBAL))
        {
            Consume();
            if (IsWord(Peek(), SqlConst.TEMPORARY) || IsWord(Peek(), SqlConst.TEMP))
            {
                Consume();
                isTemporary = true;
                tempScope = TemporaryTableScope.Global;
            }
            else
            {
                throw new InvalidOperationException("GLOBAL deve ser seguido de TEMPORARY/TEMP para tabelas temporárias.");
            }
        }

        if (!isTemporary && (IsWord(Peek(), SqlConst.TEMPORARY) || IsWord(Peek(), SqlConst.TEMP)))
        {
            Consume();
            isTemporary = true;
            tempScope = TemporaryTableScope.Connection;
        }

        ExpectWord(SqlConst.TABLE);

        var ifNotExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            // IF NOT EXISTS
            Consume(); // IF
            ExpectWord(SqlConst.NOT);
            ExpectWord(SqlConst.EXISTS);
            ifNotExists = true;
        }

        // table name
        var nameTok = Peek();
        if (nameTok.Kind != SqlTokenKind.Identifier)
            throw new InvalidOperationException($"Esperava nome da tabela, veio {nameTok.Kind} '{nameTok.Text}'");

        var table = ParseTableSource(consumeHints: false, allowFunctionSource: false);

        // Optional column list: (id INT, name VARCHAR(50))
        var colNames = new List<string>();
        if (IsSymbol(Peek(), "(") || Peek().Text == "(")
        {
            var rawColumnsBlock = ReadBalancedParenRawTokens();
            var defs = SplitRawByComma(rawColumnsBlock);

            if (defs.Count == 0 || string.IsNullOrWhiteSpace(rawColumnsBlock))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

            if (string.IsNullOrWhiteSpace(defs[0]))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list cannot start with a comma.");

            if (string.IsNullOrWhiteSpace(defs[^1]))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list cannot end with a comma.");

            if (defs.Any(string.IsNullOrWhiteSpace))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list has an empty entry between commas.");

            foreach (var def in defs)
            {
                var defTokens = new SqlTokenizer(def, _dialect).Tokenize()
                    .Where(t => t.Kind != SqlTokenKind.EndOfFile)
                    .ToList();

                if (defTokens.Count == 0)
                    throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

                var firstColToken = defTokens[0];
                if (firstColToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                    throw new InvalidOperationException($"CREATE TEMPORARY TABLE column list expects a column name, found {firstColToken.Kind} '{firstColToken.Text}'.");

                colNames.Add(firstColToken.Text);

                var depth = 0;
                for (var i = 1; i < defTokens.Count - 1; i++)
                {
                    var token = defTokens[i];
                    if (token.Text == "(") { depth++; continue; }
                    if (token.Text == ")") { if (depth > 0) depth--; continue; }
                    if (depth != 0) continue;

                    var next = defTokens[i + 1];
                    if (token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                        && next.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                        && IsLikelyColumnTypeToken(next))
                    {
                        throw new InvalidOperationException("CREATE TEMPORARY TABLE column list must separate columns with commas.");
                    }
                }
            }
        }

        // CREATE TEMPORARY TABLE ... AS SELECT ...
        // find AS at top-level (paren depth 0)
        if (!IsWord(Peek(), SqlConst.AS))
            ExpectWord(SqlConst.AS);
        else
            Consume();

        // remaining tokens compose SELECT/WITH statement
        var rest = new List<SqlToken>();
        while (!IsEnd(Peek()))
            rest.Add(Consume());

        EnsureBodyExistsAfterAs(rest, "CREATE TEMPORARY TABLE ... AS");
        EnsureNoUnexpectedTrailingStatementAfterBody(rest, "CREATE TEMPORARY TABLE ... AS");

        var selectSql = TokensToSql(rest).Trim();
        var inner = Parse(selectSql, _dialect);
        if (inner is not SqlSelectQuery sel)
            throw new InvalidOperationException("CREATE ... AS deve conter SELECT/WITH.");

        if (isTemporary && tempScope == TemporaryTableScope.Connection)
        {
            var namedScope = _dialect.GetTemporaryTableScope(table.Name ?? string.Empty, table.DbName);
            if (namedScope != TemporaryTableScope.None)
                tempScope = namedScope;
        }

        if (!isTemporary)
        {
            tempScope = _dialect.GetTemporaryTableScope(table.Name ?? string.Empty, table.DbName);
            isTemporary = tempScope != TemporaryTableScope.None;
        }

        if (!isTemporary)
            throw SqlUnsupported.ForParser("CREATE sem TEMPORARY TABLE");

        return new SqlCreateTemporaryTableQuery
        {
            Temporary = true,
            Scope = tempScope == TemporaryTableScope.None
                ? TemporaryTableScope.Connection
                : tempScope,
            IfNotExists = ifNotExists,
            Table = table,
            ColumnNames = colNames,
            AsSelect = sel
        };
    }

    private SqlCreateViewQuery ParseCreateView(bool orReplace)
    {
        ExpectWord(SqlConst.VIEW);

        // IF NOT EXISTS is not supported for CREATE VIEW in the mocked dialects.
        var ifNotExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            throw new InvalidOperationException("CREATE VIEW IF NOT EXISTS is not supported.");
        }

        // view name
        var nameTok = Peek();
        if (nameTok.Kind != SqlTokenKind.Identifier)
            throw new InvalidOperationException($"Esperava nome da view, veio {nameTok.Kind} '{nameTok.Text}'");


        var viewName = ParseTableSource(consumeHints: false);

        // Optional column list: (col1, col2, ...)
        var colNames = new List<string>();
        if (IsSymbol(Peek(), "("))
        {
            Consume(); // (
            if (IsSymbol(Peek(), ")"))
                throw new InvalidOperationException("CREATE VIEW column list requires at least one column name.");

            var expectColName = true;
            while (true)
            {
                var t = Peek();
                if (IsEnd(t))
                    throw new InvalidOperationException("CREATE VIEW column list was not closed correctly.");

                if (IsSymbol(t, ")"))
                {
                    if (expectColName)
                        throw new InvalidOperationException("CREATE VIEW column list cannot end with a comma.");

                    Consume();
                    break;
                }

                if (expectColName)
                {
                    if (IsSymbol(t, ","))
                        throw new InvalidOperationException("CREATE VIEW column list cannot start with a comma.");

                    if (t.Kind != SqlTokenKind.Identifier)
                        throw new InvalidOperationException($"CREATE VIEW column list expects a column name, found {t.Kind} '{t.Text}'.");

                    colNames.Add(Consume().Text);
                    expectColName = false;
                    continue;
                }

                if (IsSymbol(t, ","))
                {
                    Consume();
                    expectColName = true;
                    continue;
                }

                throw new InvalidOperationException("CREATE VIEW column list must separate columns with commas.");
            }
        }

        ExpectWord(SqlConst.AS);

        var rest = new List<SqlToken>();
        while (!IsEnd(Peek()))
            rest.Add(Consume());

        EnsureBodyExistsAfterAs(rest, "CREATE VIEW ... AS");
        EnsureNoUnexpectedTrailingStatementAfterBody(rest, "CREATE VIEW ... AS");

        var selectSql = TokensToSql(rest).Trim();
        var inner = Parse(selectSql, _dialect);
        if (inner is not SqlSelectQuery sel)
            throw new InvalidOperationException("CREATE VIEW ... AS deve conter SELECT/WITH.");

        return new SqlCreateViewQuery
        {
            OrReplace = orReplace,
            IfNotExists = ifNotExists,
            Table = viewName,
            ColumnNames = colNames,
            Select = sel
        };
    }

    private SqlCreateSequenceQuery ParseCreateSequence(bool orReplace)
    {
        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW and FUNCTION statements.");

        if (!_dialect.SupportsSequenceDdl)
            throw SqlUnsupported.ForDialect(_dialect, "CREATE SEQUENCE");

        ExpectWord(SqlConst.SEQUENCE);

        var ifNotExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume();
            ExpectWord(SqlConst.NOT);
            ExpectWord(SqlConst.EXISTS);
            ifNotExists = true;
        }

        var sequenceNameToken = Peek();
        if (IsEnd(sequenceNameToken) || IsSymbol(sequenceNameToken, ";"))
            throw new InvalidOperationException("CREATE SEQUENCE requires a sequence name.");

        var sequence = ParseQualifiedObjectName();
        var startValue = 1L;
        var incrementBy = 1L;
        var parsedStart = false;
        var parsedIncrement = false;

        while (!IsEnd(Peek()) && !IsSymbol(Peek(), ";"))
        {
            if (IsWord(Peek(), SqlConst.START))
            {
                if (parsedStart)
                    throw new InvalidOperationException("CREATE SEQUENCE START can only be specified once.");

                Consume();
                if (IsWord(Peek(), SqlConst.WITH))
                    Consume();

                startValue = ExpectSignedNumberLong("CREATE SEQUENCE START");
                parsedStart = true;
                continue;
            }

            if (IsWord(Peek(), SqlConst.INCREMENT))
            {
                if (parsedIncrement)
                    throw new InvalidOperationException("CREATE SEQUENCE INCREMENT can only be specified once.");

                Consume();
                if (IsWord(Peek(), SqlConst.BY))
                    Consume();

                incrementBy = ExpectSignedNumberLong("CREATE SEQUENCE INCREMENT");
                if (incrementBy == 0)
                    throw new InvalidOperationException("CREATE SEQUENCE INCREMENT cannot be zero.");

                parsedIncrement = true;
                continue;
            }

            var unexpected = Peek();
            throw new InvalidOperationException(
                $"Unexpected token after CREATE SEQUENCE: {unexpected.Kind} '{unexpected.Text}'");
        }

        EnsureStatementEnd("CREATE SEQUENCE");

        return new SqlCreateSequenceQuery
        {
            IfNotExists = ifNotExists,
            StartValue = startValue,
            IncrementBy = incrementBy,
            Table = sequence
        };
    }

    private SqlCreateIndexQuery ParseCreateIndex(bool orReplace, bool unique)
    {
        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW and FUNCTION statements.");

        Consume(); // INDEX

        var indexNameToken = Peek();
        if (IsEnd(indexNameToken) || IsSymbol(indexNameToken, ";"))
            throw new InvalidOperationException("CREATE INDEX requires an index name.");

        var indexName = ExpectIdentifier();
        ExpectWord(SqlConst.ON);
        var table = ParseCreateIndexTableName();

        if (!IsSymbol(Peek(), "("))
            throw new InvalidOperationException("CREATE INDEX requires a column list.");

        Consume(); // (
        if (IsSymbol(Peek(), ")"))
            throw new InvalidOperationException("CREATE INDEX column list requires at least one column name.");

        var keyColumns = ParseIdentifierList("CREATE INDEX column list");
        var normalizedKeyColumns = keyColumns
            .Select(static col => col.NormalizeName())
            .ToList();
        if (normalizedKeyColumns.Count != normalizedKeyColumns.Distinct(StringComparer.OrdinalIgnoreCase).Count())
            throw new InvalidOperationException("CREATE INDEX column list cannot contain duplicate columns.");

        ExpectSymbol(")");
        EnsureStatementEnd("CREATE INDEX");

        return new SqlCreateIndexQuery
        {
            IndexName = indexName,
            Unique = unique,
            KeyColumns = normalizedKeyColumns,
            Table = table
        };
    }

    private SqlCreateFunctionQuery ParseCreateFunction(bool orReplace)
    {
        if (orReplace && !_dialect.SupportsCreateOrReplaceFunctionDdl)
            throw new InvalidOperationException("CREATE OR REPLACE FUNCTION is not supported for this dialect in the mock.");

        if (!_dialect.SupportsFunctionDdl)
            throw SqlUnsupported.ForDialect(_dialect, "CREATE FUNCTION");

        ExpectWord(SqlConst.FUNCTION);

        var functionNameToken = Peek();
        if (IsEnd(functionNameToken) || IsSymbol(functionNameToken, ";"))
            throw new InvalidOperationException("CREATE FUNCTION requires a function name.");

        var function = ParseQualifiedObjectName();
        var parameters = ParseFunctionParameters(allowMissingParameterList: _dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase));

        if (_dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
            return ParseOracleCreateFunction(function, parameters, orReplace);

        if (_dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
            return ParsePostgreSqlCreateFunction(function, parameters, orReplace);

        if (_dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase)
            || _dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            return ParseInlineReturnCreateFunction(function, parameters, orReplace);
        }

        return ParseSqlServerStyleCreateFunction(function, parameters, orReplace);
    }

    private IReadOnlyList<ScalarFunctionParameterDef> ParseFunctionParameters(bool allowMissingParameterList)
    {
        if (!IsSymbol(Peek(), "("))
        {
            if (allowMissingParameterList)
                return [];

            throw new InvalidOperationException("CREATE FUNCTION requires a parameter list.");
        }

        var rawParameterList = ReadBalancedParenRawTokens().Trim();
        if (string.IsNullOrWhiteSpace(rawParameterList))
            return [];

        var defs = SplitRawByComma(rawParameterList);
        if (defs.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("CREATE FUNCTION parameter list cannot contain empty entries.");

        var parameters = defs
            .Select(ParseFunctionParameter)
            .ToList();

        var duplicateNames = parameters
            .GroupBy(static parameter => parameter.NormalizedName, StringComparer.OrdinalIgnoreCase)
            .Where(static group => group.Count() > 1)
            .Select(static group => group.Key)
            .ToList();
        if (duplicateNames.Count > 0)
            throw new InvalidOperationException($"CREATE FUNCTION parameter list cannot contain duplicate names: {string.Join(", ", duplicateNames)}.");

        return parameters;
    }

    private ScalarFunctionParameterDef ParseFunctionParameter(string rawDefinition)
    {
        var tokens = new SqlTokenizer(rawDefinition, _dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();
        if (tokens.Count == 0)
            throw new InvalidOperationException("CREATE FUNCTION parameter list requires at least one parameter definition.");

        var nameToken = tokens[0];
        if (nameToken.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE FUNCTION parameter definition requires a parameter name, found {nameToken.Kind} '{nameToken.Text}'.");

        var index = 1;
        if (index < tokens.Count && IsFunctionParameterWord(tokens[index], SqlConst.IN))
        {
            index++;
            if (index < tokens.Count && IsFunctionParameterWord(tokens[index], SqlConst.OUT))
                throw new NotSupportedException("CREATE FUNCTION currently supports only input parameters in the mock.");
        }
        else if (index < tokens.Count
            && (IsFunctionParameterWord(tokens[index], SqlConst.OUT)
                || IsFunctionParameterWord(tokens[index], SqlConst.INOUT)
                || IsFunctionParameterWord(tokens[index], SqlConst.INOUT)))
        {
            throw new NotSupportedException("CREATE FUNCTION currently supports only input parameters in the mock.");
        }

        if (tokens.Skip(index).Any(static token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "="))
            throw new NotSupportedException("CREATE FUNCTION parameter default values are not supported in the mock yet.");

        var typeSql = TokensToSql([.. tokens.Skip(index)]).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"CREATE FUNCTION parameter '{nameToken.Text}' requires a type.");

        return new ScalarFunctionParameterDef(nameToken.Text, typeSql);
    }

    private static bool IsFunctionParameterWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

    private SqlCreateFunctionQuery ParseSqlServerStyleCreateFunction(
        SqlTableSource function,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        bool orReplace)
    {
        ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ParseFunctionReturnTypeSql(SqlConst.AS);
        ExpectWord(SqlConst.AS);
        var body = ParseFunctionReturnBody(allowBeginEndBlock: true, requireBeginEndBlock: false);
        return BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace);
    }

    private SqlCreateFunctionQuery ParseInlineReturnCreateFunction(
        SqlTableSource function,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        bool orReplace)
    {
        ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ParseFunctionReturnTypeSql(SqlConst.RETURN, SqlConst.BEGIN);
        var body = ParseFunctionReturnBody(allowBeginEndBlock: true, requireBeginEndBlock: false);
        return BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace);
    }

    private SqlCreateFunctionQuery ParseOracleCreateFunction(
        SqlTableSource function,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        bool orReplace)
    {
        ExpectWord(SqlConst.RETURN);
        var returnTypeSql = ParseFunctionReturnTypeSql(SqlConst.IS, SqlConst.AS);

        if (!IsWord(Peek(), SqlConst.IS) && !IsWord(Peek(), SqlConst.AS))
            throw new InvalidOperationException("CREATE FUNCTION in Oracle syntax requires IS or AS before the body.");

        Consume();
        var body = ParseFunctionReturnBody(allowBeginEndBlock: true, requireBeginEndBlock: true);
        return BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace);
    }

    private SqlCreateFunctionQuery ParsePostgreSqlCreateFunction(
        SqlTableSource function,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        bool orReplace)
    {
        ExpectWord(SqlConst.RETURNS);
        var returnTypeSql = ParseFunctionReturnTypeSql(SqlConst.AS, SqlConst.LANGUAGE);

        string? bodySql = null;
        string? language = null;

        if (IsWord(Peek(), SqlConst.AS))
        {
            Consume();
            bodySql = ParseQuotedFunctionBodySql();
        }

        if (IsWord(Peek(), SqlConst.LANGUAGE))
        {
            Consume();
            language = ExpectIdentifier();
        }

        if (bodySql is null && IsWord(Peek(), SqlConst.AS))
        {
            Consume();
            bodySql = ParseQuotedFunctionBodySql();
        }

        if (bodySql is null)
            throw new InvalidOperationException("CREATE FUNCTION in PostgreSQL syntax requires AS '<body>'.");

        if (!string.Equals(language, "SQL", StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only PostgreSQL LANGUAGE SQL bodies in the mock.");

        var body = ParsePostgreSqlSqlFunctionBody(bodySql);
        return BuildCreateFunctionQuery(function, parameters, returnTypeSql, body, orReplace);
    }

    private string ParseFunctionReturnTypeSql(params string[] stopWords)
    {
        var returnTypeTokens = new List<SqlToken>();
        while (!IsEnd(Peek()) && !stopWords.Any(stopWord => IsWord(Peek(), stopWord)))
            returnTypeTokens.Add(Consume());

        var returnTypeSql = TokensToSql(returnTypeTokens).Trim();
        if (string.IsNullOrWhiteSpace(returnTypeSql))
            throw new InvalidOperationException("CREATE FUNCTION requires a scalar return type.");

        if (returnTypeSql.Equals(SqlConst.TABLE, StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only scalar return types in the mock.");

        return returnTypeSql;
    }

    private SqlExpr ParseFunctionReturnBody(bool allowBeginEndBlock, bool requireBeginEndBlock)
    {
        var hasBeginEndBlock = false;
        if (allowBeginEndBlock && IsWord(Peek(), SqlConst.BEGIN))
        {
            Consume();
            hasBeginEndBlock = true;
        }
        else if (requireBeginEndBlock)
        {
            throw new InvalidOperationException("CREATE FUNCTION body requires BEGIN ... END in this syntax subset.");
        }

        ExpectWord(SqlConst.RETURN);

        var bodyTokens = new List<SqlToken>();
        while (!IsEnd(Peek()))
        {
            if (hasBeginEndBlock && IsWord(Peek(), SqlConst.END))
                break;

            if (IsSymbol(Peek(), ";"))
            {
                if (hasBeginEndBlock)
                {
                    Consume();
                    break;
                }

                break;
            }

            bodyTokens.Add(Consume());
        }

        var bodySql = TokensToSql(bodyTokens).Trim();
        if (string.IsNullOrWhiteSpace(bodySql))
            throw new InvalidOperationException("CREATE FUNCTION requires a scalar expression after RETURN.");

        if (hasBeginEndBlock)
            ExpectWord(SqlConst.END);

        return SqlExpressionParser.ParseScalar(bodySql, _dialect);
    }

    private string ParseQuotedFunctionBodySql()
    {
        var token = Consume();
        if (token.Kind != SqlTokenKind.String)
            throw new InvalidOperationException("CREATE FUNCTION requires a quoted SQL body after AS in this syntax subset.");

        return token.Text;
    }

    private SqlExpr ParsePostgreSqlSqlFunctionBody(string bodySql)
    {
        var trimmed = bodySql.Trim();
        if (trimmed.EndsWith(";", StringComparison.Ordinal))
            trimmed = trimmed[..^1].TrimEnd();

        if (!trimmed.StartsWith(SqlConst.SELECT, StringComparison.OrdinalIgnoreCase))
            throw new NotSupportedException("CREATE FUNCTION currently supports only PostgreSQL LANGUAGE SQL bodies with a single SELECT <expr> statement in the mock.");

        trimmed = trimmed[6..].TrimStart();
        if (string.IsNullOrWhiteSpace(trimmed))
            throw new InvalidOperationException("CREATE FUNCTION PostgreSQL body requires a scalar expression after SELECT.");

        return SqlExpressionParser.ParseScalar(trimmed, _dialect);
    }

    private SqlCreateFunctionQuery BuildCreateFunctionQuery(
        SqlTableSource function,
        IReadOnlyList<ScalarFunctionParameterDef> parameters,
        string returnTypeSql,
        SqlExpr body,
        bool orReplace)
    {
        EnsureStatementEnd("CREATE FUNCTION");
        return new SqlCreateFunctionQuery
        {
            Table = function,
            OrReplace = orReplace,
            Parameters = parameters,
            ReturnTypeSql = returnTypeSql,
            Body = body
        };
    }

    private SqlCreateProcedureQuery ParseCreateProcedure(bool orReplace)
    {
        if (!_dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.ForDialect(_dialect, "CREATE PROCEDURE");

        ExpectWord(SqlConst.PROCEDURE);

        var procedureNameToken = Peek();
        if (IsEnd(procedureNameToken) || IsSymbol(procedureNameToken, ";"))
            throw new InvalidOperationException("CREATE PROCEDURE requires a procedure name.");

        var procedure = ParseQualifiedObjectName();
        var definition = ParseProcedureDefinition();

        while (!IsEnd(Peek()) && !IsSymbol(Peek(), ";"))
            Consume();

        EnsureStatementEnd("CREATE PROCEDURE");

        return new SqlCreateProcedureQuery
        {
            Table = procedure,
            OrReplace = orReplace,
            Definition = definition
        };
    }

    private ProcedureDef ParseProcedureDefinition()
    {
        if (!IsSymbol(Peek(), "("))
            throw new InvalidOperationException("CREATE PROCEDURE requires a parameter list.");

        var rawParameterList = ReadBalancedParenRawTokens().Trim();
        if (string.IsNullOrWhiteSpace(rawParameterList))
            return new ProcedureDef([], [], []);

        var defs = SplitRawByComma(rawParameterList);
        if (defs.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("CREATE PROCEDURE parameter list cannot contain empty entries.");

        var requiredIn = new List<ProcParam>();
        var optionalIn = new List<ProcParam>();
        var outParams = new List<ProcParam>();

        foreach (var rawDefinition in defs)
        {
            var (parameter, direction) = ParseProcedureParameter(rawDefinition);
            switch (direction)
            {
                case ParameterDirection.Input:
                    requiredIn.Add(parameter);
                    break;
                case ParameterDirection.Output:
                    outParams.Add(parameter);
                    break;
                case ParameterDirection.InputOutput:
                    requiredIn.Add(parameter);
                    outParams.Add(parameter);
                    break;
                default:
                    optionalIn.Add(parameter);
                    break;
            }
        }

        return new ProcedureDef(requiredIn, optionalIn, outParams);
    }

    private (ProcParam Parameter, ParameterDirection Direction) ParseProcedureParameter(string rawDefinition)
    {
        var tokens = new SqlTokenizer(rawDefinition, _dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();
        if (tokens.Count == 0)
            throw new InvalidOperationException("CREATE PROCEDURE parameter list requires at least one parameter definition.");

        var index = 0;
        var direction = ParameterDirection.Input;

        if (IsProcedureDirectionWord(tokens[index], SqlConst.IN))
        {
            index++;
            if (index < tokens.Count && IsProcedureDirectionWord(tokens[index], SqlConst.OUT))
            {
                direction = ParameterDirection.InputOutput;
                index++;
            }
        }
        else if (IsProcedureDirectionWord(tokens[index], SqlConst.OUT))
        {
            direction = ParameterDirection.Output;
            index++;
        }
        else if (IsProcedureDirectionWord(tokens[index], SqlConst.INOUT))
        {
            direction = ParameterDirection.InputOutput;
            index++;
        }

        if (index >= tokens.Count)
            throw new InvalidOperationException("CREATE PROCEDURE parameter definition requires a parameter name.");

        var nameToken = tokens[index++];
        if (nameToken.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE PROCEDURE parameter definition requires a parameter name, found {nameToken.Kind} '{nameToken.Text}'.");

        var typeTokens = tokens.Skip(index).ToList();
        if (typeTokens.Count == 0)
            throw new InvalidOperationException($"CREATE PROCEDURE parameter '{nameToken.Text}' requires a type.");

        if (typeTokens.Any(token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "="))
            throw new NotSupportedException("CREATE PROCEDURE parameter default values are not supported in the mock yet.");

        var typeSql = TokensToSql(typeTokens).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"CREATE PROCEDURE parameter '{nameToken.Text}' requires a type.");

        var dbType = ParseProcedureParameterDbType(typeSql);
        var parameter = new ProcParam(nameToken.Text, dbType, Required: direction != ParameterDirection.Output);
        return (parameter, direction);
    }

    private static bool IsProcedureDirectionWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

    private static DbType ParseProcedureParameterDbType(string typeSql)
        => typeSql.Trim().NormalizeName().Split(' ').First(_=>!string.IsNullOrWhiteSpace(_)).ToUpperInvariant() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };

    private SqlTableSource ParseCreateIndexTableName()
    {
        var tableNameToken = Peek();
        if (IsEnd(tableNameToken) || IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("CREATE INDEX requires a table name.");

        if (IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("CREATE INDEX requires a concrete table name.");

        var table = ParseQualifiedObjectName();

        if (IsWord(Peek(), SqlConst.AS) || Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            throw new InvalidOperationException("CREATE INDEX requires a table name without alias.");

        return table;
    }

    private SqlQueryBase ParseAlter()
    {
        ExpectWord(SqlConst.ALTER);

        if (IsWord(Peek(), SqlConst.TABLE))
            return ParseAlterTable();

        throw new InvalidOperationException("Apenas ALTER TABLE pragmático é suportado no mock no momento.");
    }

    private SqlAlterTableAddColumnQuery ParseAlterTable()
    {
        if (!_dialect.SupportsAlterTableAddColumn)
            throw SqlUnsupported.ForDialect(_dialect, "ALTER TABLE ... ADD [COLUMN]");

        ExpectWord(SqlConst.TABLE);
        var table = ParseAlterTableName();

        if (!IsWord(Peek(), SqlConst.ADD))
            throw new InvalidOperationException("Only ALTER TABLE ... ADD [COLUMN] is supported in the mock.");

        Consume(); // ADD
        if (IsWord(Peek(), SqlConst.COLUMN))
            Consume();

        var columnNameToken = Peek();
        if (IsEnd(columnNameToken) || IsSymbol(columnNameToken, ";"))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN requires a column name.");

        var columnName = ExpectIdentifier();
        var (columnType, size, decimalPlaces) = ParseAlterTableColumnTypeDefinition();
        var nullable = true;
        var sawNullability = false;
        string? defaultValueRaw = null;

        while (!IsEnd(Peek()) && !IsSymbol(Peek(), ";"))
        {
            if (IsWord(Peek(), SqlConst.DEFAULT))
            {
                if (defaultValueRaw is not null)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN DEFAULT can only be specified once.");

                Consume();
                defaultValueRaw = ParseAlterTableDefaultLiteralRaw();
                continue;
            }

            if (IsWord(Peek(), SqlConst.NOT))
            {
                if (sawNullability)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN nullability can only be specified once.");

                Consume();
                ExpectWord(SqlConst.NULL);
                nullable = false;
                sawNullability = true;
                continue;
            }

            if (IsWord(Peek(), SqlConst.NULL))
            {
                if (sawNullability)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN nullability can only be specified once.");

                Consume();
                nullable = true;
                sawNullability = true;
                continue;
            }

            var unexpected = Peek();
            throw new InvalidOperationException(
                $"Unsupported token in ALTER TABLE ADD COLUMN subset: {unexpected.Kind} '{unexpected.Text}'");
        }

        if (!nullable
            && defaultValueRaw is not null
            && string.Equals(defaultValueRaw.Trim(), SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN NOT NULL cannot use DEFAULT NULL.");

        EnsureStatementEnd("ALTER TABLE");

        return new SqlAlterTableAddColumnQuery
        {
            Table = table,
            ColumnName = columnName,
            ColumnType = columnType,
            Size = size,
            DecimalPlaces = decimalPlaces,
            Nullable = nullable,
            DefaultValueRaw = defaultValueRaw
        };
    }

    private SqlTableSource ParseAlterTableName()
    {
        var tableNameToken = Peek();
        if (IsEnd(tableNameToken) || IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("ALTER TABLE requires a table name.");

        if (IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("ALTER TABLE requires a concrete table name.");

        var table = ParseQualifiedObjectName();
        var next = Peek();

        if (IsWord(next, SqlConst.AS)
            || (next.Kind == SqlTokenKind.Identifier && !IsWord(next, SqlConst.ADD)))
            throw new InvalidOperationException("ALTER TABLE requires a table name without alias.");

        return table;
    }

    private SqlQueryBase ParseDrop()
    {
        ExpectWord(SqlConst.DROP);

        if (IsWord(Peek(), SqlConst.VIEW))
            return ParseDropView();

        if (IsWord(Peek(), SqlConst.SEQUENCE))
            return ParseDropSequence();

        if (IsWord(Peek(), SqlConst.TABLE)
            || IsWord(Peek(), SqlConst.TEMP)
            || IsWord(Peek(), SqlConst.TEMPORARY)
            || IsWord(Peek(), SqlConst.GLOBAL))
            return ParseDropTable();

        if (IsWord(Peek(), SqlConst.INDEX))
            return ParseDropIndex();

        if (IsWord(Peek(), SqlConst.FUNCTION))
            return ParseDropFunction();

        throw new InvalidOperationException("Apenas DROP VIEW, DROP TABLE, DROP INDEX e DROP SEQUENCE são suportados no mock no momento.");
    }

    private SqlDropViewQuery ParseDropView()
    {
        Consume(); // VIEW

        var ifExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume(); // IF
            ExpectWord(SqlConst.EXISTS);
            ifExists = true;
        }

        var viewNameToken = Peek();
        if (IsEnd(viewNameToken) || IsSymbol(viewNameToken, ";"))
            throw new InvalidOperationException("DROP VIEW requires a view name.");

        var first = ExpectIdentifier();
        string? dbName = null;
        var viewOnlyName = first;
        if (IsSymbol(Peek(), "."))
        {
            Consume();
            dbName = viewOnlyName;
            viewOnlyName = ExpectIdentifier();
        }
        var viewName = new SqlTableSource(
            dbName,
            viewOnlyName,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);

        var continuation = Peek();
        if (!IsEnd(continuation) && !IsSymbol(continuation, ";"))
            throw new InvalidOperationException(
                $"Unexpected token after DROP VIEW: {continuation.Kind} '{continuation.Text}'");

        EnsureStatementEnd("DROP VIEW");

        return new SqlDropViewQuery
        {
            IfExists = ifExists,
            Table = viewName
        };
    }

    private SqlDropSequenceQuery ParseDropSequence()
    {
        if (!_dialect.SupportsSequenceDdl)
            throw SqlUnsupported.ForDialect(_dialect, "DROP SEQUENCE");

        Consume(); // SEQUENCE

        var ifExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume();
            ExpectWord(SqlConst.EXISTS);
            ifExists = true;
        }

        var sequenceNameToken = Peek();
        if (IsEnd(sequenceNameToken) || IsSymbol(sequenceNameToken, ";"))
            throw new InvalidOperationException("DROP SEQUENCE requires a sequence name.");

        var sequenceName = ParseQualifiedObjectName();

        EnsureStatementEnd("DROP SEQUENCE");

        return new SqlDropSequenceQuery
        {
            IfExists = ifExists,
            Table = sequenceName
        };
    }

    private SqlDropFunctionQuery ParseDropFunction()
    {
        if (!_dialect.SupportsFunctionDdl)
            throw SqlUnsupported.ForDialect(_dialect, "DROP FUNCTION");

        Consume(); // FUNCTION

        var ifExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume();
            ExpectWord(SqlConst.EXISTS);
            ifExists = true;
        }

        var functionNameToken = Peek();
        if (IsEnd(functionNameToken) || IsSymbol(functionNameToken, ";"))
            throw new InvalidOperationException("DROP FUNCTION requires a function name.");

        var function = ParseQualifiedObjectName();

        if (IsSymbol(Peek(), "("))
            _ = ReadBalancedParenRawTokens();

        EnsureStatementEnd("DROP FUNCTION");

        return new SqlDropFunctionQuery
        {
            IfExists = ifExists,
            Table = function
        };
    }

    private SqlDropTableQuery ParseDropTable()
    {
        var isTemporary = false;
        var tempScope = TemporaryTableScope.None;

        if (IsWord(Peek(), SqlConst.GLOBAL))
        {
            Consume();
            if (IsWord(Peek(), SqlConst.TEMPORARY) || IsWord(Peek(), SqlConst.TEMP))
            {
                Consume();
                isTemporary = true;
                tempScope = TemporaryTableScope.Global;
            }
            else
            {
                throw new InvalidOperationException("GLOBAL deve ser seguido de TEMPORARY/TEMP em DROP TABLE.");
            }
        }

        if (!isTemporary && (IsWord(Peek(), SqlConst.TEMPORARY) || IsWord(Peek(), SqlConst.TEMP)))
        {
            Consume();
            isTemporary = true;
            tempScope = TemporaryTableScope.Connection;
        }

        ExpectWord(SqlConst.TABLE);

        var ifExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume();
            ExpectWord(SqlConst.EXISTS);
            ifExists = true;
        }

        var tableNameToken = Peek();
        if (IsEnd(tableNameToken) || IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("DROP TABLE requires a table name.");

        var tableName = ParseTableSource(consumeHints: false);

        EnsureStatementEnd("DROP TABLE");

        return new SqlDropTableQuery
        {
            IfExists = ifExists,
            Temporary = isTemporary,
            Scope = tempScope,
            Table = tableName
        };
    }

    private SqlDropIndexQuery ParseDropIndex()
    {
        Consume(); // INDEX

        var ifExists = false;
        if (IsWord(Peek(), SqlConst.IF))
        {
            Consume();
            ExpectWord(SqlConst.EXISTS);
            ifExists = true;
        }

        var indexNameToken = Peek();
        if (IsEnd(indexNameToken) || IsSymbol(indexNameToken, ";"))
            throw new InvalidOperationException("DROP INDEX requires an index name.");

        var indexName = ExpectIdentifier();
        SqlTableSource? table = null;

        if (IsWord(Peek(), SqlConst.ON))
        {
            Consume();
            table = ParseDropIndexOnTableName();

            if (!string.Equals(_dialect.Name, "mysql", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(_dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
                throw SqlUnsupported.ForDialect(_dialect, "DROP INDEX ... ON <table>");
        }

        EnsureStatementEnd("DROP INDEX");

        return new SqlDropIndexQuery
        {
            IndexName = indexName,
            IfExists = ifExists,
            Table = table
        };
    }

    private SqlTableSource ParseDropIndexOnTableName()
    {
        var tableNameToken = Peek();
        if (IsEnd(tableNameToken) || IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("DROP INDEX ... ON requires a table name.");

        if (IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("DROP INDEX ... ON requires a concrete table name.");

        var table = ParseQualifiedObjectName();

        if (IsWord(Peek(), SqlConst.AS) || Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            throw new InvalidOperationException("DROP INDEX ... ON requires a table name without alias.");

        if (IsSymbol(Peek(), "("))
            throw new InvalidOperationException("DROP INDEX ... ON requires a concrete table name.");

        return table;
    }

    private (DbType Type, int? Size, int? DecimalPlaces) ParseAlterTableColumnTypeDefinition()
    {
        var typeToken = Peek();
        if (typeToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN requires a SQL type name.");

        var typeName = Consume().Text;
        string? rawArgs = null;

        if (IsSymbol(Peek(), "("))
        {
            Consume();
            var args = new List<SqlToken>();
            while (!IsEnd(Peek()) && !IsSymbol(Peek(), ")"))
                args.Add(Consume());

            if (!IsSymbol(Peek(), ")"))
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments were not closed correctly.");

            Consume();
            rawArgs = TokensToSql(args);
        }

        var dbType = typeName.Trim().NormalizeName() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };

        var (size, decimalPlaces) = ParseAlterTableTypeArgs(rawArgs, dbType);
        return (dbType, size, decimalPlaces);
    }

    private static (int? Size, int? DecimalPlaces) ParseAlterTableTypeArgs(string? rawArgs, DbType dbType)
    {
        if (rawArgs is null)
        {
            if (dbType == DbType.String)
                return (255, null);
            if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
                return (null, 2);
            return (null, null);
        }

        if (string.IsNullOrWhiteSpace(rawArgs))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

        var args = rawArgs.Split(',')
            .Select(static x => x.Trim())
            .ToArray();

        if (args.Any(static x => x.Length == 0))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

        if (dbType == DbType.String || dbType == DbType.Binary)
        {
            if (args.Length != 1 || !int.TryParse(args[0], out var parsedSize) || parsedSize <= 0)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            return (parsedSize, null);
        }

        if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
        {
            if (args.Length is < 1 or > 2)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            if (!int.TryParse(args[0], out var parsedPrecision) || parsedPrecision <= 0)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            if (args.Length == 1)
                return (parsedPrecision, 2);

            if (!int.TryParse(args[1], out var parsedScale) || parsedScale < 0 || parsedScale > parsedPrecision)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            return (parsedPrecision, parsedScale);
        }

        return (null, null);
    }

    private string ParseAlterTableDefaultLiteralRaw()
    {
        var tokens = new List<SqlToken>();

        if (IsSymbol(Peek(), "+") || IsSymbol(Peek(), "-"))
            tokens.Add(Consume());

        var token = Peek();
        if (token.Kind is SqlTokenKind.Number or SqlTokenKind.String)
        {
            tokens.Add(Consume());
            return TokensToSql(tokens);
        }

        if (IsWord(token, SqlConst.NULL) || IsWord(token, SqlConst.TRUE) || IsWord(token, SqlConst.FALSE))
        {
            tokens.Add(Consume());
            return TokensToSql(tokens);
        }

        throw new InvalidOperationException("ALTER TABLE ADD COLUMN DEFAULT only supports literal values in the shared subset.");
    }

    private SqlTableSource ParseQualifiedObjectName()
    {
        var first = ExpectIdentifier();
        string? dbName = null;
        var objectName = first;
        if (IsSymbol(Peek(), "."))
        {
            Consume();
            dbName = objectName;
            objectName = ExpectIdentifier();
        }

        return new SqlTableSource(
            dbName,
            objectName,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);
    }

    private List<string> ParseIdentifierList(string context)
    {
        var identifiers = new List<string>();
        var expectIdentifier = true;

        while (true)
        {
            var token = Peek();
            if (IsEnd(token))
                throw new InvalidOperationException($"{context} was not closed correctly.");

            if (IsSymbol(token, ")"))
            {
                if (expectIdentifier)
                    throw new InvalidOperationException($"{context} cannot end with a comma.");

                break;
            }

            if (expectIdentifier)
            {
                if (IsSymbol(token, ","))
                    throw new InvalidOperationException($"{context} cannot start with a comma.");

                if (token.Kind != SqlTokenKind.Identifier)
                    throw new InvalidOperationException($"{context} expects a column name, found {token.Kind} '{token.Text}'.");

                identifiers.Add(Consume().Text);
                expectIdentifier = false;
                continue;
            }

            if (IsSymbol(token, ","))
            {
                Consume();
                expectIdentifier = true;
                continue;
            }

            throw new InvalidOperationException($"{context} must separate columns with commas.");
        }

        if (identifiers.Count == 0)
            throw new InvalidOperationException($"{context} requires at least one column name.");

        return identifiers;
    }

    private static void EnsureNoUnexpectedTrailingStatementAfterBody(
        IReadOnlyList<SqlToken> tokens,
        string statementName)
    {
        for (var i = 0; i < tokens.Count; i++)
        {
            if (!IsSymbol(tokens[i], ";"))
                continue;

            if (i != tokens.Count - 1)
            {
                var next = tokens[i + 1];
                throw new InvalidOperationException(
                    $"Unexpected token after {statementName} body: {next.Kind} '{next.Text}'");
            }
        }
    }

    private static void EnsureBodyExistsAfterAs(
        IReadOnlyList<SqlToken> tokens,
        string statementName)
    {
        if (tokens.Count == 0 || tokens.All(static t => IsSymbol(t, ";")))
            throw new InvalidOperationException($"{statementName} requires a SELECT/WITH body.");
    }

    private static readonly HashSet<string> setTypes = new(){
            "INT",
            "INTEGER",
            "BIGINT" ,
            "SMALLINT" ,
            "TINYINT",
            "DECIMAL" ,
            "NUMERIC",
            "FLOAT" ,
            "DOUBLE" ,
            "REAL" ,
            "VARCHAR",
            "CHAR" ,
            "NVARCHAR" ,
            "NCHAR" ,
            "TEXT" ,
            "DATE" ,
            "DATETIME" ,
            "TIME" ,
            "TIMESTAMP" ,
            "BOOLEAN" ,
            "BIT" ,
            "UUID" ,
            "JSON" ,
            "BLOB" ,
            "CLOB"
    };

    private static bool IsLikelyColumnTypeToken(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier
            or SqlTokenKind.Keyword
        && setTypes.Contains(token.Text.ToUpperInvariant());

    // --- Helpers de SELECT trazidos do arquivo original ---

    private bool TryParseDistinct()
    {
        if (!IsWord(Peek(), SqlConst.DISTINCT)) return false;
        Consume();
        if (IsWord(Peek(), SqlConst.DISTINCT))
            throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
        return true;
    }


    private SqlTop? TryParseTop()
    {
        // SQL Server: SELECT TOP (10) ... / SELECT TOP 10 ...
        if (!IsWord(Peek(), SqlConst.TOP)) return null;

        // Se o dialeto não suporta, deixa o SQL cair como erro em validação ou corpo
        if (!_dialect.SupportsTop)
            return null;

        Consume(); // TOP

        // TOP pode vir como (N) ou N
        if (IsSymbol(Peek(), "("))
        {
            Consume();
            var n = ExpectRowLimitExpr();
            ExpectSymbol(")");
            return new SqlTop(n);
        }

        return new SqlTop(ExpectRowLimitExpr());
    }

    /// <summary>
    /// EN: Parses optional DML RETURNING clause with dialect gate and expression validation.
    /// PT: Faz o parsing opcional da cláusula RETURNING de DML com gate de dialeto e validação de expressão.
    /// </summary>
    /// <returns>EN: Returning items when clause is present; otherwise empty list. PT: Itens de retorno quando a cláusula estiver presente; caso contrário, lista vazia.</returns>
    private IReadOnlyList<SqlSelectItem> ParseOptionalReturningItems(ReturningClauseTarget target)
    {
        if (!IsWord(Peek(), SqlConst.RETURNING))
            return [];

        var isSupported = target switch
        {
            ReturningClauseTarget.Insert => _dialect.SupportsInsertReturning,
            ReturningClauseTarget.Update => _dialect.SupportsUpdateReturning,
            ReturningClauseTarget.Delete => _dialect.SupportsDeleteReturning,
            _ => _dialect.SupportsReturning
        };

        if (!isSupported)
            throw SqlUnsupported.ForDialect(_dialect, SqlConst.RETURNING);

        Consume(); // RETURNING

        var raws = ParseReturningItemsRaw();
        return raws.ConvertAll(raw =>
        {
            var (expr, alias) = SplitTrailingAsAliasTopLevel(raw, _dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException(
                    $"RETURNING requires at least one expression (found '{DescribeFoundTokenFromRaw(raw)}').");

            try
            {
                var parsedExpr = SqlExpressionParser.ParseScalar(expr, _dialect);
                ValidateReturningExpression(parsedExpr);
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains("aggregate functions in this dialect", StringComparison.OrdinalIgnoreCase))
                    throw;

                throw new InvalidOperationException("RETURNING expression is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("RETURNING expression is invalid.", ex);
            }
            return new SqlSelectItem(expr, alias);
        });
    }

    private void ValidateReturningExpression(SqlExpr expr)
    {
        if (_dialect.SupportsAggregateFunctionsInReturningClause)
            return;

        if (!ContainsAggregateFunction(expr))
            return;

        throw new InvalidOperationException("RETURNING clause does not allow aggregate functions in this dialect.");
    }

    private static bool ContainsAggregateFunction(SqlExpr expr)
    {
        switch (expr)
        {
            case CallExpr call:
                if (IsAggregateFunctionName(call.Name))
                    return true;
                return call.Args.Any(ContainsAggregateFunction);
            case FunctionCallExpr fn:
                if (IsAggregateFunctionName(fn.Name))
                    return true;
                return fn.Args.Any(ContainsAggregateFunction);
            case UnaryExpr unary:
                return ContainsAggregateFunction(unary.Expr);
            case BinaryExpr binary:
                return ContainsAggregateFunction(binary.Left) || ContainsAggregateFunction(binary.Right);
            case InExpr inExpr:
                return ContainsAggregateFunction(inExpr.Left) || inExpr.Items.Any(ContainsAggregateFunction);
            case LikeExpr likeExpr:
                return ContainsAggregateFunction(likeExpr.Left)
                    || ContainsAggregateFunction(likeExpr.Pattern)
                    || (likeExpr.Escape is not null && ContainsAggregateFunction(likeExpr.Escape));
            case IsNullExpr isNullExpr:
                return ContainsAggregateFunction(isNullExpr.Expr);
            case RowExpr rowExpr:
                return rowExpr.Items.Any(ContainsAggregateFunction);
            case CaseExpr caseExpr:
                return (caseExpr.BaseExpr is not null && ContainsAggregateFunction(caseExpr.BaseExpr))
                    || caseExpr.Whens.Any(when => ContainsAggregateFunction(when.When) || ContainsAggregateFunction(when.Then))
                    || (caseExpr.ElseExpr is not null && ContainsAggregateFunction(caseExpr.ElseExpr));
            case WindowFunctionExpr windowExpr:
                return windowExpr.Args.Any(ContainsAggregateFunction)
                    || windowExpr.Spec.PartitionBy.Any(ContainsAggregateFunction)
                    || windowExpr.Spec.OrderBy.Any(item => ContainsAggregateFunction(item.Expr));
            case JsonAccessExpr jsonAccessExpr:
                return ContainsAggregateFunction(jsonAccessExpr.Target)
                    || ContainsAggregateFunction(jsonAccessExpr.Path);
            case QuantifiedComparisonExpr quantifiedComparisonExpr:
                return ContainsAggregateFunction(quantifiedComparisonExpr.Left);
            case ExistsExpr:
                return false;
            default:
                return false;
        }
    }

    private static readonly HashSet<string> aggregateFunctionNames = new()
    {
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "GROUP_CONCAT",
        "STRING_AGG",
        "LISTAGG",
        "JSON_ARRAYAGG",
        "JSON_OBJECTAGG",
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "VARIANCE",
        "VAR_POP",
        "VAR_SAMP",
        "VAR",
        "BIT_AND",
        "BIT_OR",
        "BIT_XOR"
    };

    private static bool IsAggregateFunctionName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return false;

        return aggregateFunctionNames.Contains(name.ToUpperInvariant());
    }

    private List<string> ParseReturningItemsRaw()
    {
        var items = new List<string>();

        while (true)
        {
            if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
            {
                if (items.Count == 0)
                    throw new InvalidOperationException(
                        $"RETURNING requires at least one expression (found '{DescribeFoundToken(Peek())}').");
                break;
            }

            if (IsSymbol(Peek(), ","))
                throw new InvalidOperationException(
                    $"RETURNING has an unexpected comma before expression (found '{DescribeFoundToken(Peek())}').");

            var raw = ReadRawExpressionUntilCommaOrTerminator().Trim();
            if (string.IsNullOrWhiteSpace(raw))
                throw new InvalidOperationException(
                    $"RETURNING requires at least one expression (found '{DescribeFoundToken(Peek())}').");

            items.Add(raw);

            if (IsSymbol(Peek(), ","))
            {
                Consume();

                if (IsEnd(Peek()) || IsSymbol(Peek(), ";"))
                    throw new InvalidOperationException(
                        $"RETURNING has a trailing comma without expression (found '{DescribeFoundToken(Peek())}').");

                continue;
            }

            break;
        }

        return items;
    }

    private string ReadRawExpressionUntilCommaOrRightParen()
    {
        var buf = new List<SqlToken>();
        int depth = 0;

        while (!IsEnd(Peek()))
        {
            var t = Peek();

            if (depth == 0 && IsSymbol(t, ";"))
                throw new InvalidOperationException("ON CONFLICT target was not closed correctly (found '<end-of-statement>').");

            if (depth == 0 && (IsSymbol(t, ",") || IsSymbol(t, ")")))
                break;

            if (IsSymbol(t, "("))
                depth++;
            else if (IsSymbol(t, ")"))
            {
                if (depth == 0)
                    throw new InvalidOperationException("ON CONFLICT target has unbalanced parentheses in expression.");
                depth--;
            }

            buf.Add(Consume());
        }

        if (depth != 0)
            throw new InvalidOperationException("ON CONFLICT target has unbalanced parentheses in expression.");

        return TokensToSql(buf);
    }

    private string ReadRawExpressionUntilCommaOrTerminator()
    {
        var buf = new List<SqlToken>();
        int depth = 0;

        while (!IsEnd(Peek()))
        {
            var t = Peek();

            if (depth == 0 && (IsSymbol(t, ",") || IsSymbol(t, ";")))
                break;

            if (IsSymbol(t, "("))
                depth++;
            else if (IsSymbol(t, ")"))
            {
                if (depth == 0)
                    throw new InvalidOperationException("RETURNING has unbalanced parentheses in expression.");
                depth--;
            }

            buf.Add(Consume());
        }

        if (depth != 0)
            throw new InvalidOperationException("RETURNING has unbalanced parentheses in expression.");

        return TokensToSql(buf);
    }

    private List<SqlSelectItem> ParseSelectItemsWithValidation()
    {
        var raws = ParseCommaSeparatedRawItemsUntilAny(
            SqlConst.FROM,
            SqlConst.WHERE,
            SqlConst.GROUP,
            SqlConst.HAVING,
            SqlConst.ORDER,
            SqlConst.LIMIT,
            SqlConst.OFFSET,
            SqlConst.FETCH,
            SqlConst.UNION,
            SqlConst.FOR);
        return raws.ConvertAll(r =>
        {
            // Fail fast on known-invalid patterns before any splitting/normalization.
            // Example: COUNT(DISTINCT DISTINCT id)
            if (Regex.IsMatch(
                    r,
                    @"\bDISTINCT\s+DISTINCT\b",
                    RegexOptions.IgnoreCase))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            var (expr, alias) = SplitTrailingAsAliasTopLevel(r, _dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException("Empty SELECT item.");

            // Fail fast: duplicated DISTINCT inside function calls like COUNT(DISTINCT DISTINCT id)
            // (the expression parser also checks, but this guard prevents corpus regressions when
            // select-item splitting/reconstruction changes token boundaries).
            if (Regex.IsMatch(
                    expr,
                    @"\bDISTINCT\s+DISTINCT\b",
                    RegexOptions.IgnoreCase))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            // Validate select item expressions. This is what makes corpus tests catch
            // typos like "SELEC" inside subqueries, invalid EXISTS(), duplicated DISTINCT, etc.
            _ = SqlExpressionParser.ParseScalar(expr, _dialect);
            return new SqlSelectItem(expr, alias);
        });
    }

    private SqlTableSource ParseFromOrDual()
    {
        if (IsWord(Peek(), SqlConst.FROM))
        {
            Consume();
            if (IsWord(Peek(), SqlConst.FROM))
                throw new InvalidOperationException("invalid: duplicated FROM keyword");
            var ts = ParseTableSource();
            ts = TryParseTableTransforms(ts);
            if (IsWord(Peek(), SqlConst.FROM))
                throw new InvalidOperationException("invalid: FROM inside FROM");
            return ts;
        }
        return new SqlTableSource(null, "DUAL", null, Derived: null, null, null, Pivot: null);
    }

    private List<SqlJoin> ParseJoins(SqlTableSource from)
    {
        var joins = new List<SqlJoin>();
        if (from is null) return joins;
        while (true)
        {
            if (IsSymbol(Peek(), ","))
            {
                Consume();
                var commaTable = TryParseTableTransforms(ParseTableSource());
                joins.Add(new SqlJoin(
                    commaTable.Derived is not null || commaTable.DerivedUnion is not null || commaTable.TableFunction is not null
                        ? SqlJoinType.CrossApply
                        : SqlJoinType.Cross,
                    commaTable,
                    new LiteralExpr(true)));
                continue;
            }

            if (IsJoinStart(Peek()))
            {
                joins.Add(ParseJoin());
                continue;
            }

            break;
        }
        return joins;
    }

    private SqlExpr? TryParseWhereExpr()
    {
        if (!IsWord(Peek(), SqlConst.WHERE)) return null;
        Consume();
        // SqlConst.ON here is important for INSERT ... SELECT ... WHERE ... ON DUPLICATE ...
        var txt = ReadClauseTextUntilTopLevelStop(SqlConst.GROUP, SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.HAVING, SqlConst.FOR, SqlConst.ON, SqlConst.RETURNING);
        return SqlExpressionParser.ParseWhere(txt, _dialect, _parameters);
    }

    private List<string> TryParseGroupBy()
    {
        var list = new List<string>();
        if (!IsWord(Peek(), SqlConst.GROUP)) return list;
        Consume();
        ExpectWord(SqlConst.BY);
        list.AddRange(ParseRawItemsUntil(SqlConst.HAVING, SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING));
        if (list.Count == 0)
            throw new InvalidOperationException("GROUP BY sem expressões.");
        return list;
    }

    private SqlExpr? TryParseHavingExpr()
    {
        if (!IsWord(Peek(), SqlConst.HAVING)) return null;
        Consume();
        var txt = ReadClauseTextUntilTopLevelStop(SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING);
        return SqlExpressionParser.ParseWhere(txt, _dialect, _parameters);
    }

    private List<SqlOrderByItem> TryParseOrderBy()
    {
        var list = new List<SqlOrderByItem>();
        if (!IsWord(Peek(), SqlConst.ORDER)) return list;
        Consume();
        ExpectWord(SqlConst.BY);
        // Reutiliza lógica simplificada
        var raws = _allowInsertSelectSuffixBoundary
            ? ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON)
            : ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING);
        foreach (var r in raws)
        {
            var raw = r.Trim();
            bool? nullsFirst = null;

            if (raw.EndsWith(" NULLS FIRST", StringComparison.OrdinalIgnoreCase))
            {
                if (!_dialect.SupportsOrderByNullsModifier)
                    throw SqlUnsupported.ForDialect(_dialect, "ORDER BY ... NULLS FIRST");
                nullsFirst = true;
                raw = raw[..^12].Trim();
            }
            else if (raw.EndsWith(" NULLS LAST", StringComparison.OrdinalIgnoreCase))
            {
                if (!_dialect.SupportsOrderByNullsModifier)
                    throw SqlUnsupported.ForDialect(_dialect, "ORDER BY ... NULLS LAST");
                nullsFirst = false;
                raw = raw[..^11].Trim();
            }

            var desc = raw.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
            var val = desc
                ? raw[..^5].Trim()
                : (raw.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase)
                    ? raw[..^4].Trim()
                    : raw);
            list.Add(new SqlOrderByItem(val, desc, nullsFirst));
        }
        return list;
    }

    private SqlRowLimit? TryParseRowLimitTail(bool hasOrderBy)
    {
        // MySQL/Postgres: LIMIT ...
        if (IsWord(Peek(), SqlConst.LIMIT))
        {
            if (!_dialect.SupportsLimitOffset && !_dialect.AllowsParserLimitOffsetCompatibility)
                throw SqlUnsupported.ForPagination(_dialect, SqlConst.LIMIT);

            Consume();
            var a = ExpectRowLimitExpr();
            if (IsSymbol(Peek(), ","))
            {
                Consume();
                return new SqlLimitOffset(Count: ExpectRowLimitExpr(), Offset: a);
            }
            if (IsWord(Peek(), SqlConst.OFFSET))
            {
                Consume();
                return new SqlLimitOffset(Count: a, Offset: ExpectRowLimitExpr());
            }
            return new SqlLimitOffset(Count: a, Offset: null);
        }

        // Oracle/SQL Server/Postgres: OFFSET ... FETCH ...
        if (IsWord(Peek(), SqlConst.OFFSET))
        {
            if (!_dialect.SupportsOffsetFetch)
                throw SqlUnsupported.ForPagination(_dialect, SqlConst.OFFSET_FETCH);
            if (_dialect.RequiresOrderByForOffsetFetch && !hasOrderBy)
                throw SqlUnsupported.ForOffsetFetchRequiresOrderBy(_dialect);

            Consume();
            var offset = ExpectRowLimitExpr();
            // Oracle/SQLServer frequentemente exigem ROW/ROWS
            if (IsWord(Peek(), SqlConst.ROW) || IsWord(Peek(), SqlConst.ROWS))
                Consume();

            if (IsWord(Peek(), SqlConst.FETCH))
            {
                Consume();
                // NEXT/FIRST
                if (IsWord(Peek(), SqlConst.NEXT) || IsWord(Peek(), SqlConst.FIRST))
                    Consume();

                var count = ExpectRowLimitExpr();

                if (IsWord(Peek(), SqlConst.ROW) || IsWord(Peek(), SqlConst.ROWS))
                    Consume();

                if (IsWord(Peek(), SqlConst.ONLY))
                    Consume();

                return new SqlLimitOffset(Count: count, Offset: offset);
            }

            return new SqlLimitOffset(Count: new LiteralExpr(int.MaxValue), Offset: offset);
        }

        // Oracle/Postgres: FETCH FIRST n ROWS ONLY
        if (IsWord(Peek(), SqlConst.FETCH))
        {
            if (!_dialect.SupportsFetchFirst)
                throw SqlUnsupported.ForPagination(_dialect, SqlConst.FETCH_FIRST_NEXT);

            Consume();
            if (IsWord(Peek(), SqlConst.NEXT) || IsWord(Peek(), SqlConst.FIRST))
                Consume();

            var count = ExpectRowLimitExpr();

            if (IsWord(Peek(), SqlConst.ROW) || IsWord(Peek(), SqlConst.ROWS))
                Consume();

            if (IsWord(Peek(), SqlConst.ONLY))
                Consume();

            return new SqlLimitOffset(Count: count, Offset: null);
        }

        return null;
    }

    private SqlExpr ExpectRowLimitExpr()
    {
        var t = Peek();
        if (t.Kind == SqlTokenKind.Number)
        {
            Consume();
            return new LiteralExpr(int.Parse(t.Text, CultureInfo.InvariantCulture));
        }
        if (t.Kind == SqlTokenKind.Parameter)
        {
            Consume();
            return new ParameterExpr(t.Text);
        }
        throw new InvalidOperationException($"Esperava número inteiro ou parâmetro para limite de linhas, veio {t.Kind} '{t.Text}'.");
    }

    private void TryConsumeQueryHintOption()
    {
        if (!IsWord(Peek(), SqlConst.OPTION))
            return;

        if (!_dialect.SupportsSqlServerQueryHints)
            throw SqlUnsupported.ForOptionQueryHints(_dialect);

        Consume(); // OPTION
        _ = ReadBalancedParenRawTokens();
    }

    // --- Helpers de CTE e Table Source ---

    private List<SqlCte> TryParseCtes()
    {
        var list = new List<SqlCte>();
        if (!IsWord(Peek(), SqlConst.WITH)) return list;
        Consume();
        if (!_dialect.SupportsWithCte)
            throw SqlUnsupported.ForDialect(_dialect, SqlConst.WITH_CTE);

        if (IsWord(Peek(), SqlConst.RECURSIVE))
        {
            if (!_dialect.SupportsWithRecursive)
                throw SqlUnsupported.ForWithRecursive(_dialect);
            Consume();
        }

        while (true)
        {
            var name = ExpectIdentifier();
            // Pula colunas opcionais (col1, col2)
            if (IsSymbol(Peek(), "("))
            {
                Consume();
                while (!IsSymbol(Peek(), ")")) { Consume(); } // Pula tokens
                Consume(); // )
            }
            ExpectWord(SqlConst.AS);
            if (IsWord(Peek(), SqlConst.NOT) && IsWord(Peek(1), SqlConst.MATERIALIZED))
            {
                if (!_dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(_dialect, "WITH ... AS NOT MATERIALIZED");
                Consume(); // NOT
                Consume(); // MATERIALIZED
            }
            else if (IsWord(Peek(), SqlConst.MATERIALIZED))
            {
                if (!_dialect.SupportsWithMaterializedHint)
                    throw SqlUnsupported.ForDialect(_dialect, "WITH ... AS MATERIALIZED");
                Consume();
            }
            var innerSql = ReadBalancedParenRawTokens();
            var q = Parse(innerSql, _dialect);
            if (q is SqlSelectQuery or SqlUnionQuery)
                list.Add(new SqlCte(name, q));

            if (IsSymbol(Peek(), ",")) { Consume(); continue; }
            break;
        }
        return list;
    }

    private SqlTableSource ParseTableSource(
        bool consumeHints = true,
        bool allowFunctionSource = true,
        IReadOnlyCollection<string>? aliasStopWords = null)
    {
        if (IsSymbol(Peek(), "("))
        {
            var innerSql = ReadBalancedParenRawTokens();
            var alias = ReadOptionalAlias(aliasStopWords);

            // Derived table pode ser um SELECT simples OU um UNION/UNION ALL.
            // O Parse() atual devolve apenas o primeiro SELECT quando existe UNION,
            // então aqui detectamos e parseamos a cadeia completa.
            var parsed = Parse(innerSql, _dialect);
            if (parsed is SqlUnionQuery union)
            {
                return new SqlTableSource(
                    null,
                    null,
                    alias,
                    Derived: null,
                    DerivedUnion: new UnionChain(union.Parts, union.AllFlags, union.OrderBy, union.RowLimit),
                    DerivedSql: innerSql,
                    Pivot: null);
            }

            if (parsed is SqlSelectQuery sq)
                return new SqlTableSource(null, null, alias, sq, null, innerSql, Pivot: null);

            throw new InvalidOperationException("Derived table deve ser um SELECT");
        }

        var first = ExpectIdentifier();

        if (allowFunctionSource && IsSupportedTableFunctionName(first) && IsSymbol(Peek(), "("))
            return ParseTableFunctionSource(first);

        if (allowFunctionSource
            && IsSymbol(Peek(), ".")
            && Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
            && IsSupportedTableFunctionName(Peek(1).Text)
            && IsSymbol(Peek(2), "("))
        {
            Consume(); // .
            return ParseTableFunctionSource(ExpectIdentifier(), first);
        }

        string? db = null;
        var table = first;
        var mySqlIndexHints = new List<SqlMySqlIndexHint>();
        if (IsSymbol(Peek(), "."))
        {
            Consume();
            db = table;
            table = ExpectIdentifier();
        }
        if (consumeHints)
            mySqlIndexHints.AddRange(ConsumeTableHintsIfPresent());
        var alias2 = ReadOptionalAlias(aliasStopWords);
        if (consumeHints)
            mySqlIndexHints.AddRange(ConsumeTableHintsIfPresent());
        return new SqlTableSource(
            db,
            table,
            alias2,
            null,
            null,
            null,
            Pivot: null,
            MySqlIndexHints: mySqlIndexHints);
    }

    private SqlTableSource ParseTableFunctionSource(string functionName, string? schemaName = null)
    {
        if (functionName.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
            return ParseJsonTableSource(functionName, schemaName);

        var argsSql = ReadBalancedParenRawTokens();
        var function = new FunctionCallExpr(
            functionName,
            [.. SplitRawByComma(argsSql)
                .Select(static arg => arg.Trim())
                .Where(static arg => arg.Length > 0)
                .Select(arg => SqlExpressionParser.ParseScalar(arg, _dialect, _parameters))]);

        ValidateTableFunctionSource(function);

        if (function.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase)
            && IsWord(Peek(), SqlConst.WITH) && IsSymbol(Peek(1), "("))
        {
            Consume(); // WITH
            var rawSchema = ReadBalancedParenRawTokens();
            var aliasWithSchema = ReadOptionalAlias();
            return new SqlTableSource(
                schemaName,
                null,
                aliasWithSchema,
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                MySqlIndexHints: null,
                TableFunction: function,
                OpenJsonWithClause: ParseOpenJsonWithClause(rawSchema));
        }

        var alias = ReadOptionalAlias();
        return new SqlTableSource(
            schemaName,
            null,
            alias,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null,
            TableFunction: function,
            OpenJsonWithClause: null,
            JsonTableClause: null);
    }

    private SqlTableSource ParseJsonTableSource(string functionName, string? schemaName)
    {
        var argsSql = ReadBalancedParenRawTokens();
        var parts = SplitRawByComma(argsSql)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();
        if (parts.Count != 2)
            throw new NotSupportedException("JSON_TABLE table source currently supports json document plus path/COLUMNS clause in the mock.");

        var columnsKeywordIndex = IndexOfTopLevelKeyword(parts[1], SqlConst.COLUMNS);
        if (columnsKeywordIndex < 0)
            throw new InvalidOperationException("JSON_TABLE requires a COLUMNS clause.");

        var pathSql = parts[1][..columnsKeywordIndex].Trim();
        if (string.IsNullOrWhiteSpace(pathSql))
            throw new InvalidOperationException("JSON_TABLE requires a row path expression before COLUMNS.");

        var columnsSegment = parts[1][(columnsKeywordIndex + SqlConst.COLUMNS.Length)..].TrimStart();
        if (!TryExtractSingleParenthesizedBlock(columnsSegment, out var rawColumns, out var trailingSql))
            throw new InvalidOperationException("JSON_TABLE COLUMNS clause must be enclosed in parentheses.");

        if (!string.IsNullOrWhiteSpace(trailingSql))
            throw new InvalidOperationException($"JSON_TABLE has unexpected tokens after COLUMNS clause: '{trailingSql.Trim()}'.");

        var function = new FunctionCallExpr(
            functionName,
            [
                SqlExpressionParser.ParseScalar(parts[0], _dialect, _parameters),
                SqlExpressionParser.ParseScalar(pathSql, _dialect, _parameters)
            ]);

        ValidateTableFunctionSource(function);

        var alias = ReadOptionalAlias();
        return new SqlTableSource(
            schemaName,
            null,
            alias,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null,
            TableFunction: function,
            OpenJsonWithClause: null,
            JsonTableClause: ParseJsonTableClause(rawColumns));
    }

    private void ValidateTableFunctionSource(FunctionCallExpr function)
    {
        if (function.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            if (!_dialect.SupportsOpenJsonFunction)
                throw SqlUnsupported.ForDialect(_dialect, SqlConst.OPENJSON);

            if (function.Args.Count is < 1 or > 2)
                throw new NotSupportedException("OPENJSON table source currently supports one or two arguments in the mock.");

            return;
        }

        if (function.Name.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase))
        {
            if (function.Args.Count == 3 && !_dialect.SupportsStringSplitOrdinalArgument)
                throw SqlUnsupported.ForDialect(_dialect, "STRING_SPLIT enable_ordinal");

            if (!_dialect.SupportsStringSplitFunction)
                throw SqlUnsupported.ForDialect(_dialect, SqlConst.STRING_SPLIT);

            if (function.Args.Count is < 2 or > 3)
                throw new NotSupportedException("STRING_SPLIT table source currently supports two or three arguments in the mock.");

            return;
        }

        if (function.Name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
        {
            if (!_dialect.SupportsJsonTableFunction)
                throw SqlUnsupported.ForDialect(_dialect, SqlConst.JSON_TABLE);

            if (function.Args.Count != 2)
                throw new NotSupportedException("JSON_TABLE table source currently supports exactly two arguments in the mock.");

            return;
        }

        throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");
    }

    private static bool IsSupportedTableFunctionName(string functionName)
        => functionName.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase);

    private static SqlOpenJsonWithClause ParseOpenJsonWithClause(string rawSchema)
    {
        var items = SplitRawByComma(rawSchema)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();

        if (items.Count == 0)
            throw new InvalidOperationException("OPENJSON WITH requires at least one column definition.");

        var columns = items
            .Select(ParseOpenJsonWithColumn)
            .ToList();

        return new SqlOpenJsonWithClause(columns);
    }

    private static SqlOpenJsonWithColumn ParseOpenJsonWithColumn(string rawItem)
    {
        var item = rawItem.Trim();
        var asJson = false;
        if (item.EndsWith(" AS JSON", StringComparison.OrdinalIgnoreCase))
        {
            asJson = true;
            item = item[..^8].TrimEnd();
        }

        string? path = null;
        var pathMatch = Regex.Match(
            item,
            @"\s+(?<path>N?'(?:''|[^'])*')\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (pathMatch.Success)
        {
            path = UnquoteSqlStringLiteral(pathMatch.Groups["path"].Value);
            item = item[..pathMatch.Index].TrimEnd();
        }

        var nameAndTypeMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+)$",
            RegexOptions.CultureInvariant);
        if (!nameAndTypeMatch.Success)
            throw new InvalidOperationException($"OPENJSON WITH column definition is invalid: '{rawItem}'.");

        var name = nameAndTypeMatch.Groups["name"].Value.NormalizeName();
        var sqlType = nameAndTypeMatch.Groups["type"].Value.Trim();
        if (string.IsNullOrWhiteSpace(sqlType))
            throw new InvalidOperationException($"OPENJSON WITH column '{name}' requires a SQL type.");

        return new SqlOpenJsonWithColumn(
            name,
            sqlType,
            ParseOpenJsonColumnDbType(sqlType),
            path,
            asJson);
    }

    private static SqlJsonTableClause ParseJsonTableClause(string rawColumns)
    {
        var items = SplitRawByComma(rawColumns)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();

        if (items.Count == 0)
            throw new InvalidOperationException("JSON_TABLE COLUMNS requires at least one column definition.");

        return new SqlJsonTableClause([.. items.Select(ParseJsonTableEntry)]);
    }

    private static SqlJsonTableEntry ParseJsonTableEntry(string rawItem)
    {
        var item = rawItem.Trim();
        if (!item.StartsWith(SqlConst.NESTED, StringComparison.OrdinalIgnoreCase))
            return ParseJsonTableColumn(rawItem);

        var nestedMatch = Regex.Match(
            item,
            @"^NESTED(?:\s+PATH)?\s+(?<path>N?'(?:''|[^'])*')\s+(?<rest>.+)$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!nestedMatch.Success)
            throw new InvalidOperationException($"JSON_TABLE nested path definition is invalid: '{rawItem}'.");

        var rest = nestedMatch.Groups["rest"].Value.TrimStart();
        if (!rest.StartsWith(SqlConst.COLUMNS, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"JSON_TABLE nested path requires COLUMNS clause: '{rawItem}'.");

        var rawColumns = rest[SqlConst.COLUMNS.Length..].TrimStart();
        if (!TryExtractSingleParenthesizedBlock(rawColumns, out var nestedColumnsRaw, out var trailingSql))
            throw new InvalidOperationException("JSON_TABLE nested COLUMNS clause must be enclosed in parentheses.");

        if (!string.IsNullOrWhiteSpace(trailingSql))
            throw new InvalidOperationException($"JSON_TABLE nested path has unexpected tokens after COLUMNS clause: '{trailingSql.Trim()}'.");

        return new SqlJsonTableNestedPath(
            UnquoteSqlStringLiteral(nestedMatch.Groups["path"].Value),
            ParseJsonTableClause(nestedColumnsRaw));
    }

    private static SqlJsonTableColumn ParseJsonTableColumn(string rawItem)
    {
        var item = rawItem.Trim();
        var ordinalityMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+FOR\s+ORDINALITY$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (ordinalityMatch.Success)
        {
            return new SqlJsonTableColumn(
                ordinalityMatch.Groups["name"].Value.NormalizeName(),
                "BIGINT",
                DbType.Int64,
                null,
                true);
        }

        var existsPathMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+?)\s+EXISTS\s+PATH\s+(?<path>N?'(?:''|[^'])*')$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (existsPathMatch.Success)
        {
            var existsName = existsPathMatch.Groups["name"].Value.NormalizeName();
            var existsTypeSql = existsPathMatch.Groups["type"].Value.Trim();
            if (string.IsNullOrWhiteSpace(existsTypeSql))
                throw new InvalidOperationException($"JSON_TABLE column '{existsName}' requires a SQL type.");

            return new SqlJsonTableColumn(
                existsName,
                existsTypeSql,
                ParseOpenJsonColumnDbType(existsTypeSql),
                UnquoteSqlStringLiteral(existsPathMatch.Groups["path"].Value),
                false,
                true);
        }

        var onError = ParseJsonTableColumnFallback(ref item, "ON ERROR");
        var onEmpty = ParseJsonTableColumnFallback(ref item, "ON EMPTY");

        string? path = null;
        var pathMatch = Regex.Match(
            item,
            @"\s+PATH\s+(?<path>N?'(?:''|[^'])*')\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (pathMatch.Success)
        {
            path = UnquoteSqlStringLiteral(pathMatch.Groups["path"].Value);
            item = item[..pathMatch.Index].TrimEnd();
        }

        var nameAndTypeMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+)$",
            RegexOptions.CultureInvariant);
        if (!nameAndTypeMatch.Success)
            throw new InvalidOperationException($"JSON_TABLE column definition is invalid: '{rawItem}'.");

        var name = nameAndTypeMatch.Groups["name"].Value.NormalizeName();
        var sqlType = nameAndTypeMatch.Groups["type"].Value.Trim();
        if (string.IsNullOrWhiteSpace(sqlType))
            throw new InvalidOperationException($"JSON_TABLE column '{name}' requires a SQL type.");

        return new SqlJsonTableColumn(
            name,
            sqlType,
            ParseOpenJsonColumnDbType(sqlType),
            path,
            false,
            false,
            onEmpty,
            onError);
    }

    private static SqlJsonTableColumnFallback? ParseJsonTableColumnFallback(ref string item, string clauseName)
    {
        var pattern = clauseName.Equals("ON EMPTY", StringComparison.OrdinalIgnoreCase)
            ? @"^(?<prefix>.*)\s+(?<kind>NULL|ERROR|DEFAULT\s+(?<value>N?'(?:''|[^'])*'))\s+ON\s+EMPTY$"
            : @"^(?<prefix>.*)\s+(?<kind>NULL|ERROR|DEFAULT\s+(?<value>N?'(?:''|[^'])*'))\s+ON\s+ERROR$";

        var match = Regex.Match(
            item,
            pattern,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
        if (!match.Success)
            return null;

        item = match.Groups["prefix"].Value.TrimEnd();
        var kind = match.Groups["kind"].Value.Trim();
        return kind.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase)
            ? new SqlJsonTableColumnFallback(SqlJsonTableColumnFallbackKind.Null)
            : kind.Equals("ERROR", StringComparison.OrdinalIgnoreCase)
                ? new SqlJsonTableColumnFallback(SqlJsonTableColumnFallbackKind.Error)
                : new SqlJsonTableColumnFallback(
                    SqlJsonTableColumnFallbackKind.Default,
                    UnquoteSqlStringLiteral(match.Groups["value"].Value));
    }

    private static int IndexOfTopLevelKeyword(string sql, string keyword)
    {
        var depth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;
        var inBacktick = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < sql.Length && sql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '"')
                    inDoubleQuote = false;

                continue;
            }

            if (inBacktick)
            {
                if (ch == '`')
                    inBacktick = false;

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '`')
            {
                inBacktick = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (!MatchesKeywordAt(sql, keyword, i))
                continue;

            var beforeOk = i == 0 || !IsKeywordIdentifierPart(sql[i - 1]);
            var afterIndex = i + keyword.Length;
            var afterOk = afterIndex >= sql.Length || !IsKeywordIdentifierPart(sql[afterIndex]);
            if (beforeOk && afterOk)
                return i;
        }

        return -1;
    }

    private static bool TryExtractSingleParenthesizedBlock(string sql, out string inner, out string trailingSql)
    {
        inner = string.Empty;
        trailingSql = string.Empty;
        if (string.IsNullOrWhiteSpace(sql) || sql[0] != '(')
            return false;

        var depth = 0;
        var inSingleQuote = false;
        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];
            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < sql.Length && sql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch != ')')
                continue;

            depth--;
            if (depth != 0)
                continue;

            inner = sql[1..i];
            trailingSql = sql[(i + 1)..];
            return true;
        }

        return false;
    }

    private static bool MatchesKeywordAt(string sql, string keyword, int index)
        => sql.AsSpan(index).StartsWith(keyword, StringComparison.OrdinalIgnoreCase);

    private static bool IsKeywordIdentifierPart(char ch)
        => char.IsLetterOrDigit(ch) || ch is '_' or '$' or '#';

    private static string UnquoteSqlStringLiteral(string token)
    {
        var trimmed = token.Trim();
        if (trimmed.StartsWith("N'", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[1..];

        if (trimmed.Length >= 2 && trimmed[0] == '\'' && trimmed[^1] == '\'')
            return trimmed[1..^1].Replace("''", "'");

        return trimmed;
    }

    private static readonly Dictionary<string, DbType> fnDtType = new()
    {
        { "DATETIMEOFFSET", DbType.DateTimeOffset},
        { "DATETIME2", DbType.DateTime},
        { "DATETIME", DbType.DateTime},
        { "SMALLDATETIME", DbType.DateTime },
        { "DATE", DbType.Date },
        { "TIME", DbType.Time },
        { "BIGINT", DbType.Int64 },
        { "INT", DbType.Int32 },
        { "INTEGER", DbType.Int32 },
        { "SMALLINT", DbType.Int16 },
        { "TINYINT", DbType.Byte },
        { "DECIMAL", DbType.Decimal },
        { "NUMERIC", DbType.Decimal },
        { "MONEY", DbType.Currency },
        { "SMALLMONEY", DbType.Currency },
        { "FLOAT", DbType.Single },
        { "REAL", DbType.Double },
        { "BIT", DbType.Boolean },
        { "UNIQUEIDENTIFIER", DbType.Guid },
        { "VARBINARY", DbType.Binary },
        { "BINARY", DbType.Binary },
        { "IMAGE", DbType.Binary },
        { "XML", DbType.Binary },
    };

    private static DbType ParseOpenJsonColumnDbType(string sqlType)
    {
        var normalized = sqlType.Trim().NormalizeName().ToUpperInvariant().Split(' ')[0];
        return fnDtType.TryGetValue(normalized, out var dt)
            ? dt
            : DbType.String;
    }

    private SqlTableSource TryParseTableTransforms(SqlTableSource source)
    {
        source = TryParsePivot(source);
        source = TryParseUnpivot(source);
        return source;
    }

    private SqlTableSource TryParsePivot(SqlTableSource source)
    {
        if (!IsWord(Peek(), SqlConst.PIVOT))
            return source;

        if (!_dialect.SupportsPivotClause)
            throw SqlUnsupported.ForDialect(_dialect, SqlConst.PIVOT);

        Consume(); // PIVOT
        var raw = ReadBalancedParenRawTokens();
        var spec = ParsePivotSpec(raw);

        var pivotAlias = ReadOptionalAlias();
        return source with
        {
            Alias = pivotAlias ?? source.Alias,
            Pivot = spec
        };
    }

    private SqlTableSource TryParseUnpivot(SqlTableSource source)
    {
        if (!IsWord(Peek(), SqlConst.UNPIVOT))
            return source;

        if (!_dialect.SupportsUnpivotClause)
            throw SqlUnsupported.ForDialect(_dialect, SqlConst.UNPIVOT);

        Consume(); // UNPIVOT
        var raw = ReadBalancedParenRawTokens();
        var spec = ParseUnpivotSpec(raw);

        var unpivotAlias = ReadOptionalAlias();
        return source with
        {
            Alias = unpivotAlias ?? source.Alias,
            Unpivot = spec
        };
    }

    private static SqlPivotSpec ParsePivotSpec(string raw)
    {
        var m = Regex.Match(
            raw,
            @"^\s*(?<agg>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<arg>[^\)]+?)\s*\)\s+FOR\s+(?<for>[A-Za-z_][A-Za-z0-9_\.]*)\s+IN\s*\((?<in>.+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!m.Success)
            throw new InvalidOperationException("invalid: unsupported PIVOT syntax");

        var aggregateFunction = m.Groups["agg"].Value.Trim();
        var aggregateArgRaw = m.Groups["arg"].Value.Trim();
        var forColumnRaw = m.Groups["for"].Value.Trim();
        var inListRaw = m.Groups["in"].Value.Trim();

        var inItems = new List<SqlPivotInItem>();
        foreach (var itemRaw in SplitPivotInItems(inListRaw))
        {
            var item = itemRaw.Trim();
            if (item.Length == 0)
                continue;

            var im = Regex.Match(item, @"^(?<val>.+?)(?:\s+AS\s+(?<alias>[A-Za-z_][A-Za-z0-9_]*))?$", RegexOptions.IgnoreCase);
            if (!im.Success)
                throw new InvalidOperationException("invalid: unsupported PIVOT IN item");

            var valueRaw = im.Groups["val"].Value.Trim();
            var alias = im.Groups["alias"].Success
                ? im.Groups["alias"].Value.Trim()
                : valueRaw.Trim('\'', '"').Replace('.', '_');

            if (string.IsNullOrWhiteSpace(alias))
                throw new InvalidOperationException("invalid: PIVOT IN item alias");

            inItems.Add(new SqlPivotInItem(valueRaw, alias));
        }

        if (inItems.Count == 0)
            throw new InvalidOperationException("invalid: PIVOT IN list is empty");

        return new SqlPivotSpec(aggregateFunction, aggregateArgRaw, forColumnRaw, inItems);
    }

    private static SqlUnpivotSpec ParseUnpivotSpec(string raw)
    {
        const string identifierPattern = @"(?:\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)";

        var match = Regex.Match(
            raw,
            $@"^\s*(?<value>{identifierPattern})\s+FOR\s+(?<name>{identifierPattern})\s+IN\s*\((?<in>.+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

        if (!match.Success)
            throw new InvalidOperationException("invalid: unsupported UNPIVOT syntax");

        var valueColumnName = match.Groups["value"].Value.NormalizeName();
        var nameColumnName = match.Groups["name"].Value.NormalizeName();
        var inListRaw = match.Groups["in"].Value.Trim();

        var inItems = new List<SqlUnpivotInItem>();
        foreach (var itemRaw in SplitPivotInItems(inListRaw))
        {
            var item = itemRaw.Trim();
            if (item.Length == 0)
                continue;

            if (!Regex.IsMatch(item, $"^{identifierPattern}$", RegexOptions.CultureInvariant))
                throw new InvalidOperationException("invalid: unsupported UNPIVOT IN item");

            var normalized = item.NormalizeName();
            inItems.Add(new SqlUnpivotInItem(normalized, normalized));
        }

        if (inItems.Count == 0)
            throw new InvalidOperationException("invalid: UNPIVOT IN list is empty");

        return new SqlUnpivotSpec(valueColumnName, nameColumnName, inItems);
    }

    private SqlForJsonClause? TryParseForJsonClause()
    {
        if (!IsWord(Peek(), SqlConst.FOR) || !IsWord(Peek(1), "JSON"))
            return null;

        if (!_dialect.SupportsForJsonClause)
            throw SqlUnsupported.ForDialect(_dialect, SqlConst.FOR_JSON);

        Consume(); // FOR
        Consume(); // JSON

        SqlForJsonMode mode;
        if (IsWord(Peek(), SqlConst.PATH))
        {
            mode = SqlForJsonMode.Path;
            Consume();
        }
        else if (IsWord(Peek(), SqlConst.AUTO))
        {
            mode = SqlForJsonMode.Auto;
            Consume();
        }
        else
        {
            throw new InvalidOperationException("FOR JSON requires PATH or AUTO mode.");
        }

        string? rootName = null;
        var includeNullValues = false;
        var withoutArrayWrapper = false;

        while (IsSymbol(Peek(), ","))
        {
            Consume();

            if (IsWord(Peek(), SqlConst.ROOT))
            {
                if (rootName is not null)
                    throw new InvalidOperationException("FOR JSON ROOT option cannot be specified more than once.");

                Consume();
                if (!IsSymbol(Peek(), "("))
                    throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

                var rootArgRaw = ReadBalancedParenRawTokens().Trim();
                rootName = ParseForJsonRootName(rootArgRaw);
                continue;
            }

            if (IsWord(Peek(), SqlConst.INCLUDE_NULL_VALUES))
            {
                if (includeNullValues)
                    throw new InvalidOperationException("FOR JSON INCLUDE_NULL_VALUES option cannot be specified more than once.");

                includeNullValues = true;
                Consume();
                continue;
            }

            if (IsWord(Peek(), SqlConst.WITHOUT_ARRAY_WRAPPER))
            {
                if (withoutArrayWrapper)
                    throw new InvalidOperationException("FOR JSON WITHOUT_ARRAY_WRAPPER option cannot be specified more than once.");

                withoutArrayWrapper = true;
                Consume();
                continue;
            }

            throw new InvalidOperationException($"FOR JSON option '{Peek().Text}' is not supported in the mock.");
        }

        return new SqlForJsonClause(mode, rootName, includeNullValues, withoutArrayWrapper);
    }

    private static string ParseForJsonRootName(string raw)
    {
        var trimmed = raw.Trim();
        if (trimmed.Length == 0)
            throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

        if (!Regex.IsMatch(trimmed, @"^N?'(?:''|[^'])*'$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            throw new InvalidOperationException("FOR JSON ROOT requires a string literal root name.");

        return UnquoteSqlStringLiteral(trimmed);
    }

    private static IEnumerable<string> SplitPivotInItems(string raw)
    {
        var list = new List<string>();
        var sb = new StringBuilder();
        int depth = 0;

        foreach (var ch in raw)
        {
            if (ch == '(') depth++;
            if (ch == ')') depth--;

            if (ch == ',' && depth == 0)
            {
                list.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            sb.Append(ch);
        }

        if (sb.Length > 0)
            list.Add(sb.ToString());

        return list;
    }

    private IReadOnlyList<SqlMySqlIndexHint> ConsumeTableHintsIfPresent()
    {
        var mySqlHints = new List<SqlMySqlIndexHint>();

        while (true)
        {
            if (IsWord(Peek(), SqlConst.WITH) && IsSymbol(Peek(1), "("))
            {
                if (!_dialect.SupportsSqlServerTableHints)
                    throw SqlUnsupported.ForDialect(_dialect, "WITH(table hints)");

                Consume(); // WITH
                _ = ReadBalancedParenRawTokens();
                continue;
            }

            if (IsSymbol(Peek(), "("))
            {
                if (!_dialect.SupportsSqlServerTableHints)
                    break;

                _ = ReadBalancedParenRawTokens();
                continue;
            }

            if (IsWord(Peek(), SqlConst.USE) || IsWord(Peek(), SqlConst.IGNORE) || IsWord(Peek(), SqlConst.FORCE))
            {
                if (!_dialect.SupportsMySqlIndexHints)
                    throw SqlUnsupported.ForDialect(_dialect, "INDEX hints");

                mySqlHints.Add(ConsumeMySqlIndexHint());
                continue;
            }

            break;
        }

        return mySqlHints;
    }

    private SqlMySqlIndexHint ConsumeMySqlIndexHint()
    {
        var kindToken = Consume(); // USE | IGNORE | FORCE
        var kind = kindToken.Text.NormalizeName();
        SqlMySqlIndexHintKind mappedKind;
        if (kind.Equals("use", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Use;
        else if (kind.Equals("ignore", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Ignore;
        else if (kind.Equals("force", StringComparison.OrdinalIgnoreCase))
            mappedKind = SqlMySqlIndexHintKind.Force;
        else
            throw new InvalidOperationException("MySQL index hint inválido: tipo de hint desconhecido.");

        if (IsWord(Peek(), SqlConst.INDEX) || IsWord(Peek(), "KEY"))
        {
            Consume();
        }
        else
        {
            throw new InvalidOperationException("MySQL index hint inválido: esperado INDEX/KEY.");
        }

        var scope = SqlMySqlIndexHintScope.Any;
        if (IsWord(Peek(), SqlConst.FOR))
        {
            Consume();
            if (IsWord(Peek(), SqlConst.JOIN))
            {
                Consume();
                scope = SqlMySqlIndexHintScope.Join;
            }
            else if (IsWord(Peek(), SqlConst.ORDER))
            {
                Consume();
                ExpectWord(SqlConst.BY);
                scope = SqlMySqlIndexHintScope.OrderBy;
            }
            else if (IsWord(Peek(), SqlConst.GROUP))
            {
                Consume();
                ExpectWord(SqlConst.BY);
                scope = SqlMySqlIndexHintScope.GroupBy;
            }
            else
            {
                throw new InvalidOperationException("MySQL index hint inválido: esperado JOIN, ORDER BY ou GROUP BY após FOR.");
            }
        }

        if (!IsSymbol(Peek(), "("))
            throw new InvalidOperationException("MySQL index hint inválido: esperado lista de índices entre parênteses.");

        var hintIndexListRaw = ReadBalancedParenRawTokens();
        var indexNames = ValidateMySqlIndexHintList(hintIndexListRaw);

        return new SqlMySqlIndexHint(mappedKind, scope, indexNames);
    }

    private static IReadOnlyList<string> ValidateMySqlIndexHintList(string hintIndexListRaw)
    {
        var rawItems = hintIndexListRaw.Split(',').Select(static x => x.Trim()).ToList();

        if (rawItems.Count == 0 || rawItems.All(static x => x.Length == 0))
            throw new InvalidOperationException("MySQL index hint inválido: lista de índices vazia.");

        if (rawItems.Any(static x => x.Length == 0))
            throw new InvalidOperationException("MySQL index hint inválido: lista contém item vazio.");

        var parsedItems = new List<string>(rawItems.Count);
        foreach (var item in rawItems)
        {
            if (item.Equals(SqlConst.PRIMARY, StringComparison.OrdinalIgnoreCase))
            {
                parsedItems.Add(SqlConst.PRIMARY);
                continue;
            }

            // MySQL quoted identifier with backticks; supports escaped backtick as `` inside name.
            if (Regex.IsMatch(item, @"^`(?:``|[^`])+`$", RegexOptions.CultureInvariant))
            {
                parsedItems.Add(UnquoteMySqlIdentifier(item));
                continue;
            }

            // Unquoted: accept common MySQL identifier chars including '$'.
            if (Regex.IsMatch(item, @"^[A-Za-z_$][A-Za-z0-9_$]*$", RegexOptions.CultureInvariant))
            {
                parsedItems.Add(item);
                continue;
            }

            throw new InvalidOperationException($"MySQL index hint inválido: índice '{item}' não é válido.");
        }

        return parsedItems;
    }

    private static string UnquoteMySqlIdentifier(string item)
        => item[1..^1].Replace("``", "`");

    private SqlJoin ParseJoin()
    {
        if (IsWord(Peek(), SqlConst.CROSS) && IsWord(Peek(1), SqlConst.APPLY))
        {
            if (!_dialect.SupportsApplyClause)
                throw CreateApplyUnsupportedException("CROSS APPLY", 2);

            Consume(); // CROSS
            Consume(); // APPLY

            var tableCross = ParseTableSource();
            ValidateApplySource(tableCross, "CROSS APPLY");
            return new SqlJoin(SqlJoinType.CrossApply, tableCross, new LiteralExpr(true));
        }

        if (IsWord(Peek(), SqlConst.OUTER) && IsWord(Peek(1), SqlConst.APPLY))
        {
            if (!_dialect.SupportsApplyClause)
                throw CreateApplyUnsupportedException("OUTER APPLY", 2);

            Consume(); // OUTER
            Consume(); // APPLY

            var tableOuter = ParseTableSource();
            ValidateApplySource(tableOuter, "OUTER APPLY");
            return new SqlJoin(SqlJoinType.OuterApply, tableOuter, new LiteralExpr(true));
        }

        var type = SqlJoinType.Inner;
        if (IsWord(Peek(), SqlConst.LEFT)) { Consume(); type = SqlJoinType.Left; }
        else if (IsWord(Peek(), SqlConst.RIGHT)) { Consume(); type = SqlJoinType.Right; }
        else if (IsWord(Peek(), SqlConst.CROSS)) { Consume(); type = SqlJoinType.Cross; }
        else if (IsWord(Peek(), SqlConst.INNER)) { Consume(); type = SqlJoinType.Inner; }
        if (IsWord(Peek(), SqlConst.OUTER)) Consume();
        ExpectWord(SqlConst.JOIN);

        var isLateral = false;
        if (IsWord(Peek(), "LATERAL"))
        {
            Consume();
            isLateral = true;
        }

        var table = TryParseTableTransforms(ParseTableSource());
        if (isLateral)
            table = table with { IsLateral = true };
        SqlExpr onExpr = new LiteralExpr(true);

        if (type != SqlJoinType.Cross)
        {
            ExpectWord(SqlConst.ON);
            var txt = ReadClauseTextUntilTopLevelStop(SqlConst.JOIN, SqlConst.LEFT, SqlConst.RIGHT, SqlConst.INNER, SqlConst.CROSS, SqlConst.OUTER, SqlConst.APPLY, SqlConst.WHERE, SqlConst.GROUP, SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION);
            onExpr = SqlExpressionParser.ParseWhere(txt, _dialect, _parameters);
        }
        return new SqlJoin(type, table, onExpr);
    }

    private NotSupportedException CreateApplyUnsupportedException(string clause, int sourceOffset)
    {
        var functionInfo = TryPeekApplyTableFunctionInfo(sourceOffset);
        if (functionInfo is not null)
        {
            var (functionName, argCount) = functionInfo.Value;

            if (functionName.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase) && !_dialect.SupportsOpenJsonFunction)
                return SqlUnsupported.ForDialect(_dialect, SqlConst.OPENJSON);

            if (functionName.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase))
            {
                if (argCount == 3 && !_dialect.SupportsStringSplitOrdinalArgument)
                    return SqlUnsupported.ForDialect(_dialect, "STRING_SPLIT enable_ordinal");

                if (!_dialect.SupportsStringSplitFunction)
                    return SqlUnsupported.ForDialect(_dialect, SqlConst.STRING_SPLIT);
            }
        }

        return SqlUnsupported.ForDialect(_dialect, clause);
    }

    private (string Name, int ArgCount)? TryPeekApplyTableFunctionInfo(int startOffset)
    {
        var parts = new List<string>();
        for (var offset = startOffset; offset <= startOffset + 24; offset++)
        {
            var token = Peek(offset);
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;
            parts.Add(token.Text);
        }

        if (parts.Count == 0)
            return null;

        var snippet = string.Join(" ", parts);
        var match = Regex.Match(
            snippet,
            @"(?ix)\b(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*(OPENJSON|STRING_SPLIT)\s*\(");
        if (!match.Success)
            return null;

        var functionName = match.Groups[1].Value;
        var openParenIndex = snippet.IndexOf('(', match.Index + match.Length - 1);
        if (openParenIndex < 0)
            return (functionName, 0);

        return (functionName, CountFunctionArgsInSnippet(snippet, openParenIndex));
    }

    private static int CountFunctionArgsInSnippet(string snippet, int openParenIndex)
    {
        var depth = 0;
        var argCount = 0;
        var sawTokenInCurrentArg = false;

        for (var index = openParenIndex; index < snippet.Length; index++)
        {
            var ch = snippet[index];

            if (ch == '(')
            {
                depth++;
                if (depth > 1)
                    sawTokenInCurrentArg = true;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    if (sawTokenInCurrentArg)
                        argCount++;

                    return argCount;
                }

                sawTokenInCurrentArg = true;
                continue;
            }

            if (depth == 1 && ch == ',')
            {
                if (sawTokenInCurrentArg)
                {
                    argCount++;
                    sawTokenInCurrentArg = false;
                }

                continue;
            }

            if (depth >= 1 && !char.IsWhiteSpace(ch))
                sawTokenInCurrentArg = true;
        }

        return 0;
    }

    private static void ValidateApplySource(SqlTableSource table, string clause)
    {
        if (table.Derived is not null || table.DerivedUnion is not null || table.TableFunction is not null)
            return;

        throw new NotSupportedException($"{clause} currently supports only derived subqueries and supported table-valued functions in the mock.");
    }

    // --- Helpers de Token Plumbing ---

    private string? ReadOptionalAlias(IReadOnlyCollection<string>? additionalStopWords = null)
    {
        if (IsWord(Peek(), SqlConst.AS))
        {
            // Se depois do AS vier uma keyword (SELECT/WITH/VALUES/SET/etc),
            // isso NÃƒO é alias â€” é parte da sintaxe do comando (ex: CREATE ... AS SELECT).
            var next = Peek(1);
            if (next.Kind == SqlTokenKind.Identifier && IsClauseKeywordToken(next, additionalStopWords))
                return null;

            Consume(); // AS
            return ExpectIdentifier();
        }

        var t = Peek();
        if (t.Kind == SqlTokenKind.Identifier && !IsClauseKeywordToken(t, additionalStopWords))
            return Consume().Text;

        return null;
    }

    private List<string> ParseRawItemsUntil(params string[] stopWords)
        => ParseCommaSeparatedRawItemsUntilAny(stopWords);

    private List<string> ParseCommaSeparatedRawItemsUntilAny(params string[] stopWords)
    {
        var items = new List<string>();
        var buf = new List<SqlToken>();
        int depth = 0;
        while (!IsEnd(Peek()))
        {
            var t = Peek();
            if (IsSymbol(t, "(")) depth++;
            else if (IsSymbol(t, ")")) depth--;

            if (depth == 0 && IsSymbol(t, ";")) break;
            if (depth == 0 && ShouldStopAtTopLevelToken(t, stopWords, buf)) break;

            if (depth == 0 && IsSymbol(t, ","))
            {
                Consume();
                items.Add(TokensToSql(buf));
                buf.Clear();
                continue;
            }
            buf.Add(Consume());
        }
        if (buf.Count > 0) items.Add(TokensToSql(buf));
        return items;
    }

    private string ReadClauseTextUntilTopLevelStop(params string[] stopWords)
    {
        var buf = new List<SqlToken>();
        int depth = 0;
        while (!IsEnd(Peek()))
        {
            var t = Peek();
            if (IsSymbol(t, "(")) depth++;
            else if (IsSymbol(t, ")")) depth--;
            if (depth == 0 && IsSymbol(t, ";")) break;
            if (depth == 0 && ShouldStopAtTopLevelToken(t, stopWords, buf)) break;
            buf.Add(Consume());
        }
        return TokensToSql(buf);
    }

    /// <summary>
    /// EN: Determines whether current top-level token should stop clause/item scanning, preserving ordered-set syntax boundaries.
    /// PT: Determina se o token atual em nível de topo deve encerrar a varredura da cláusula/item, preservando fronteiras de sintaxe ordered-set.
    /// </summary>
    /// <param name="current">EN: Token currently inspected at top level. PT: Token inspecionado no nível de topo.</param>
    /// <param name="stopWords">EN: Candidate clause stop words. PT: Palavras de parada candidatas de cláusula.</param>
    /// <param name="buffer">EN: Tokens already buffered for current segment. PT: Tokens já acumulados para o segmento atual.</param>
    /// <returns>EN: True when parser should stop before current token. PT: True quando o parser deve parar antes do token atual.</returns>
    private static bool ShouldStopAtTopLevelToken(SqlToken current, IReadOnlyList<string> stopWords, IReadOnlyList<SqlToken> buffer)
    {
        if (!stopWords.Any(sw => IsWord(current, sw)))
            return false;

        // Keep shared sequence syntax like NEXT VALUE FOR / PREVIOUS VALUE FOR
        // inside the same SELECT expression.
        if (IsWord(current, SqlConst.FOR) && EndsWithWords(buffer, SqlConst.NEXT, SqlConst.VALUE))
            return false;

        if (IsWord(current, SqlConst.FOR) && EndsWithWords(buffer, SqlConst.PREVIOUS, SqlConst.VALUE))
            return false;

        // Keep "WITHIN GROUP (...)" inside the same SELECT expression.
        if (IsWord(current, SqlConst.GROUP) && EndsWithWord(buffer, SqlConst.WITHIN))
            return false;

        return true;
    }

    /// <summary>
    /// EN: Checks whether buffered tokens end with a specific keyword/identifier word.
    /// PT: Verifica se os tokens acumulados terminam com uma palavra-chave/identificador específica.
    /// </summary>
    /// <param name="buffer">EN: Buffered tokens. PT: Tokens acumulados.</param>
    /// <param name="word">EN: Word to match at buffer tail. PT: Palavra para comparar no final do buffer.</param>
    /// <returns>EN: True when tail token matches the expected word. PT: True quando o token final corresponde Ã  palavra esperada.</returns>
    private static bool EndsWithWord(IReadOnlyList<SqlToken> buffer, string word)
    {
        if (buffer.Count == 0)
            return false;

        var tail = buffer[^1];
        return IsWord(tail, word);
    }

    private static bool EndsWithWords(IReadOnlyList<SqlToken> buffer, params string[] words)
    {
        if (buffer.Count < words.Length)
            return false;

        for (var index = 0; index < words.Length; index++)
        {
            if (!IsWord(buffer[buffer.Count - words.Length + index], words[index]))
                return false;
        }

        return true;
    }

    private string TokensToSql(List<SqlToken> toks)
    {
        // Reconstrói SQL "bom o bastante" para reparse, sem inserir espaços que mudem a semântica.
        // Regra de ouro: não colocar espaços ao redor de '.', parênteses e vírgulas, senão "u.id" vira "u . id"
        // e o splitter de alias pode achar que "id" é alias.
        var sb = new StringBuilder();

        SqlToken? prev = null;

        foreach (var t in toks)
        {
            var text = t.Kind switch
            {
                SqlTokenKind.String => $"'{EscapeStringLiteral(t.Text)}'", // tokenizer entrega sem aspas
                SqlTokenKind.Identifier => NeedsIdentifierQuoting(t.Text) ? QuoteIdentifier(t.Text) : t.Text,
                _ => t.Text
            };

            if (sb.Length > 0 && NeedsSpace(prev, t))
                sb.Append(' ');

            sb.Append(text);
            prev = t;
        }

        return sb.ToString().Trim();

        string EscapeStringLiteral(string value)
        {
            if (_dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash)
            {
                return value
                    .Replace("\\", "\\\\")
                    .Replace("'", "\\'");
            }

            return value.Replace("'", "''");
        }

        bool NeedsIdentifierQuoting(string ident)
        {
            if (string.IsNullOrWhiteSpace(ident))
                return true;

            if (_dialect.IsKeyword(ident))
                return true;

            // Keep quoted when identifier cannot be represented as a bare token.
            // This preserves names originally written with quotes, e.g. `idx``quoted`.
            if (!Regex.IsMatch(ident, @"^[A-Za-z_#][A-Za-z0-9_$#]*$", RegexOptions.CultureInvariant))
                return true;

            return ident.Contains(' ')
                   || ident.Contains('\t')
                   || ident.Contains('\n')
                   || ident.Contains('\r');
        }

        string QuoteIdentifier(string ident)
        {
            // Prefer a quote style that is valid for identifiers in this dialect and won't
            // be interpreted as a string literal by the tokenizer.
            var style = _dialect.IdentifierEscapeStyle;

            if (style == SqlIdentifierEscapeStyle.double_quote && _dialect.IsStringQuote('"'))
            {
                if (_dialect.AllowsBacktickIdentifiers)
                    style = SqlIdentifierEscapeStyle.backtick;
                else if (_dialect.AllowsBracketIdentifiers)
                    style = SqlIdentifierEscapeStyle.bracket;
            }

            return style switch
            {
                SqlIdentifierEscapeStyle.backtick => $"`{ident.Replace("`", "``")}`",
                SqlIdentifierEscapeStyle.double_quote => $"\"{ident.Replace("\"", "\"\"")}\"",
                SqlIdentifierEscapeStyle.bracket => $"[{ident.Replace("]", "]]")}]",
                _ => ident
            };
        }

        static bool IsWordLike(SqlToken tok)
            => tok.Kind is SqlTokenKind.Identifier
            or SqlTokenKind.Keyword
            or SqlTokenKind.Number
            or SqlTokenKind.Parameter
            or SqlTokenKind.String;

        static bool NeedsSpace(SqlToken? p, SqlToken c)
        {
            if (p is null) return false;

            // Spacing rules around punctuation.
            // Never put spaces BEFORE these symbols.
            if (c.Kind == SqlTokenKind.Symbol && (c.Text is "." or ")" or "," or ";")) return false;

            // Never put spaces AFTER these symbols.
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is "." or "(")) return false;

            // Usually keep a space AFTER ")" and "," when followed by a word-like token (") AS", ", col").
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is ")" or ","))
                return IsWordLike(c) || c.Kind == SqlTokenKind.Number || c.Kind == SqlTokenKind.String;

            // Semicolon is statement terminator: no space needed.
            if (p.Value.Kind == SqlTokenKind.Symbol && p.Value.Text == ";") return false;

            // Keep tight for dot/open-paren handled above.
            // Function calls: COUNT( ... ) => no space before '('
            if (c.Kind == SqlTokenKind.Symbol && c.Text == "(") return false;

            // Default: separate word-like tokens to avoid "SELECTu" type merges
            if (IsWordLike(p.Value) && IsWordLike(c)) return true;

            // Keep a space between operator-like tokens and words/numbers where appropriate
            if ((p.Value.Kind == SqlTokenKind.Operator && c.Kind != SqlTokenKind.Symbol) ||
                (c.Kind == SqlTokenKind.Operator && p.Value.Kind != SqlTokenKind.Symbol))
                return true;

            // Conservative default
            return true;
        }
    }

    private static (string Expr, string? Alias) SplitTrailingAsAliasTopLevel(string raw, ISqlDialect dialect)
    {
        raw = raw.Trim();
        if (raw.Length == 0)
            return (raw, null);

        var options = new AliasSplitOptions(
            dialect.IsStringQuote('"'),
            dialect.AllowsBacktickIdentifiers,
            dialect.AllowsDoubleQuoteIdentifiers && !dialect.IsStringQuote('"'),
            dialect.AllowsBracketIdentifiers);

        var explicitAlias = TrySplitExplicitAliasTopLevel(raw, dialect, options);
        if (explicitAlias is not null)
            return explicitAlias.Value;

        var implicitAlias = TrySplitImplicitAliasTopLevel(raw, dialect, options);
        if (implicitAlias is not null)
            return implicitAlias.Value;

        return (raw, null);
    }

    private static (string Expr, string Alias)? TrySplitExplicitAliasTopLevel(
        string raw,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var state = new AliasForwardScanState();
        for (int i = 0; i + 4 <= raw.Length; i++)
        {
            if (TryConsumeAliasForwardQuotedChar(raw, ref i, ref state))
                continue;

            var ch = raw[i];
            if (ch == '(')
            {
                state.Depth++;
                continue;
            }

            if (ch == ')')
            {
                state.Depth = Math.Max(0, state.Depth - 1);
                continue;
            }

            if (state.Depth != 0)
                continue;

            if (TryBeginAliasForwardQuotedChar(ch, options, ref state))
                continue;

            if (!IsExplicitAliasKeyword(raw, i))
                continue;

            var expr = raw[..i].Trim();
            var aliasRaw = raw[(i + 2)..].Trim();
            if (aliasRaw.Length == 0)
                return null;

            return (expr, NormalizeAlias(aliasRaw, dialect, options));
        }

        return null;
    }

    private static (string Expr, string Alias)? TrySplitImplicitAliasTopLevel(
        string raw,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var state = new AliasBackwardScanState();
        for (int i = raw.Length - 1; i >= 0; i--)
        {
            if (TryConsumeAliasBackwardQuotedChar(raw[i], ref state))
                continue;

            var ch = raw[i];
            if (ch == ')')
            {
                state.Depth++;
                continue;
            }

            if (ch == '(')
            {
                state.Depth = Math.Max(0, state.Depth - 1);
                continue;
            }

            if (state.Depth != 0)
                continue;

            if (TryBeginAliasBackwardQuotedChar(ch, options, ref state))
                continue;

            if (!char.IsWhiteSpace(ch))
                continue;

            var split = TryCreateImplicitAliasSplit(raw, i, dialect, options);
            if (split is not null)
                return split;
        }

        return null;
    }

    private static (string Expr, string Alias)? TryCreateImplicitAliasSplit(
        string raw,
        int separatorIndex,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var left = raw[..separatorIndex].TrimEnd();
        var right = raw[(separatorIndex + 1)..].TrimStart();
        if (left.Length == 0 || right.Length == 0)
            return null;

        ThrowIfUnsupportedAliasQuote(right, dialect, options);

        var lastLeft = left.TrimEnd();
        if (Regex.IsMatch(lastLeft, @"(<=>|<>|!=|>=|<=|=|>|<|\+|-|\*|/|,)\s*$", RegexOptions.CultureInvariant))
            return null;
        if (Regex.IsMatch(lastLeft, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            return null;

        var compositeTemporalIdentifier = $"{lastLeft} {right}";
        if (dialect.TemporalFunctionIdentifierNames.Any(name => name.Equals(compositeTemporalIdentifier, StringComparison.OrdinalIgnoreCase)))
            return null;

        if (!LooksLikeAliasToken(right, options))
            return null;

        var alias = NormalizeAlias(right, dialect, options);
        if (dialect.IsKeyword(alias))
            return null;

        return (left, alias);
    }

    private static void ThrowIfUnsupportedAliasQuote(string aliasRaw, ISqlDialect dialect, AliasSplitOptions options)
    {
        if ((aliasRaw[0] == '`' && !options.AllowBacktick)
            || (aliasRaw[0] == '[' && !options.AllowBracket)
            || (aliasRaw[0] == '"' && !options.AllowDqIdent && !options.DqIsString))
        {
            throw SqlUnsupported.ForDialect(dialect, $"Identificador/alias quoting: '{aliasRaw[0]}'");
        }
    }

    private static bool IsExplicitAliasKeyword(string raw, int index)
    {
        if (!(raw[index] is 'A' or 'a') || !(raw[index + 1] is 'S' or 's'))
            return false;

        var leftOk = index == 0 || char.IsWhiteSpace(raw[index - 1]);
        var rightOk = index + 2 < raw.Length && char.IsWhiteSpace(raw[index + 2]);
        return leftOk && rightOk;
    }

    private static bool TryConsumeAliasForwardQuotedChar(string raw, ref int index, ref AliasForwardScanState state)
    {
        var ch = raw[index];
        if (state.InSingle)
        {
            if (ch == '\'' && index + 1 < raw.Length && raw[index + 1] == '\'')
                index++;
            else if (ch == '\'')
                state.InSingle = false;

            return true;
        }

        if (state.InDoubleString)
        {
            if (ch == '"')
                state.InDoubleString = false;

            return true;
        }

        if (state.InDoubleIdent)
        {
            if (ch == '"')
                state.InDoubleIdent = false;

            return true;
        }

        if (state.InBacktick)
        {
            if (ch == '`')
                state.InBacktick = false;

            return true;
        }

        if (!state.InBracket)
            return false;

        if (ch == ']')
        {
            if (index + 1 < raw.Length && raw[index + 1] == ']')
                index++;
            else
                state.InBracket = false;
        }

        return true;
    }

    private static bool TryBeginAliasForwardQuotedChar(char ch, AliasSplitOptions options, ref AliasForwardScanState state)
    {
        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DqIsString)
                state.InDoubleString = true;
            else if (options.AllowDqIdent)
                state.InDoubleIdent = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktick)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == '[' && options.AllowBracket)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool TryConsumeAliasBackwardQuotedChar(char ch, ref AliasBackwardScanState state)
    {
        if (state.InSingle)
        {
            if (ch == '\'')
                state.InSingle = false;

            return true;
        }

        if (state.InDoubleString)
        {
            if (ch == '"')
                state.InDoubleString = false;

            return true;
        }

        if (state.InDoubleIdent)
        {
            if (ch == '"')
                state.InDoubleIdent = false;

            return true;
        }

        if (state.InBacktick)
        {
            if (ch == '`')
                state.InBacktick = false;

            return true;
        }

        if (!state.InBracket)
            return false;

        if (ch == '[')
            state.InBracket = false;

        return true;
    }

    private static bool TryBeginAliasBackwardQuotedChar(char ch, AliasSplitOptions options, ref AliasBackwardScanState state)
    {
        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DqIsString)
                state.InDoubleString = true;
            else if (options.AllowDqIdent)
                state.InDoubleIdent = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktick)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == ']' && options.AllowBracket)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool LooksLikeAliasToken(string rawRight, AliasSplitOptions options)
    {
        rawRight = rawRight.Trim();
        if (rawRight.Length == 0)
            return false;

        if (rawRight[0] == '`')
            return options.AllowBacktick && rawRight.Length >= 2 && rawRight[^1] == '`';

        if (rawRight[0] == '"')
            return options.AllowDqIdent && rawRight.Length >= 2 && rawRight[^1] == '"';

        if (rawRight[0] == '[')
            return options.AllowBracket && rawRight.Length >= 2 && rawRight[^1] == ']';

        for (var i = 0; i < rawRight.Length; i++)
        {
            if (char.IsWhiteSpace(rawRight[i]))
                return false;
        }

        var first = rawRight[0];
        if (!(char.IsLetter(first) || first == '_'))
            return false;

        for (var i = 1; i < rawRight.Length; i++)
        {
            var ch = rawRight[i];
            if (!(char.IsLetterOrDigit(ch) || ch == '_' || ch == '$'))
                return false;
        }

        return true;
    }

    private static string NormalizeAlias(
        string aliasRaw,
        ISqlDialect dialect,
        bool dqIsString,
        bool allowBacktick,
        bool allowDqIdent,
        bool allowBracket)
    {
        aliasRaw = aliasRaw.Trim();

        // If alias is quoted in a way the dialect doesn't allow, fail fast.
        if (aliasRaw.StartsWith("`") && !allowBacktick)
            throw SqlUnsupported.ForDialect(dialect, "alias/identificadores com '`'");

        if (aliasRaw.StartsWith("[") && !allowBracket)
            throw SqlUnsupported.ForDialect(dialect, "alias/identificadores com '['");

        if (aliasRaw.StartsWith("\"") && !allowDqIdent && !dqIsString)
            throw SqlUnsupported.ForDialect(dialect, "alias/identificadores com '\"'");

        // Strip outer identifier quotes (only those permitted for identifiers).
        if (allowBacktick && aliasRaw.Length >= 2 && aliasRaw[0] == '`' && aliasRaw[^1] == '`')
        {
            // MySQL/SQLite identifier escape: `` => `
            var inner = aliasRaw[1..^1].Replace("``", "`");
            return inner;
        }

        if (allowDqIdent && aliasRaw.Length >= 2 && aliasRaw[0] == '"' && aliasRaw[^1] == '"')
        {
            // SQL standard escape: "" => "
            var inner = aliasRaw[1..^1].Replace("\"\"", "\"");
            return inner;
        }

        if (allowBracket && aliasRaw.Length >= 2 && aliasRaw[0] == '[' && aliasRaw[^1] == ']')
        {
            // SQL Server escape: ]] => ]
            var inner = aliasRaw[1..^1].Replace("]]", "]");
            return inner;
        }

        return aliasRaw;
    }

    private static string NormalizeAlias(string aliasRaw, ISqlDialect dialect, AliasSplitOptions options)
        => NormalizeAlias(
            aliasRaw,
            dialect,
            options.DqIsString,
            options.AllowBacktick,
            options.AllowDqIdent,
            options.AllowBracket);

    private readonly record struct AliasSplitOptions(
        bool DqIsString,
        bool AllowBacktick,
        bool AllowDqIdent,
        bool AllowBracket);

    private struct AliasForwardScanState
    {
        public int Depth;
        public bool InSingle;
        public bool InDoubleString;
        public bool InDoubleIdent;
        public bool InBacktick;
        public bool InBracket;
    }

    private struct AliasBackwardScanState
    {
        public int Depth;
        public bool InSingle;
        public bool InDoubleString;
        public bool InDoubleIdent;
        public bool InBacktick;
        public bool InBracket;
    }


    // Plumbing Básico
    private SqlToken Peek(int offset = 0) => (_i + offset < _toks.Count) ? _toks[_i + offset] : SqlToken.EOF;
    private SqlToken Consume() => _toks[_i++];
    private static bool IsEnd(SqlToken t) => t.Kind == SqlTokenKind.EndOfFile;
    private static bool IsWord(SqlToken t, string w) => t.Text.Equals(w, StringComparison.OrdinalIgnoreCase);
    private static bool IsSymbol(SqlToken t, string s) => t.Kind == SqlTokenKind.Symbol && t.Text == s;
    private void ExpectWord(string w)
    {
        if (!IsWord(Peek(), w)) throw new InvalidOperationException($"Esperava {w}, veio {Peek().Text}");
        Consume();
    }
    private void ExpectSymbol(string s)
    {
        // Alguns símbolos vêm tokenizados como Operator, e o Kind pode variar.
        // O que importa pra validar estrutura é o texto literal do token.
        var t = Peek();
        if (!string.Equals(t.Text, s, StringComparison.Ordinal))
            throw new InvalidOperationException($"Esperava símbolo {s}, veio {t.Text}");
        Consume();
    }
    private string ExpectIdentifier()
    {
        var t = Consume();
        if (t.Kind == SqlTokenKind.Identifier || t.Kind == SqlTokenKind.Keyword) return t.Text;
        throw new InvalidOperationException($"Esperava identifier, veio {t.Kind}");
    }

    private long ExpectSignedNumberLong(string clauseName)
    {
        var sign = 1L;
        if (IsSymbol(Peek(), "+"))
        {
            Consume();
        }
        else if (IsSymbol(Peek(), "-"))
        {
            Consume();
            sign = -1L;
        }

        var t = Consume();
        if (t.Kind == SqlTokenKind.Number)
            return sign * long.Parse(t.Text, CultureInfo.InvariantCulture);

        if (t.Kind == SqlTokenKind.Parameter)
            return sign * ResolveParameterLong(t.Text);

        throw new InvalidOperationException($"{clauseName} requires an integer literal or parameter.");
    }

    private int ResolveParameterInt(string parameterToken)
    {
        if (_parameters is null)
            throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

        var normalized = parameterToken.TrimStart('@', ':', '?');

        foreach (IDataParameter parameter in _parameters)
        {
            var name = (parameter.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            if (parameter.Value is null || parameter.Value == DBNull.Value)
                throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

            return Convert.ToInt32(parameter.Value, CultureInfo.InvariantCulture);
        }

        throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");
    }

    private long ResolveParameterLong(string parameterToken)
    {
        if (_parameters is null)
            throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

        var normalized = parameterToken.TrimStart('@', ':', '?');

        foreach (IDataParameter parameter in _parameters)
        {
            var name = (parameter.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            if (parameter.Value is null || parameter.Value == DBNull.Value)
                throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");

            return Convert.ToInt64(parameter.Value, CultureInfo.InvariantCulture);
        }

        throw new FormatException($"The input string '{parameterToken}' was not in a correct format.");
    }
    private static string NormalizeClauseText(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return string.Empty;

        var txt = raw!.Trim();
        if (txt.EndsWith(";", StringComparison.Ordinal))
            txt = txt[..^1].TrimEnd();

        return txt;
    }

    private void EnsureStatementEnd(string statementName)
    {
        if (IsSymbol(Peek(), ";"))
            Consume();

        if (!IsEnd(Peek()))
        {
            var t = Peek();
            throw new InvalidOperationException($"Unexpected token after {statementName}: {t.Kind} '{t.Text}'");
        }
    }

    private void ExpectEndOrUnionBoundary()
    {
        // Após um SELECT completo, só é válido terminar o statement ou seguir com UNION.
        // No MySQL, quando SELECT está dentro de INSERT ... SELECT, pode haver ON DUPLICATE KEY UPDATE depois.
        var t = Peek();
        if (IsEnd(t) || IsWord(t, SqlConst.UNION)) return;

        // boundary especial: INSERT ... SELECT ... ON DUPLICATE / ON CONFLICT / RETURNING
        if (_allowInsertSelectSuffixBoundary && (IsWord(t, SqlConst.ON) || IsWord(t, SqlConst.RETURNING))) return;

        // tolera ';' final se o split top-level não removeu
        if (IsSymbol(t, ";")) { Consume(); return; }

        throw new InvalidOperationException($"Token inesperado após SELECT: {t.Kind} '{t.Text}'");
    }

    private static readonly HashSet<string> JoinStart = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.JOIN, SqlConst.INNER, SqlConst.LEFT, SqlConst.RIGHT, SqlConst.CROSS, SqlConst.OUTER
    };

    private static bool IsJoinStart(SqlToken t)
        => JoinStart.Contains(t.Text);

    private void SkipUntilTopLevelWord(params string[] words)
    {
        if (words is null || words.Length == 0)
            throw new ArgumentException("words vazio", nameof(words));

        var set = new HashSet<string>(words, StringComparer.OrdinalIgnoreCase);

        var depth = 0;
        while (!IsEnd(Peek()))
        {
            var t = Peek();

            if (IsSymbol(t, "(")) { depth++; Consume(); continue; }
            if (IsSymbol(t, ")")) { depth = Math.Max(0, depth - 1); Consume(); continue; }

            if (depth == 0 && t.Kind == SqlTokenKind.Identifier && set.Contains(t.Text))
                return;

            if (depth == 0 && t.Kind == SqlTokenKind.Keyword && set.Contains(t.Text))
                return;

            // Alguns tokenizers colocam palavras como Identifier; outros como Keyword.
            if (depth == 0 && set.Contains(t.Text))
                return;

            Consume();
        }

        throw new InvalidOperationException($"Não encontrei nenhum destes tokens no nível top-level: {string.Join(", ", words)}");
    }

    private static readonly HashSet<string> ClauseKeywordToken = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.FROM   ,
        SqlConst.WHERE  ,
        SqlConst.GROUP  ,
        SqlConst.HAVING ,
        SqlConst.ORDER  ,
        SqlConst.LIMIT  ,
        SqlConst.UNION  ,
        SqlConst.ON     ,
        SqlConst.JOIN   ,
        SqlConst.INNER  ,
        SqlConst.LEFT   ,
        SqlConst.RIGHT  ,
        SqlConst.CROSS  ,
        SqlConst.OUTER  ,
        SqlConst.APPLY  ,
        SqlConst.OFFSET ,
        SqlConst.FETCH  ,
        SqlConst.OPTION ,
        SqlConst.SET    ,  // UPDATE
        SqlConst.VALUES ,  // INSERT
        SqlConst.SELECT ,  // INSERT...SELECT (e derived cases)
        SqlConst.INTO   , // útil em variações/dialetos
        SqlConst.USING  ,
        SqlConst.WHEN   ,
        SqlConst.MATCHED,
        SqlConst.THEN
      , SqlConst.PIVOT
      , SqlConst.UNPIVOT
      , SqlConst.RETURNING
    };

    private static bool IsClauseKeywordToken(SqlToken t, IReadOnlyCollection<string>? additionalStopWords = null)
        => ClauseKeywordToken.Contains(t.Text)
           || (additionalStopWords?.Contains(t.Text) == true);

    // Helpers estáticos de split (mantidos do original)
    internal static List<string> SplitStatementsTopLevel(string sql, ISqlDialect dialect)
    {
        var res = new List<string>();
        if (string.IsNullOrWhiteSpace(sql))
            return res;

        var options = new StatementSplitOptions(
            dialect.SupportsDollarQuotedStrings,
            dialect.StringEscapeStyle,
            dialect.IsStringQuote('"'),
            dialect.AllowsDoubleQuoteIdentifiers,
            dialect.AllowsBacktickIdentifiers,
            dialect.AllowsBracketIdentifiers);
        var start = 0;
        var state = new StatementSplitState();

        for (int i = 0; i < sql.Length; i++)
        {
            if (TryConsumeStatementQuotedChar(sql, options, ref i, ref state))
                continue;

            if (TryBeginStatementQuotedChar(sql, options, ref i, ref state))
                continue;

            var ch = sql[i];
            if (ch == '(')
            {
                state.Depth++;
                continue;
            }

            if (ch == ')')
            {
                state.Depth = Math.Max(0, state.Depth - 1);
                continue;
            }

            if (ch == ';' && state.Depth == 0)
            {
                AddTopLevelStatement(res, sql, start, i);
                start = i + 1;
            }
        }

        AddTopLevelStatement(res, sql, start, sql.Length);
        return res;
    }

    private static void AddTopLevelStatement(List<string> statements, string sql, int start, int endExclusive)
    {
        var stmt = sql[start..endExclusive].Trim();
        if (stmt.Length > 0)
            statements.Add(stmt);
    }

    private static bool TryConsumeStatementQuotedChar(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        if (state.DollarTag is not null)
        {
            if (MatchesDollarTag(sql, index, state.DollarTag))
            {
                index += state.DollarTag.Length - 1;
                state.DollarTag = null;
            }

            return true;
        }

        var ch = sql[index];
        if (state.InSingle)
            return ConsumeStringQuotedChar(sql, options.EscapeStyle, '\'', ref index, ref state.InSingle);

        if (state.InStringDouble)
            return ConsumeStringQuotedChar(sql, options.EscapeStyle, '"', ref index, ref state.InStringDouble);

        if (state.InIdentDouble)
            return ConsumeIdentifierQuotedChar(sql, '"', ref index, ref state.InIdentDouble);

        if (state.InBacktick)
            return ConsumeIdentifierQuotedChar(sql, '`', ref index, ref state.InBacktick);

        if (!state.InBracket)
            return false;

        if (ch == ']')
        {
            if (index + 1 < sql.Length && sql[index + 1] == ']')
                index++;
            else
                state.InBracket = false;
        }

        return true;
    }

    private static bool TryBeginStatementQuotedChar(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        var ch = sql[index];
        if (TryBeginDollarQuotedString(sql, options, ref index, ref state))
            return true;

        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DoubleQuoteIsString)
                state.InStringDouble = true;
            else if (options.AllowDoubleQuoteIdentifiers)
                state.InIdentDouble = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktickIdentifiers)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == '[' && options.AllowBracketIdentifiers)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool TryBeginDollarQuotedString(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        if (!options.SupportsDollarQuotedStrings || sql[index] != '$')
            return false;

        var closingTagIndex = index + 1;
        while (closingTagIndex < sql.Length
            && (char.IsLetterOrDigit(sql[closingTagIndex]) || sql[closingTagIndex] == '_'))
        {
            closingTagIndex++;
        }

        if (closingTagIndex >= sql.Length || sql[closingTagIndex] != '$')
            return false;

        state.DollarTag = sql[index..(closingTagIndex + 1)];
        index = closingTagIndex;
        return true;
    }

    private static bool MatchesDollarTag(string sql, int index, string dollarTag)
        => index + dollarTag.Length <= sql.Length
           && sql.AsSpan(index, dollarTag.Length).SequenceEqual(dollarTag);

    private static bool ConsumeStringQuotedChar(
        string sql,
        SqlStringEscapeStyle escapeStyle,
        char quote,
        ref int index,
        ref bool inQuote)
    {
        var ch = sql[index];
        if (escapeStyle == SqlStringEscapeStyle.doubled_quote
            && ch == quote
            && index + 1 < sql.Length
            && sql[index + 1] == quote)
        {
            index++;
            return true;
        }

        if (escapeStyle == SqlStringEscapeStyle.backslash
            && ch == quote
            && index > 0
            && sql[index - 1] == '\\')
        {
            return true;
        }

        if (ch == quote)
            inQuote = false;

        return true;
    }

    private static bool ConsumeIdentifierQuotedChar(
        string sql,
        char quote,
        ref int index,
        ref bool inQuote)
    {
        if (sql[index] == quote && index + 1 < sql.Length && sql[index + 1] == quote)
        {
            index++;
            return true;
        }

        if (sql[index] == quote)
            inQuote = false;

        return true;
    }

    private readonly record struct StatementSplitOptions(
        bool SupportsDollarQuotedStrings,
        SqlStringEscapeStyle EscapeStyle,
        bool DoubleQuoteIsString,
        bool AllowDoubleQuoteIdentifiers,
        bool AllowBacktickIdentifiers,
        bool AllowBracketIdentifiers);

    private struct StatementSplitState
    {
        public int Depth;
        public bool InSingle;
        public bool InStringDouble;
        public bool InIdentDouble;
        public bool InBacktick;
        public bool InBracket;
        public string? DollarTag;
    }

    private static bool IsTrailingTokenInWherePredicate(InvalidOperationException ex)
        => ex.Message.Contains("fim da expressão", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("end of expression", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("token inesperado no prefix", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("unexpected token in prefix", StringComparison.OrdinalIgnoreCase);
    // Stub para método que verifica subquery escalar (removido para brevidade, adicione se precisar da validação estrita)
    /// <summary>
    /// EN: Parses a SQL fragment as subquery expression and throws when fragment is not a SELECT query.
    /// PT: Faz o parsing de um fragmento SQL como expressão de subquery e lança exceção quando o fragmento não é uma query SELECT.
    /// </summary>
    /// <param name="sql">EN: SQL fragment to parse as subquery. PT: Fragmento SQL para parsear como subquery.</param>
    /// <param name="t">EN: Current token used for contextual error composition. PT: Token atual usado para composição contextual de erro.</param>
    /// <param name="ctx">EN: Context label appended to validation error messages. PT: Rótulo de contexto anexado Ã s mensagens de erro de validação.</param>
    /// <param name="dialect">EN: Dialect used for parsing. PT: Dialeto usado no parsing.</param>
    /// <returns>EN: Parsed subquery expression node. PT: Nó de expressão de subquery parseado.</returns>
    public static SubqueryExpr ParseSubqueryExprOrThrow(
        string sql,
        SqlToken t,
        string ctx,
        ISqlDialect dialect)
    {
        var q = Parse(sql, dialect);
        if (q is SqlSelectQuery sq) return new SubqueryExpr(sql, sq);
        throw new InvalidOperationException("Subquery deve ser SELECT " + ctx + " | " + t.Text);
    }
}
