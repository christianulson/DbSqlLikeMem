namespace DbSqlLikeMem;

internal sealed class SqlQueryParser
{
    private readonly SqlQueryParserContext _ctx;
    // INSERT ... SELECT pode ter um sufixo de UPSERT após o SELECT (MySQL ON DUPLICATE..., Postgres ON CONFLICT ...)
    private bool _allowInsertSelectSuffixBoundary
    {
        get => _ctx.AllowInsertSelectSuffixBoundary;
        set => _ctx.AllowInsertSelectSuffixBoundary = value;
    }

    private ISqlDialect _dialect => _ctx.Dialect;
    private IDataParameterCollection? _parameters => _ctx.Parameters;
    private Func<string, bool>? _customFunctionSupported => _ctx.CustomFunctionSupported;
    private AutoSqlSyntaxFeatures _autoSyntaxFeatures => _ctx.AutoSyntaxFeatures;

    private static readonly SqlQueryAstCache _astCache = SqlQueryAstCache.CreateFromEnvironment();
    private static readonly SqlQueryParsePreludeCache _preludeCache = SqlQueryParsePreludeCache.CreateFromEnvironment();


    /// <summary>
    /// EN: Creates a parser for the provided SQL text and dialect without command parameters.
    /// PT: Cria um parser para o SQL informado e o dialeto informado sem parametros de comando.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    public SqlQueryParser(
        string sql,
        DbMock db,
        ISqlDialect dialect)
        : this(sql, db, dialect, null)
    {
    }

    /// <summary>
    /// EN: Creates a parser for the provided SQL text, dialect, and command parameters.
    /// PT: Cria um parser para o SQL informado, o dialeto informado e os parametros de comando.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized numeric values. PT: Parâmetros de comando opcionais usados por caminhos do parser que resolvem valores numéricos parametrizados.</param>
    public SqlQueryParser(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters)
        : this(sql, db, dialect, parameters, null)
    {
    }

    /// <summary>
    /// EN: Creates a parser for the provided SQL text, dialect, parameters, and optional custom function resolver.
    /// PT: Cria um parser para o SQL informado, o dialeto informado, os parametros e um resolvedor opcional de funcoes customizadas.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized numeric values. PT: Parâmetros de comando opcionais usados por caminhos do parser que resolvem valores numéricos parametrizados.</param>
    /// <param name="customFunctionSupported">EN: Optional custom function resolver used to accept schema-defined functions during validation. PT: Resolver opcional de funcoes customizadas usado para aceitar funcoes definidas no schema durante a validacao.</param>
    public SqlQueryParser(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        var prelude = GetPrelude(sql, dialect);
        _ctx = new SqlQueryParserContext(
            prelude.Tokens,
            db,
            dialect,
            parameters,
            customFunctionSupported,
            prelude.AutoSyntaxFeatures,
            innerSql => Parse(innerSql, db, dialect, null, customFunctionSupported),
            expr => SqlExpressionParser.ParseScalar(expr, db, dialect, parameters, customFunctionSupported),
            txt => SqlExpressionParser.ParseWhere(txt, db, dialect, parameters, customFunctionSupported));
    }

    private SqlQueryParser(
        IReadOnlyList<SqlToken> toks,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        AutoSqlSyntaxFeatures autoSyntaxFeatures,
        Func<string, bool>? customFunctionSupported = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(toks, nameof(toks));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ctx = new SqlQueryParserContext(
            toks,
            db, dialect,
            parameters,
            customFunctionSupported,
            autoSyntaxFeatures,
            innerSql => Parse(innerSql, db, dialect, null, customFunctionSupported),
            expr => SqlExpressionParser.ParseScalar(expr, db, dialect, parameters, customFunctionSupported),
            txt => SqlExpressionParser.ParseWhere(txt, db, dialect, parameters, customFunctionSupported));
    }

    /// <summary>
    /// EN: Parses one SQL statement into an AST root using default parser options and no parameter collection.
    /// PT: Faz o parsing de um statement SQL para a raiz da AST usando opções padrão do parser e sem coleção de parâmetros.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="db">EN: Database. PT: Banco.</param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase Parse(
        string sql,
        DbMock db,
        ISqlDialect dialect)
        => Parse(sql, db, dialect, null);

    /// <summary>
    /// EN: Parses one SQL statement into an AST root using dialect capabilities and optional command parameters.
    /// PT: Faz o parsing de um statement SQL para a raiz da AST usando capacidades do dialeto e parâmetros de comando opcionais.
    /// </summary>
    /// <param name="sql">EN: SQL text to parse. PT: Texto SQL para parsear.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect that controls tokenizer/parser behavior and feature gates. PT: Dialeto que controla o comportamento do tokenizer/parser e os gates de recursos.</param>
    /// <param name="parameters">EN: Optional command parameters used by parser paths that resolve parameterized numeric values. PT: Parâmetros de comando opcionais usados por caminhos do parser que resolvem valores numéricos parametrizados.</param>
    /// <param name="customFunctionSupported">EN: Optional custom function resolver used to accept schema-defined functions during validation. PT: Resolver opcional de funcoes customizadas usado para aceitar funcoes definidas no schema durante a validacao.</param>
    /// <returns>EN: Parsed query AST root. PT: Raiz da AST da query parseada.</returns>
    public static SqlQueryBase Parse(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        Func<string, bool>? customFunctionSupported = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
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
                throw SqlUnsupported.NotSupportedMerge(dialect);

            if (customFunctionSupported is not null)
            {
                var parsedWithoutCache = ParseUncached(preludeTokens, db, dialect, parameters, autoSyntaxFeatures, customFunctionSupported);
                EnsureDialectSupport(parsedWithoutCache, dialect);
                return parsedWithoutCache with { RawSql = sql };
            }

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
                var uncached = ParseUncached(preludeTokens, db, dialect, null, autoSyntaxFeatures);
                EnsureDialectSupport(uncached, dialect);
                return uncached with { RawSql = sql };
            }

            var parsed = ParseUncached(preludeTokens, db, dialect, parameters, autoSyntaxFeatures);
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
        var autoSyntaxFeatures = AutoDialectFactory.IsAutoDialect(dialect)
            ? AutoDialectFactory.DetectSyntax(sql, tokens)
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
                throw SqlUnsupported.NotSupportedMerge(dialect);
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
            throw SqlUnsupported.NotSupported(dialect, SqlConst.WITH_CTE);

        EnsureRowLimitDialectSupport(select.RowLimit, dialect);

        if (select.ForJson is not null && !dialect.SupportsForJsonClause)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.FOR_JSON);

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
                    throw SqlUnsupported.NotSupportedPagination(dialect, SqlConst.OFFSET_FETCH);
                return;
            }

            if (!dialect.SupportsFetchFirst)
                throw SqlUnsupported.NotSupportedPagination(dialect, SqlConst.FETCH_FIRST_NEXT);
        }
    }

    private static SqlQueryBase ParseUncached(
        IReadOnlyList<SqlToken> tokens,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters,
        AutoSqlSyntaxFeatures autoSyntaxFeatures,
        Func<string, bool>? customFunctionSupported = null)
    {
        var q = new SqlQueryParser(tokens, db, dialect, parameters, autoSyntaxFeatures, customFunctionSupported);
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
                throw SqlUnsupported.NotSupportedMerge(dialect);

            result = q.ParseMerge();
        }
        else
            throw SqlUnsupported.NotSupportedUnknownTopLevelStatement(dialect, first.Text);

        return result;
    }

    /// <summary>
    /// EN: Parses a SQL batch and yields AST roots for each top-level statement using default parser options.
    /// PT: Faz o parsing de um lote SQL e retorna raízes de AST para cada statement top-level usando opções padrão do parser.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect used to split and parse each statement. PT: Dialeto usado para separar e parsear cada statement.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequência de raízes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMulti(
        string sql,
        DbMock db,
        ISqlDialect dialect)
        => ParseMulti(sql, db, dialect, null);

    /// <summary>
    /// EN: Parses a SQL batch and yields AST roots for each top-level statement split by semicolon boundaries.
    /// PT: Faz o parsing de um lote SQL e retorna raízes de AST para cada statement top-level separado por fronteiras de ponto e vírgula.
    /// </summary>
    /// <param name="sql">EN: SQL batch text. PT: Texto SQL em lote.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect used to split and parse each statement. PT: Dialeto usado para separar e parsear cada statement.</param>
    /// <param name="parameters">EN: Optional parameters forwarded to each statement parse. PT: Parâmetros opcionais repassados para o parse de cada statement.</param>
    /// <returns>EN: Sequence of parsed AST roots. PT: Sequência de raízes de AST parseadas.</returns>
    public static IEnumerable<SqlQueryBase> ParseMulti(
        string sql,
        DbMock db,
        ISqlDialect dialect,
        IDataParameterCollection? parameters)
    {
        // O split top-level ainda é útil para separar statements por ';'
        foreach (var s in SqlStatementSplitter.SplitStatementsTopLevel(sql, dialect))
        {
            if (string.IsNullOrWhiteSpace(s)) continue;
            yield return Parse(s, db, dialect, parameters);
        }
    }

    internal static void ClearAstCache()
        => _astCache.Clear();

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
        => SqlStatementSplitter.SplitStatementsTopLevel(sql, dialect);

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
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect used for parsing. PT: Dialeto usado no parsing.</param>
    /// <returns>EN: Normalized UNION chain representation. PT: Representação normalizada de cadeia UNION.</returns>
    public static UnionChain ParseUnionChain(
        string sql,
        DbMock db,
        ISqlDialect dialect)
    {
        var parsed = Parse(sql, db, dialect);
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
    /// <param name="db"></param>
    /// <returns>EN: Normalized UNION chain representation. PT: Representacao normalizada de cadeia UNION.</returns>
    public static UnionChain ParseUnionChainAuto(string sql, DbMock db)
        => ParseUnionChain(sql, db, AutoDialectFactory.Create());

    // ------------------------------------------------------------
    // NOVAS IMPLEMENTAÃ‡Ã•ES DE INSERT / UPDATE / DELETE VIA TOKENS
    // ------------------------------------------------------------

    private SqlInsertQuery ParseInsert()
        => ParseInsertLike(false);

    private SqlInsertQuery ParseReplace()
        => ParseInsertLike(true);

    private SqlInsertQuery ParseInsertLike(bool isReplace)
    {
        _ctx.Consume(); // INSERT / REPLACE
        var insertIgnore = ConsumeOptionalInsertModifiers(isReplace);
        if (_ctx.IsWord(SqlConst.INTO)) _ctx.Consume();

        var table = ParseTableSource(
            consumeHints: false,
            allowFunctionSource: false,
            aliasStopWords: [SqlConst.VALUE, SqlConst.PARTITION]); // Tabela

        var partitionNames = _ctx.ParseOptionalPartitionClause();
        if (partitionNames.Count == 0 && table.PartitionNames is { Count: > 0 } tablePartitionNames)
        {
            partitionNames = tablePartitionNames;
            table = table with { PartitionNames = [] };
        }
        else if (table.PartitionNames is { Count: > 0 })
        {
            table = table with { PartitionNames = [] };
        }

        // REPLACE ... SET col1 = expr, col2 = expr
        var valuesRaw = new List<List<string>>();
        var valuesExpr = new List<List<SqlExpr?>>();
        List<string> cols;
        bool hasExplicitColumnList;
        SqlSelectQuery? insertSelect = null;
        if (_ctx.IsWord(SqlConst.SET))
        {
            _ctx.Consume(); // SET
            var assignments = ParseReplaceSetAssignments();
            cols = [.. assignments.Select(a => a.Column)];
            valuesRaw.Add([.. assignments.Select(a => a.ValueRaw)]);
            valuesExpr.Add([.. assignments.Select(a => a.ValueExpr)]);
            hasExplicitColumnList = false;
        }
        else
        {
            // Colunas opcionais: (col1, col2)
            hasExplicitColumnList = _ctx.IsSymbol("(");
            cols = ParseCols();
        }

        // VALUES / VALUE ou SELECT?
        if (valuesRaw.Count == 0 && (_ctx.IsWord(SqlConst.VALUES) || _ctx.IsWord(SqlConst.VALUE)))
        {
            _ctx.Consume(); // VALUES / VALUE
            _ctx.ParseInsertValuesRows(valuesRaw, valuesExpr);
        }
        else if (valuesRaw.Count == 0 && (_ctx.IsWord(SqlConst.SELECT) || _ctx.IsWord(SqlConst.WITH)))
        {
            _allowInsertSelectSuffixBoundary = true;
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
        var onDup = isReplace ? null : _ctx.ParseOnDuplicated();
        var returning = _ctx.ParseOptionalReturningItems(_ctx.Dialect.SupportsInsertReturning);

        _ctx.EnsureStatementEnd(SqlConst.INSERT);

        return new SqlInsertQuery
        {
            Table = table,
            Columns = cols,
            ValuesRaw = valuesRaw,
            ValuesExpr = valuesExpr,
            InsertSelect = insertSelect,
            Returning = returning,
            PartitionNames = partitionNames,
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
            if (_ctx.IsWord(SqlConst.LOW_PRIORITY)
                || _ctx.IsWord(SqlConst.DELAYED)
                || (!isReplace && _ctx.IsWord(SqlConst.HIGH_PRIORITY))
                || (!isReplace && _ctx.IsWord(SqlConst.IGNORE)))
            {
                if (!isReplace && _ctx.IsWord(SqlConst.IGNORE))
                    sawIgnore = true;
                _ctx.Consume();
                continue;
            }

            break;
        }

        return sawIgnore;
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
        if (!_ctx.IsSymbol("("))
            return [];

        var cols = new List<string>();
        _ctx.Consume(); // (

        while (true)
        {
            if (_ctx.IsEnd() || _ctx.IsSymbol(";"))
                throw new InvalidOperationException("INSERT column list was not closed correctly.");

            if (_ctx.IsSymbol(")"))
            {
                _ctx.Consume();
                return cols;
            }

            if (_ctx.IsSymbol(","))
                throw new InvalidOperationException("INSERT column list has an unexpected comma before column.");

            cols.Add(_ctx.ExpectIdentifier());

            if (_ctx.IsSymbol(","))
            {
                _ctx.Consume();

                if (_ctx.IsSymbol(")"))
                    throw new InvalidOperationException("INSERT column list has a trailing comma without column.");

                continue;
            }

            if (_ctx.IsEnd() || _ctx.IsSymbol(";"))
                throw new InvalidOperationException("INSERT column list was not closed correctly.");

            if (!_ctx.IsSymbol(")"))
                throw new InvalidOperationException("INSERT column list must separate columns with commas.");
        }
    }

    private SqlUpdateQuery ParseUpdate()
        => _ctx.ParseUpdate(ParseUpdateAssignmentsList);

    private SqlDeleteQuery ParseDelete()
        => _ctx.ParseDelete();

    private SqlMergeQuery ParseMerge()
    {
        _ctx.Consume(); // MERGE
        if (_ctx.IsWord(SqlConst.INTO)) _ctx.Consume();

        // target table + alias (ex: stats target)
        var target = ParseTableSource(allowFunctionSource: false);

        if (!_ctx.HasTopLevelWordInRemaining(SqlConst.USING))
            throw new InvalidOperationException("MERGE requer cláusula USING. Ex.: MERGE INTO <target> USING <source> ON ...");

        if (!_ctx.HasTopLevelWordInRemaining(SqlConst.ON))
            throw new InvalidOperationException("MERGE requer cláusula ON. Ex.: MERGE INTO <target> USING <source> ON <condição>");

        if (!_ctx.HasTopLevelMergeWhenClause())
            throw new InvalidOperationException("MERGE requer ao menos uma cláusula WHEN (MATCHED/NOT MATCHED).");

        // O resto do MERGE é grande demais pra agora.
        // Só avançamos tokens até o fim pra não deixar lixo se você evoluir o parser.
        while (Peek().Kind != SqlTokenKind.EndOfFile)
            _ctx.Consume();

        return new SqlMergeQuery
        {
            Table = target
        };
    }

    // ------------------------------------------------------------
    // SELECT (Lógica já existente, mantida e integrada)
    // ------------------------------------------------------------

    /// <summary>
    /// EN: Implements ParseSelectOrUnionQuery.
    /// PT: Implementa ParseSelectOrUnionQuery.
    /// </summary>
    private SqlQueryBase ParseSelectOrUnionQuery()
        => _ctx.ParseSelectOrUnion(ParseSelectQuery);

    /// <summary>
    /// EN: Parses a SELECT query with optional control over CTE parsing and ORDER BY/pagination tail parsing.
    /// PT: Faz o parsing de uma query SELECT com controle opcional de parsing de CTE e cauda ORDER BY/paginação.
    /// </summary>
    /// <param name="allowCtes">EN: When true, WITH/CTE clauses are parsed before SELECT. PT: Quando verdadeiro, cláusulas WITH/CTE são parseadas antes do SELECT.</param>
    /// <param name="allowOrderByAndLimit">EN: When true, ORDER BY and row-limit tails are parsed. PT: Quando verdadeiro, caudas ORDER BY e limite de linhas são parseadas.</param>
    /// <returns>EN: Parsed SELECT AST node. PT: Nó AST de SELECT parseado.</returns>
    public SqlSelectQuery ParseSelectQuery(bool allowCtes = true, bool allowOrderByAndLimit = true)
    {
        var ctes = allowCtes
            ? SqlCteParserHelper.TryParseCtes(_ctx, _ctx.ParseQuery)
            : [];

        _ctx.ExpectWord(SqlConst.SELECT);
        if (_ctx.IsWord(SqlConst.SELECT))
            throw new InvalidOperationException("invalid: duplicated SELECT keyword");
        var distinct = TryParseDistinct();
        var top = TryParseTop();
        TryParseSelectModifiers();
        var selectItems = SqlSelectItemParserHelper.ParseSelectItemsWithValidation(
            _ctx.ParseCommaSeparatedRawItemsUntilAny(
                SqlConst.FROM,
                SqlConst.WHERE,
                SqlConst.GROUP,
                SqlConst.HAVING,
                SqlConst.ORDER,
                SqlConst.LIMIT,
                SqlConst.OFFSET,
                SqlConst.FETCH,
                SqlConst.UNION,
                SqlConst.FOR),
            _ctx.Db,
            _ctx.Dialect,
            _customFunctionSupported);
        var table = ParseFromOrDual();
        var joins = ParseJoins(table);
        while (_ctx.IsSymbol(","))
        {
            _ctx.Consume();
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
        var orderBy = allowOrderByAndLimit
            ? SqlOrderByHelper.TryParseOrderBy(
                _ctx,
                boundary => boundary
                    ? _ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON)
                    : _ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING))
            : [];
        var rowLimit = allowOrderByAndLimit
            ? SqlPaginationHelper.TryParseRowLimitTail(_ctx, orderBy.Count > 0)
            : null;
        var forJson = allowOrderByAndLimit
            ? SqlForJsonClauseHelper.TryParseForJsonClause(_ctx)
            : null;
        if (allowOrderByAndLimit)
            _ctx.TryConsumeQueryHintOption();
        if (top is not null)
            rowLimit ??= top;

        if (allowOrderByAndLimit)
        {
            _ctx.ExpectEndOrUnionBoundary();
        }
        else
        {
            var t = Peek();
            if (!SqlQueryParserContext.IsEnd(t)
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

        if (AutoDialectFactory.IsAutoDialect(_dialect))
        {
            query = DialectNormalizer.NormalizeAutoSelect(
                query,
                _autoSyntaxFeatures,
                _ctx.ResolveParameterInt);
        }

        return query;
    }

    private void TryParseSelectModifiers()
    {
        while (_ctx.IsWord(SqlConst.SQL_CALC_FOUND_ROWS))
        {
            if (!_dialect.SupportsSqlCalcFoundRowsModifier)
                throw SqlUnsupported.NotSupported(_dialect, "SELECT modifier SQL_CALC_FOUND_ROWS");

            _ctx.Consume();
        }
    }

    // ------------------------------------------------------------
    // Helpers de Token (Generalizados)
    // ------------------------------------------------------------

    private List<SqlAssignment> ParseUpdateAssignmentsList()
        => _ctx.ParseUpdateAssignmentsList();

    private List<SqlAssignment> ParseReplaceSetAssignments()
        => _ctx.ParseReplaceSetAssignments();

    private SqlQueryBase ParseCreate()
    {
        _ctx.ExpectWord(SqlConst.CREATE);

        // CREATE OR REPLACE ...
        var orReplace = false;
        if (_ctx.IsWord(SqlConst.OR))
        {
            _ctx.Consume();
            _ctx.ExpectWord(SqlConst.REPLACE);
            orReplace = true;
        }

        // CREATE VIEW ...
        if (_ctx.IsWord(SqlConst.VIEW))
            return _ctx.ParseCreateView(orReplace);

        var uniqueIndex = false;
        if (_ctx.IsWord(SqlConst.UNIQUE))
        {
            _ctx.Consume();
            uniqueIndex = true;
        }

        if (_ctx.IsWord(SqlConst.INDEX))
            return _ctx.ParseCreateIndex(orReplace, uniqueIndex);

        if (_ctx.IsWord(SqlConst.SEQUENCE))
            return _ctx.ParseCreateSequence(orReplace);

        if (_ctx.IsWord(SqlConst.FUNCTION))
            return _ctx.ParseCreateFunction(orReplace);

        if (_ctx.IsWord(SqlConst.PROCEDURE))
            return _ctx.ParseCreateProcedure(orReplace);

        if (_ctx.IsWord(SqlConst.TRIGGER))
            return _ctx.ParseCreateTrigger(orReplace);

        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW, FUNCTION and PROCEDURE statements.");
        return SqlCreateTemporaryTableHelper.ParseCreateTemporaryTable(_ctx);
    }

    private SqlQueryBase ParseAlter()
        => _ctx.ParseAlter();

    private SqlQueryBase ParseDrop()
        => _ctx.ParseDrop();

    // --- Helpers de SELECT trazidos do arquivo original ---

    private bool TryParseDistinct()
    {
        if (!_ctx.IsWord(SqlConst.DISTINCT)) return false;
        _ctx.Consume();
        if (_ctx.IsWord(SqlConst.DISTINCT))
            throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
        return true;
    }
    private SqlTop? TryParseTop()
    {
        // SQL Server: SELECT TOP (10) ... / SELECT TOP 10 ...
        if (!_ctx.IsWord(SqlConst.TOP)) return null;

        // Se o dialeto não suporta, deixa o SQL cair como erro em validação ou corpo
        if (!_dialect.SupportsTop)
            return null;

        _ctx.Consume(); // TOP

        // TOP pode vir como (N) ou N
        if (_ctx.IsSymbol("("))
        {
            _ctx.Consume();
            var n = _ctx.ExpectRowLimitExpr();
            _ctx.ExpectSymbol(")");
            return new SqlTop(n);
        }

        return new SqlTop(_ctx.ExpectRowLimitExpr());
    }

    private SqlTableSource ParseFromOrDual()
    {
        if (_ctx.IsWord(SqlConst.FROM))
        {
            _ctx.Consume();
            if (_ctx.IsWord(SqlConst.FROM))
                throw new InvalidOperationException("invalid: duplicated FROM keyword");
            var ts = ParseTableSource();
            ts = TryParseTableTransforms(ts);
            if (_ctx.IsWord(SqlConst.FROM))
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
            if (_ctx.IsSymbol(","))
            {
                _ctx.Consume();
                var commaTable = TryParseTableTransforms(ParseTableSource());
                joins.Add(new SqlJoin(
                    commaTable.Derived is not null || commaTable.DerivedUnion is not null || commaTable.TableFunction is not null
                        ? SqlJoinType.CrossApply
                        : SqlJoinType.Cross,
                    commaTable,
                    new LiteralExpr(true)));
                continue;
            }

            if (_ctx.IsJoinStart())
            {
                joins.Add(_ctx.ParseJoin(() => TryParseTableTransforms(ParseTableSource())));
                continue;
            }

            break;
        }
        return joins;
    }

    private SqlExpr? TryParseWhereExpr()
    {
        if (!_ctx.IsWord(SqlConst.WHERE)) return null;
        _ctx.Consume();
        // SqlConst.ON here is important for INSERT ... SELECT ... WHERE ... ON DUPLICATE ...
        var txt = _ctx.ReadClauseTextUntilTopLevelStop(SqlConst.GROUP, SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.HAVING, SqlConst.FOR, SqlConst.ON, SqlConst.RETURNING);
        return _ctx.ParseWhere(txt);
    }

    private List<string> TryParseGroupBy()
    {
        var list = new List<string>();
        if (!_ctx.IsWord(SqlConst.GROUP)) return list;
        _ctx.Consume();
        _ctx.ExpectWord(SqlConst.BY);
        list.AddRange(_ctx.ParseCommaSeparatedRawItemsUntilAny(SqlConst.HAVING, SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON));
        if (list.Count == 0)
            throw new InvalidOperationException("GROUP BY sem expressões.");
        return list;
    }

    private SqlExpr? TryParseHavingExpr()
    {
        if (!_ctx.IsWord(SqlConst.HAVING)) return null;
        _ctx.Consume();
        var txt = _ctx.ReadClauseTextUntilTopLevelStop(SqlConst.ORDER, SqlConst.LIMIT, SqlConst.OFFSET, SqlConst.FETCH, SqlConst.UNION, SqlConst.FOR, SqlConst.RETURNING, SqlConst.ON);
        return _ctx.ParseWhere(txt);
    }

    // --- Helpers de CTE e Table Source ---

    private SqlTableSource ParseTableSource(
        bool consumeHints = true,
        bool allowFunctionSource = true,
        IReadOnlyCollection<string>? aliasStopWords = null)
        => _ctx.ParseTableSource(consumeHints, allowFunctionSource, aliasStopWords);

    private SqlTableSource TryParseTableTransforms(SqlTableSource source)
    {
        source = _ctx.TryParsePivot(source);
        source = _ctx.TryParseUnpivot(source);
        return source;
    }

    // --- Helpers de Token Plumbing ---

    // Plumbing Básico
    private SqlToken Peek(int offset = 0) => _ctx.Peek(offset);
    private static bool IsWord(SqlToken t, string w) => SqlQueryParserContext.IsWord(t, w);
    private static bool IsSymbol(SqlToken t, string s) => SqlQueryParserContext.IsSymbol(t, s);

    // Stub para método que verifica subquery escalar (removido para brevidade, adicione se precisar da validação estrita)
    /// <summary>
    /// EN: Parses a SQL fragment as subquery expression and throws when fragment is not a SELECT query.
    /// PT: Faz o parsing de um fragmento SQL como expressão de subquery e lança exceção quando o fragmento não é uma query SELECT.
    /// </summary>
    /// <param name="sql">EN: SQL fragment to parse as subquery. PT: Fragmento SQL para parsear como subquery.</param>
    /// <param name="t">EN: Current token used for contextual error composition. PT: Token atual usado para composição contextual de erro.</param>
    /// <param name="ctx">EN: Context label appended to validation error messages. PT: Rótulo de contexto anexado Ã s mensagens de erro de validação.</param>
    /// <param name="db"></param>
    /// <param name="dialect">EN: Dialect used for parsing. PT: Dialeto usado no parsing.</param>
    /// <returns>EN: Parsed subquery expression node. PT: Nó de expressão de subquery parseado.</returns>
    public static SubqueryExpr ParseSubqueryExprOrThrow(
        string sql,
        SqlToken t,
        string ctx,
        DbMock db,
        ISqlDialect dialect)
    {
        var q = Parse(sql, db, dialect);
        if (q is SqlSelectQuery sq) return new SubqueryExpr(sql, sq);
        throw new InvalidOperationException("Subquery deve ser SELECT " + ctx + " | " + t.Text);
    }
}
