using System.Text;

namespace DbSqlLikeMem;

internal sealed class SqlQueryParser
{
    private readonly IReadOnlyList<SqlToken> _toks;
    private readonly ISqlDialect _dialect;
    private int _i;
    // INSERT ... SELECT pode ter um sufixo de UPSERT após o SELECT (MySQL ON DUPLICATE..., Postgres ON CONFLICT ...)
    private bool _allowOnDuplicateBoundary;

    public SqlQueryParser(string sql, ISqlDialect dialect)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(dialect);
        // Normaliza espaços básicos se necessário, mas o tokenizer cuida da maior parte
        _dialect = dialect;
        _toks = new SqlTokenizer(sql, _dialect).Tokenize();
        _i = 0;
    }

    public static SqlQueryBase Parse(string sql, ISqlDialect dialect)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var q = new SqlQueryParser(sql, dialect);
        var first = q.Peek();

        SqlQueryBase? result;
        if (IsWord(first, "SELECT") || IsWord(first, "WITH"))
            result = q.ParseSelectQuery();
        else if (IsWord(first, "INSERT"))
            result = q.ParseInsert();
        else if (IsWord(first, "UPDATE"))
            result = q.ParseUpdate();
        else if (IsWord(first, "DELETE"))
            result = q.ParseDelete();
        else if (IsWord(first, "CREATE"))
            result = q.ParseCreate();
        else if (IsWord(first, "MERGE"))
        {
            // Para MySQL, MERGE simplesmente não existe (é sintaxe inválida para o dialeto).
            // Os testes de corpus esperam ThrowInvalid aqui, não NotSupported.
            if (!dialect.SupportsMerge)
                throw new NotSupportedException($"invalid: MERGE statement not supported for dialect {dialect.Name}");

            result = q.ParseMerge();
        }
        else
            throw new InvalidOperationException("SQL não suportado ou parser inválido: " + first.Text);

        // Para estratégias que precisam do SQL original (ex: UPDATE/DELETE ... JOIN (SELECT ...))
        return result with { RawSql = sql };
    }

    public static IEnumerable<SqlQueryBase> ParseMulti(
        string sql,
        ISqlDialect dialect)
    {
        // O split top-level ainda é útil para separar statements por ';'
        foreach (var s in SplitStatementsTopLevel(sql, dialect))
        {
            if (string.IsNullOrWhiteSpace(s)) continue;
            yield return Parse(s, dialect);
        }
    }

    // Mantido para compatibilidade com lógica de Union
    public sealed record UnionChain(
        IReadOnlyList<SqlQueryBase> Parts,
        IReadOnlyList<bool> AllFlags
    );

    public static UnionChain ParseUnionChain(string sql, ISqlDialect dialect)
    {
        var p = new SqlQueryParser(sql, dialect);
        var parts = new List<SqlQueryBase>();
        var allFlags = new List<bool>();

        parts.Add(p.ParseSelectQuery());

        while (IsWord(p.Peek(), "UNION"))
        {
            p.Consume(); // UNION
            var isAll = false;
            if (IsWord(p.Peek(), "ALL"))
            {
                p.Consume();
                isAll = true;
            }
            allFlags.Add(isAll);
            parts.Add(p.ParseSelectQuery());
        }
        return new UnionChain(parts, allFlags);
    }

    // ------------------------------------------------------------
    // NOVAS IMPLEMENTAÇÕES DE INSERT / UPDATE / DELETE VIA TOKENS
    // ------------------------------------------------------------

    private SqlInsertQuery ParseInsert()
    {
        Consume(); // INSERT
        if (IsWord(Peek(), "INTO")) Consume();

        var table = ParseTableSource(); // Tabela

        // Colunas opcionais: (col1, col2)
        var cols = ParseCols();

        // VALUES ou SELECT?
        var valuesRaw = new List<List<string>>();
        var valuesExpr = new List<List<SqlExpr?>>();
        SqlSelectQuery? insertSelect = null;

        if (IsWord(Peek(), "VALUES"))
        {
            Consume(); // VALUES
            while (true)
            {
                if (IsSymbol(Peek(), "("))
                {
                    // Lê raw tokens dentro dos parênteses para ValuesRaw
                    // AST espera List<string> (tokens raw). 
                    // Melhor seria parsear expressões, mas mantendo compatibilidade com teu AST:
                    var rawBlock = ReadBalancedParenRawTokens();
                    // ReadBalanced retorna string "expr, expr", vamos dividir por virgula simplificado ou salvar tudo?
                    // O AST diz "IReadOnlyList<List<string>> ValuesRaw". Assumindo lista de tokens.
                    // Para simplificar, vamos armazenar o conteúdo bruto tokenizado.

                    // Nota: Teu AST pede List<string> que são tokens raw por valor? 
                    // Vou assumir que cada item da lista interna é um valor (ex: "1", "'abc'").
                    var rowValues = SplitRawByComma(rawBlock);
                    valuesRaw.Add(rowValues);
                    valuesExpr.Add([.. rowValues.Select(v => TryParseScalar(v))]);
                }

                if (IsSymbol(Peek(), ","))
                {
                    Consume();
                    continue;
                }
                break;
            }
        }
        else if (IsWord(Peek(), "SELECT") || IsWord(Peek(), "WITH"))
        {
            _allowOnDuplicateBoundary = _dialect.SupportsOnDuplicateKeyUpdate
                                       || string.Equals(_dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase);
            insertSelect = ParseSelectQuery();
            _allowOnDuplicateBoundary = false;
        }

        // Must be VALUES(...) or SELECT...
        if (valuesRaw.Count == 0 && insertSelect is null)
            throw new InvalidOperationException("Invalid INSERT statement: expected VALUES or SELECT.");

        // ON DUPLICATE KEY UPDATE
        var onDup = ParseOnDuplicated();

        return new SqlInsertQuery
        {
            Table = table,
            Columns = cols,
            ValuesRaw = valuesRaw,
            ValuesExpr = valuesExpr,
            InsertSelect = insertSelect,
            HasOnDuplicateKeyUpdate = (onDup != null),
            OnDupAssigns = onDup?.Assignments.Select(a => (a.Column, a.ValueRaw)).ToList() ?? [],
            OnDupAssignsParsed = onDup?.Assignments.Select(a => new SqlAssignment(a.Column, a.ValueRaw, TryParseScalar(a.ValueRaw))).ToList() ?? new List<SqlAssignment>()
        };
    }

    private List<string> ParseCols()
    {
        if (!IsSymbol(Peek(), "("))
            return [];

        var cols = new List<string>();
        Consume(); // (
        while (!IsSymbol(Peek(), ")"))
        {
            cols.Add(ExpectIdentifier());
            if (IsSymbol(Peek(), ",")) Consume();
            else break;
        }
        ExpectSymbol(")");

        return cols;
    }

    private SqlOnDuplicateKeyUpdate? ParseOnDuplicated()
    {
        if (!IsWord(Peek(), "ON"))
            return null;

        var next = Peek(1);

        // MySQL: ON DUPLICATE KEY UPDATE
        if (IsWord(next, "DUPLICATE"))
        {
            if (!_dialect.SupportsOnDuplicateKeyUpdate)
                throw new InvalidOperationException($"Dialeto '{_dialect.Name}' não suporta ON DUPLICATE KEY UPDATE.");

            Consume(); // ON
            ExpectWord("DUPLICATE");
            ExpectWord("KEY");
            ExpectWord("UPDATE");

            var assigns = ParseAssignmentsList();
            return new SqlOnDuplicateKeyUpdate(assigns);
        }

        // PostgreSQL: ON CONFLICT (...) DO UPDATE SET ...  |  ON CONFLICT DO NOTHING
        if (IsWord(next, "CONFLICT"))
        {
            if (!string.Equals(_dialect.Name, "postgresql", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Dialeto '{_dialect.Name}' não suporta ON CONFLICT.");

            Consume(); // ON
            ExpectWord("CONFLICT");

            // Target opcional: (col1, col2, ...)
            if (IsSymbol(Peek(), "("))
            {
                Consume(); // (
                while (!IsSymbol(Peek(), ")"))
                {
                    _ = ExpectIdentifier();
                    if (IsSymbol(Peek(), ",")) Consume();
                    else break;
                }
                ExpectSymbol(")");
            }

            ExpectWord("DO");

            if (IsWord(Peek(), "NOTHING"))
            {
                Consume();
                return new SqlOnDuplicateKeyUpdate([]);
            }

            ExpectWord("UPDATE");
            ExpectWord("SET");
            var assigns = ParseAssignmentsList();
            return new SqlOnDuplicateKeyUpdate(assigns);
        }

        return null;
    }

    private SqlUpdateQuery ParseUpdate()
    {
        Consume(); // UPDATE
        var table = ParseTableSource();

        // MySQL: UPDATE <table> [alias] JOIN (...) ... SET ...
        //string? alias = null;
        var hasJoin = false;

        //// alias (ex: UPDATE users u JOIN ...)
        //if (Peek().Kind == SqlTokenKind.Identifier && !IsWord(Peek(), "SET") && !IsJoinStart(Peek()))
        //{
        //    alias = Consume().Text;
        //}

        // Se vier JOIN antes do SET, pulamos os tokens do JOIN aqui (as estratégias smart usam RawSql)
        if (IsJoinStart(Peek()))
        {
            hasJoin = true;
            SkipUntilTopLevelWord("SET");
        }

        ExpectWord("SET");

        var assignsList = ParseAssignmentsList();
        var setList = assignsList.Select(a => (a.Column, a.ValueRaw)).ToList();

        string? whereRaw = null;
        if (IsWord(Peek(), "WHERE"))
        {
            Consume(); // WHERE
            whereRaw = ReadClauseTextUntilTopLevelStop();
        }

        var setParsed = setList.Select(it => new SqlAssignment(it.Column, it.ValueRaw, TryParseScalar(it.ValueRaw))).ToList();
        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = SqlExpressionParser.ParseWhere(whereRaw!, _dialect); }
            catch { whereExpr = null; }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        return new SqlUpdateQuery
        {
            Table = table,
            Set = setList,
            SetParsed = setParsed,
            WhereRaw = whereRaw,
            Where = whereExpr,
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

        if (IsWord(Peek(), "FROM"))
        {
            // DELETE FROM t WHERE ...
            Consume();
            table = ParseTableSource();
        }
        else
        {
            // DELETE sem FROM (alguns dialetos permitem, outros não).
            // a) DELETE t WHERE ...
            // b) DELETE a FROM t a JOIN (...) s ON ...
            // Para (b) precisamos guardar a tabela real (t), não o alias (a).

            if (!_dialect.SupportsDeleteWithoutFrom)
                throw new InvalidOperationException($"DELETE sem FROM não suportado no dialeto '{_dialect.Name}'.");

            var first = ParseTableSource(); // pode ser tabela ou alvo

            if (IsWord(Peek(), "FROM"))
            {
                if (!_dialect.SupportsDeleteTargetAlias)
                    throw new InvalidOperationException($"DELETE <alvo> FROM ... não suportado no dialeto '{_dialect.Name}'.");

                // DELETE <alias> FROM <table> <alias> JOIN ...
                Consume(); // FROM
                table = ParseTableSource(); // ex: users

                // alias pós-tabela (ex: users u)
                if (Peek().Kind == SqlTokenKind.Identifier && !IsWord(Peek(), "WHERE") && !IsJoinStart(Peek()))
                    Consume();

                if (IsJoinStart(Peek()))
                {
                    hasJoin = true;
                    // A estratégia smart faz o parsing completo a partir do RawSql; aqui só garantimos que o parser não trava.
                }
            }
            else
            {
                // DELETE <table> WHERE ...
                table = first;

                // alias opcional (DELETE users u WHERE ...) - tolerado
                if (Peek().Kind == SqlTokenKind.Identifier &&
                    !IsWord(Peek(), "WHERE") &&
                    !IsWord(Peek(), "ORDER") &&
                    !IsWord(Peek(), "LIMIT") &&
                    !IsJoinStart(Peek()))
                {
                    Consume();
                }
            }
        }

        string? whereRaw = null;

        if (IsWord(Peek(), "WHERE"))
        {
            Consume();
            whereRaw = ReadClauseTextUntilTopLevelStop();
        }

        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = SqlExpressionParser.ParseWhere(whereRaw!, _dialect); }
            catch { whereExpr = null; }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        return new SqlDeleteQuery
        {
            Table = table,
            WhereRaw = whereRaw,
            Where = whereExpr,
            DeleteFromSelect = hasJoin
                ? new SqlSelectQuery([], false, [], [], null, [], null, [], null)
                : null
        };
    }

    private SqlMergeQuery ParseMerge()
    {
        Consume(); // MERGE
        if (IsWord(Peek(), "INTO")) Consume();

        // target table + alias (ex: stats target)
        var target = ParseTableSource();

        // O resto do MERGE é grande demais pra agora.
        // Só avançamos tokens até o fim pra não deixar lixo se você evoluir o parser.
        while (Peek().Kind != SqlTokenKind.EndOfFile)
            Consume();

        return new SqlMergeQuery
        {
            Table = target
        };
    }

    // ------------------------------------------------------------
    // SELECT (Lógica já existente, mantida e integrada)
    // ------------------------------------------------------------

    public SqlSelectQuery ParseSelectQuery()
    {
        var ctes = TryParseCtes();

        ExpectWord("SELECT");
        if (IsWord(Peek(), "SELECT"))
            throw new InvalidOperationException("invalid: duplicated SELECT keyword");
        var distinct = TryParseDistinct();
        var top = TryParseTop();
        var selectItems = ParseSelectItemsWithValidation();
        var table = ParseFromOrDual();
        var joins = ParseJoins(table);
        var where = TryParseWhereExpr();
        var groupBy = TryParseGroupBy();
        var having = TryParseHavingExpr();
        var orderBy = TryParseOrderBy();
        var rowLimit = TryParseRowLimitTail();
        if (top is not null)
        {
            // TOP é prefixo (SQL Server). Se também apareceu LIMIT/FETCH no fim, prioriza o fim.
            rowLimit ??= top;
        }

        ExpectEndOrUnionBoundary();

        return new SqlSelectQuery(
            Ctes: ctes,
            Distinct: distinct,
            SelectItems: selectItems,
            Joins: joins,
            Where: where,
            OrderBy: orderBy,
            RowLimit: rowLimit,
            GroupBy: groupBy,
            Having: having
        )
        {
            Table = table
        };
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

    private List<SqlAssignment> ParseAssignmentsList()
    {
        var list = new List<SqlAssignment>();
        while (!IsEnd(Peek()))
        {
            var col = ExpectIdentifierWithDots();
            ExpectSymbol("=");

            // Lê expressão até , ou WHERE ou fim
            var exprRaw = ReadClauseTextUntilTopLevelStop(",", "WHERE");
            list.Add(new SqlAssignment(col, exprRaw));

            if (IsSymbol(Peek(), ","))
            {
                Consume();
                continue;
            }
            break;
        }
        return list;
    }

    private static List<string> SplitRawByComma(string rawBlock)
    {
        // Método "quick & dirty" para separar "1, 'abc', func(x)" em lista de strings, respeitando parens
        // Usado apenas para ValuesRaw do Insert
        var res = new List<string>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < rawBlock.Length; i++)
        {
            if (rawBlock[i] == '(') depth++;
            else if (rawBlock[i] == ')') depth--;

            if (depth == 0 && rawBlock[i] == ',')
            {
                res.Add(rawBlock[start..i].Trim());
                start = i + 1;
            }
        }
        if (start < rawBlock.Length)
            res.Add(rawBlock[start..].Trim());
        return res;
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
        return TokensToSql(buf);
    }

    private SqlQueryBase ParseCreate()
    {
        ExpectWord("CREATE");

        // CREATE OR REPLACE ...
        var orReplace = false;
        if (IsWord(Peek(), "OR"))
        {
            Consume();
            ExpectWord("REPLACE");
            orReplace = true;
        }

        // CREATE VIEW ...
        if (IsWord(Peek(), "VIEW"))
            return ParseCreateView(orReplace);

        var isTemporary = false;
        var tempScope = TemporaryTableScope.None;
        if (IsWord(Peek(), "GLOBAL"))
        {
            Consume();
            if (IsWord(Peek(), "TEMPORARY") || IsWord(Peek(), "TEMP"))
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

        if (!isTemporary && (IsWord(Peek(), "TEMPORARY") || IsWord(Peek(), "TEMP")))
        {
            Consume();
            isTemporary = true;
            tempScope = TemporaryTableScope.Connection;
        }

        ExpectWord("TABLE");

        var ifNotExists = false;
        if (IsWord(Peek(), "IF"))
        {
            // IF NOT EXISTS
            Consume(); // IF
            ExpectWord("NOT");
            ExpectWord("EXISTS");
            ifNotExists = true;
        }

        // table name
        var nameTok = Peek();
        if (nameTok.Kind != SqlTokenKind.Identifier)
            throw new InvalidOperationException($"Esperava nome da tabela, veio {nameTok.Kind} '{nameTok.Text}'");

        var table = ParseTableSource();

        // Optional column list: (id INT, name VARCHAR(50))
        var colNames = new List<string>();
        if (IsSymbol(Peek(), "("))
        {
            Consume(); // (
            var depth = 1;
            var expectColName = true;
            while (!IsEnd(Peek()) && depth > 0)
            {
                var t = Consume();
                if (IsSymbol(t, "(")) { depth++; continue; }
                if (IsSymbol(t, ")"))
                {
                    depth--;
                    if (depth == 0) break;
                    continue;
                }

                if (depth == 1 && expectColName && t.Kind == SqlTokenKind.Identifier)
                {
                    colNames.Add(t.Text);
                    expectColName = false;
                    continue;
                }

                if (depth == 1 && IsSymbol(t, ","))
                {
                    expectColName = true;
                    continue;
                }
            }
        }

        // CREATE TEMPORARY TABLE ... AS SELECT ...
        // find AS at top-level (paren depth 0)
        if (!IsWord(Peek(), "AS"))
            ExpectWord("AS");
        else
            Consume();

        // remaining tokens compose SELECT/WITH statement
        var rest = new List<SqlToken>();
        while (!IsEnd(Peek()))
            rest.Add(Consume());

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
            throw new NotSupportedException("Apenas CREATE TEMPORARY TABLE é suportado no mock no momento.");

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

    private SqlQueryBase ParseCreateView(bool orReplace)
    {
        ExpectWord("VIEW");

        // IF NOT EXISTS (not official MySQL syntax, but we accept for mocks)
        var ifNotExists = false;
        if (IsWord(Peek(), "IF"))
        {
            Consume(); // IF
            ExpectWord("NOT");
            ExpectWord("EXISTS");
            ifNotExists = true;
        }

        // view name
        var nameTok = Peek();
        if (nameTok.Kind != SqlTokenKind.Identifier)
            throw new InvalidOperationException($"Esperava nome da view, veio {nameTok.Kind} '{nameTok.Text}'");


        var viewName = ParseTableSource();

        // Optional column list: (col1, col2, ...)
        var colNames = new List<string>();
        if (IsSymbol(Peek(), "("))
        {
            Consume(); // (
            var depth = 1;
            var expectColName = true;
            while (!IsEnd(Peek()) && depth > 0)
            {
                var t = Consume();
                if (IsSymbol(t, "(")) { depth++; continue; }
                if (IsSymbol(t, ")"))
                {
                    depth--;
                    if (depth == 0) break;
                    continue;
                }

                if (depth == 1 && expectColName && t.Kind == SqlTokenKind.Identifier)
                {
                    colNames.Add(t.Text);
                    expectColName = false;
                    continue;
                }

                if (depth == 1 && IsSymbol(t, ","))
                {
                    expectColName = true;
                    continue;
                }
            }
        }

        ExpectWord("AS");

        var rest = new List<SqlToken>();
        while (!IsEnd(Peek()))
            rest.Add(Consume());

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

    // --- Helpers de SELECT trazidos do arquivo original ---

    private bool TryParseDistinct()
    {
        if (!IsWord(Peek(), "DISTINCT")) return false;
        Consume();
        if (IsWord(Peek(), "DISTINCT"))
            throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
        return true;
    }


    private SqlTop? TryParseTop()
    {
        // SQL Server: SELECT TOP (10) ... / SELECT TOP 10 ...
        if (!IsWord(Peek(), "TOP")) return null;

        // Se o dialeto não suporta, deixa o SQL cair como erro em validação ou corpo
        if (!_dialect.SupportsTop)
            return null;

        Consume(); // TOP

        // TOP pode vir como (N) ou N
        if (IsSymbol(Peek(), "("))
        {
            Consume();
            var n = ExpectNumberInt();
            ExpectSymbol(")");
            return new SqlTop(n);
        }

        return new SqlTop(ExpectNumberInt());
    }

    private List<SqlSelectItem> ParseSelectItemsWithValidation()
    {
        var raws = ParseCommaSeparatedRawItemsUntilAny("FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION");
        return raws.ConvertAll(r =>
        {
            // Fail fast on known-invalid patterns before any splitting/normalization.
            // Example: COUNT(DISTINCT DISTINCT id)
            if (System.Text.RegularExpressions.Regex.IsMatch(
                    r,
                    @"\bDISTINCT\s+DISTINCT\b",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            {
                throw new InvalidOperationException("invalid: duplicated DISTINCT keyword");
            }

            var (expr, alias) = SplitTrailingAsAliasTopLevel(r, _dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException("Empty SELECT item.");

            // Fail fast: duplicated DISTINCT inside function calls like COUNT(DISTINCT DISTINCT id)
            // (the expression parser also checks, but this guard prevents corpus regressions when
            // select-item splitting/reconstruction changes token boundaries).
            if (System.Text.RegularExpressions.Regex.IsMatch(
                    expr,
                    @"\bDISTINCT\s+DISTINCT\b",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase))
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
        if (IsWord(Peek(), "FROM"))
        {
            Consume();
            if (IsWord(Peek(), "FROM"))
                throw new InvalidOperationException("invalid: duplicated FROM keyword");
            var ts = ParseTableSource();
            if (IsWord(Peek(), "FROM"))
                throw new InvalidOperationException("invalid: FROM inside FROM");
            return ts;
        }
        return new SqlTableSource(null, "DUAL", null, Derived: null, null, null);
    }

    private List<SqlJoin> ParseJoins(SqlTableSource from)
    {
        var joins = new List<SqlJoin>();
        if (from is null) return joins;
        while (IsJoinStart(Peek())) joins.Add(ParseJoin());
        return joins;
    }

    private SqlExpr? TryParseWhereExpr()
    {
        if (!IsWord(Peek(), "WHERE")) return null;
        Consume();
        // "ON" here is important for INSERT ... SELECT ... WHERE ... ON DUPLICATE ...
        var txt = ReadClauseTextUntilTopLevelStop("GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "HAVING", "ON");
        return SqlExpressionParser.ParseWhere(txt, _dialect);
    }

    private List<string> TryParseGroupBy()
    {
        var list = new List<string>();
        if (!IsWord(Peek(), "GROUP")) return list;
        Consume();
        ExpectWord("BY");
        list.AddRange(ParseRawItemsUntil("HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION"));
        if (list.Count == 0)
            throw new InvalidOperationException("GROUP BY sem expressões.");
        return list;
    }

    private SqlExpr? TryParseHavingExpr()
    {
        if (!IsWord(Peek(), "HAVING")) return null;
        Consume();
        var txt = ReadClauseTextUntilTopLevelStop("ORDER", "LIMIT", "OFFSET", "FETCH", "UNION");
        return SqlExpressionParser.ParseWhere(txt, _dialect);
    }

    private List<SqlOrderByItem> TryParseOrderBy()
    {
        var list = new List<SqlOrderByItem>();
        if (!IsWord(Peek(), "ORDER")) return list;
        Consume();
        ExpectWord("BY");
        // Reutiliza lógica simplificada
        var raws = ParseCommaSeparatedRawItemsUntilAny("LIMIT", "OFFSET", "FETCH", "UNION");
        foreach (var r in raws)
        {
            var desc = r.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
            var val = desc
                ? r[..^5].Trim()
                : (r.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase)
                    ? r[..^4].Trim()
                    : r);
            list.Add(new SqlOrderByItem(val, desc));
        }
        return list;
    }

    private SqlRowLimit? TryParseRowLimitTail()
    {
        // MySQL/Postgres: LIMIT ...
        if (IsWord(Peek(), "LIMIT"))
        {
            if (!_dialect.SupportsLimitOffset)
                throw new NotSupportedException($"LIMIT não suportado no dialeto '{_dialect.Name}'.");

            Consume();
            int a = ExpectNumberInt();
            if (IsSymbol(Peek(), ","))
            {
                Consume();
                return new SqlLimitOffset(Count: ExpectNumberInt(), Offset: a);
            }
            if (IsWord(Peek(), "OFFSET"))
            {
                Consume();
                return new SqlLimitOffset(Count: a, Offset: ExpectNumberInt());
            }
            return new SqlLimitOffset(Count: a, Offset: null);
        }

        // Oracle/SQL Server/Postgres: OFFSET ... FETCH ...
        if (IsWord(Peek(), "OFFSET"))
        {
            if (!_dialect.SupportsOffsetFetch)
                throw new NotSupportedException($"OFFSET/FETCH não suportado no dialeto '{_dialect.Name}'.");

            Consume();
            var offset = ExpectNumberInt();
            // Oracle/SQLServer frequentemente exigem ROW/ROWS
            if (IsWord(Peek(), "ROW") || IsWord(Peek(), "ROWS"))
                Consume();

            if (IsWord(Peek(), "FETCH"))
            {
                Consume();
                // NEXT/FIRST
                if (IsWord(Peek(), "NEXT") || IsWord(Peek(), "FIRST"))
                    Consume();

                var count = ExpectNumberInt();

                if (IsWord(Peek(), "ROW") || IsWord(Peek(), "ROWS"))
                    Consume();

                if (IsWord(Peek(), "ONLY"))
                    Consume();

                return new SqlFetch(Count: count, Offset: offset);
            }

            return new SqlFetch(Count: int.MaxValue, Offset: offset);
        }

        // Oracle/Postgres: FETCH FIRST n ROWS ONLY
        if (IsWord(Peek(), "FETCH"))
        {
            if (!_dialect.SupportsFetchFirst)
                throw new NotSupportedException($"FETCH FIRST/NEXT não suportado no dialeto '{_dialect.Name}'.");

            Consume();
            if (IsWord(Peek(), "NEXT") || IsWord(Peek(), "FIRST"))
                Consume();

            var count = ExpectNumberInt();

            if (IsWord(Peek(), "ROW") || IsWord(Peek(), "ROWS"))
                Consume();

            if (IsWord(Peek(), "ONLY"))
                Consume();

            return new SqlFetch(Count: count, Offset: null);
        }

        return null;
    }

    // --- Helpers de CTE e Table Source ---

    private List<SqlCte> TryParseCtes()
    {
        var list = new List<SqlCte>();
        if (!IsWord(Peek(), "WITH")) return list;
        Consume();
        if (!_dialect.SupportsWithCte)
            throw new NotSupportedException($"WITH/CTE não suportado para {_dialect.Name} v{_dialect.Version}.");

        if (IsWord(Peek(), "RECURSIVE")) Consume();

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
            ExpectWord("AS");
            var innerSql = ReadBalancedParenRawTokens();
            var q = Parse(innerSql, _dialect);
            if (q is SqlSelectQuery sq) list.Add(new SqlCte(name, sq));

            if (IsSymbol(Peek(), ",")) { Consume(); continue; }
            break;
        }
        return list;
    }

    private SqlTableSource ParseTableSource()
    {
        if (IsSymbol(Peek(), "("))
        {
            var innerSql = ReadBalancedParenRawTokens();
            var alias = ReadOptionalAlias();

            // Derived table pode ser um SELECT simples OU um UNION/UNION ALL.
            // O Parse() atual devolve apenas o primeiro SELECT quando existe UNION,
            // então aqui detectamos e parseamos a cadeia completa.
            var union = ParseUnionChain(innerSql, _dialect);

            if (union.Parts.Count > 1)
            {
                // UNION dentro do derived
                return new SqlTableSource(null, null, alias, Derived: null, DerivedUnion: union, DerivedSql: innerSql);
            }

            // Apenas 1 parte => SELECT normal
            if (union.Parts[0] is SqlSelectQuery sq)
                return new SqlTableSource(null, null, alias, sq, null, innerSql);

            throw new InvalidOperationException("Derived table deve ser um SELECT");
        }

        var first = ExpectIdentifier();
        string? db = null;
        var table = first;
        if (IsSymbol(Peek(), "."))
        {
            Consume();
            db = table;
            table = ExpectIdentifier();
        }
        var alias2 = ReadOptionalAlias();
        return new SqlTableSource(db, table, alias2, null, null, null);
    }

    private SqlJoin ParseJoin()
    {
        var type = SqlJoinType.Inner;
        if (IsWord(Peek(), "LEFT")) { Consume(); type = SqlJoinType.Left; }
        else if (IsWord(Peek(), "RIGHT")) { Consume(); type = SqlJoinType.Right; }
        else if (IsWord(Peek(), "CROSS")) { Consume(); type = SqlJoinType.Cross; }
        else if (IsWord(Peek(), "INNER")) { Consume(); type = SqlJoinType.Inner; }
        if (IsWord(Peek(), "OUTER")) Consume();
        ExpectWord("JOIN");

        var table = ParseTableSource();
        SqlExpr onExpr = new LiteralExpr(true);

        if (type != SqlJoinType.Cross)
        {
            ExpectWord("ON");
            var txt = ReadClauseTextUntilTopLevelStop("JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION");
            onExpr = SqlExpressionParser.ParseWhere(txt, _dialect);
        }
        return new SqlJoin(type, table, onExpr);
    }

    // --- Helpers de Token Plumbing ---

    private string? ReadOptionalAlias()
    {
        if (IsWord(Peek(), "AS"))
        {
            // Se depois do AS vier uma keyword (SELECT/WITH/VALUES/SET/etc),
            // isso NÃO é alias — é parte da sintaxe do comando (ex: CREATE ... AS SELECT).
            var next = Peek(1);
            if (next.Kind == SqlTokenKind.Identifier && IsClauseKeywordToken(next))
                return null;

            Consume(); // AS
            return ExpectIdentifier();
        }

        var t = Peek();
        if (t.Kind == SqlTokenKind.Identifier && !IsClauseKeywordToken(t))
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
            if (depth == 0 && stopWords.Any(sw => IsWord(t, sw))) break;

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
            if (depth == 0 && stopWords.Any(sw => IsWord(t, sw))) break;
            buf.Add(Consume());
        }
        return TokensToSql(buf);
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
                SqlTokenKind.String => $"'{t.Text}'", // tokenizer entrega sem aspas
                SqlTokenKind.Identifier => NeedsIdentifierQuoting(t.Text) ? QuoteIdentifier(t.Text) : t.Text,
                _ => t.Text
            };

            if (sb.Length > 0 && NeedsSpace(prev, t))
                sb.Append(' ');

            sb.Append(text);
            prev = t;
        }

        return sb.ToString().Trim();

        bool NeedsIdentifierQuoting(string ident)
            => ident.Contains(' ', StringComparison.Ordinal)
               || ident.Contains('\t', StringComparison.Ordinal)
               || ident.Contains('\n', StringComparison.Ordinal)
               || ident.Contains('\r', StringComparison.Ordinal);

        string QuoteIdentifier(string ident) => _dialect.IdentifierEscapeStyle switch
        {
            SqlIdentifierEscapeStyle.backtick => $"`{ident.Replace("`", "``", StringComparison.Ordinal)}`",
            SqlIdentifierEscapeStyle.double_quote => $"\"{ident.Replace("\"", "\"\"", StringComparison.Ordinal)}\"",
            SqlIdentifierEscapeStyle.bracket => $"[{ident.Replace("]", "]]", StringComparison.Ordinal)}]",
            _ => ident
        };

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
        if (raw.Length == 0) return (raw, null);

        // Quote behavior depends on dialect:
        // - Strings: single quote always; double quote only if dialect treats it as string
        // - Identifiers: backtick / double quote / brackets only if dialect allows them
        bool dqIsString = dialect.IsStringQuote('"');
        bool allowBacktick = dialect.AllowsBacktickIdentifiers;
        bool allowDqIdent = dialect.AllowsDoubleQuoteIdentifiers && !dqIsString;
        bool allowBracket = dialect.AllowsBracketIdentifiers;

        static bool LooksLikeAliasToken(string rawRight, bool allowBacktick, bool allowDqIdent, bool allowBracket)
        {
            rawRight = rawRight.Trim();
            if (rawRight.Length == 0) return false;

            // Quoted identifiers (may contain spaces) – only if dialect allows that quoting style.
            if (rawRight[0] == '`')
                return allowBacktick && rawRight.Length >= 2 && rawRight[^1] == '`';

            if (rawRight[0] == '"')
                return allowDqIdent && rawRight.Length >= 2 && rawRight[^1] == '"';

            if (rawRight[0] == '[')
                return allowBracket && rawRight.Length >= 2 && rawRight[^1] == ']';

            // Unquoted alias MUST be a single token (no whitespace) and start with letter/_.
            for (int i = 0; i < rawRight.Length; i++)
                if (char.IsWhiteSpace(rawRight[i])) return false;

            var c0 = rawRight[0];
            if (!(char.IsLetter(c0) || c0 == '_')) return false;

            for (int i = 1; i < rawRight.Length; i++)
            {
                var ch = rawRight[i];
                if (!(char.IsLetterOrDigit(ch) || ch == '_' || ch == '$'))
                    return false;
            }

            return true;
        }

        // 1) Preferencial: "... AS alias" (top-level: fora de parênteses e fora de strings/ident-quotes)
        {
            int depth = 0;
            bool inSingle = false, inDoubleString = false, inDoubleIdent = false, inBacktick = false, inBracket = false;

            for (int i = 0; i + 4 <= raw.Length; i++)
            {
                var ch = raw[i];

                if (inSingle)
                {
                    if (ch == '\'' && i + 1 < raw.Length && raw[i + 1] == '\'') { i++; continue; } // ''
                    if (ch == '\'') inSingle = false;
                    continue;
                }

                if (inDoubleString)
                {
                    if (ch == '"') inDoubleString = false;
                    continue;
                }

                if (inDoubleIdent)
                {
                    if (ch == '"') inDoubleIdent = false;
                    continue;
                }

                if (inBacktick)
                {
                    if (ch == '`') inBacktick = false;
                    continue;
                }

                if (inBracket)
                {
                    if (ch == ']')
                    {
                        // SQL Server bracket escaping: ]]
                        if (i + 1 < raw.Length && raw[i + 1] == ']') { i++; continue; }
                        inBracket = false;
                    }
                    continue;
                }

                if (ch == '(') { depth++; continue; }
                if (ch == ')') { if (depth > 0) depth--; continue; }

                if (depth != 0) continue;

                if (ch == '\'') { inSingle = true; continue; }
                if (ch == '"')
                {
                    if (dqIsString) inDoubleString = true;
                    else if (allowDqIdent) inDoubleIdent = true;
                    continue;
                }
                if (ch == '`' && allowBacktick) { inBacktick = true; continue; }
                if (ch == '[' && allowBracket) { inBracket = true; continue; }

                if ((ch == 'a' || ch == 'A') && i + 4 <= raw.Length)
                {
                    if ((raw[i + 1] == 's' || raw[i + 1] == 'S') &&
                        char.IsWhiteSpace(raw[i + 2]) &&
                        char.IsWhiteSpace(raw[i - 1 >= 0 ? i - 1 : 0]))
                    {
                        // Too fragile; we instead do a proper word check below with boundaries
                    }
                }

                // Match " AS " with word boundaries
                if ((ch == 'A' || ch == 'a')
                    && (raw[i + 1] == 'S' || raw[i + 1] == 's'))
                {
                    bool leftOk = (i == 0) || char.IsWhiteSpace(raw[i - 1]);
                    bool rightOk = (i + 2 < raw.Length) && char.IsWhiteSpace(raw[i + 2]);
                    if (leftOk && rightOk)
                    {
                        var expr = raw[..i].Trim();
                        var aliasRaw = raw[(i + 2)..].Trim();
                        if (aliasRaw.Length == 0) return (raw, null);

                        var alias = NormalizeAlias(aliasRaw, dialect, dqIsString, allowBacktick, allowDqIdent, allowBracket);
                        return (expr, alias);
                    }
                }
            }
        }

        // 2) Caso: "... alias" (sem AS) no final, top-level.
        // Vamos tentar separar o último token como alias, respeitando os quotes permitidos pelo dialeto.
        {
            int depth = 0;
            bool inSingle = false, inDoubleString = false, inDoubleIdent = false, inBacktick = false, inBracket = false;

            for (int i = raw.Length - 1; i >= 0; i--)
            {
                var ch = raw[i];

                if (inSingle)
                {
                    if (ch == '\'') inSingle = false;
                    continue;
                }

                if (inDoubleString)
                {
                    if (ch == '"') inDoubleString = false;
                    continue;
                }

                if (inDoubleIdent)
                {
                    if (ch == '"') inDoubleIdent = false;
                    continue;
                }

                if (inBacktick)
                {
                    if (ch == '`') inBacktick = false;
                    continue;
                }

                if (inBracket)
                {
                    if (ch == '[') inBracket = false;
                    continue;
                }

                if (ch == ')') { depth++; continue; }
                if (ch == '(') { if (depth > 0) depth--; continue; }

                if (depth != 0) continue;

                if (ch == '\'') { inSingle = true; continue; }
                if (ch == '"')
                {
                    if (dqIsString) inDoubleString = true;
                    else if (allowDqIdent) inDoubleIdent = true;
                    continue;
                }
                if (ch == '`' && allowBacktick) { inBacktick = true; continue; }
                if (ch == ']' && allowBracket) { inBracket = true; continue; }

                if (char.IsWhiteSpace(ch))
                {
                    var left = raw[..i].TrimEnd();
                    var right = raw[(i + 1)..].TrimStart();

                    if (left.Length == 0 || right.Length == 0)
                        continue;

                    // If right starts with a quote that this dialect DOES NOT allow for identifiers,
                    // treat it as not supported rather than silently accepting.
                    if ((right[0] == '`' && !allowBacktick) ||
                        (right[0] == '[' && !allowBracket) ||
                        (right[0] == '"' && !allowDqIdent && !dqIsString))
                    {
                        throw new NotSupportedException($"Identificador/alias quoting não suportado no dialeto {dialect.Name}: '{right[0]}'.");
                    }

                    // Avoid splitting if it looks like "expr op something"
                    // Here we keep the heuristic minimal: if left ends with one of these, it's not alias.
                    var lastLeft = left.TrimEnd();
                    if (lastLeft.EndsWith('=')
                        || lastLeft.EndsWith('>')
                        || lastLeft.EndsWith('<')
                        || lastLeft.EndsWith('!')
                        || lastLeft.EndsWith('+')
                        || lastLeft.EndsWith('-')
                        || lastLeft.EndsWith('*')
                        || lastLeft.EndsWith('/')
                        || lastLeft.EndsWith(','))
                        continue;

                    // Alias must be a single identifier token (possibly quoted). Without this guard,
                    // expressions like "... ELSE 0 END" could be incorrectly split as expr="... ELSE"
                    // alias="0 END", which breaks CASE parsing.
                    if (!LooksLikeAliasToken(right, allowBacktick, allowDqIdent, allowBracket))
                        continue;

                    var alias = NormalizeAlias(right, dialect, dqIsString, allowBacktick, allowDqIdent, allowBracket);

                    // Do not treat SQL keywords as aliases.
                    // Example: "CASE ... 'ON DUPLICATE' ... END" (END is a keyword, not an alias).
                    if (dialect.IsKeyword(alias))
                        continue;

                    return (left, alias);
                }
            }
        }

        return (raw, null);
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
        if (aliasRaw.StartsWith('`') && !allowBacktick)
            throw new NotSupportedException($"Dialeto '{dialect.Name}' não permite alias/identificadores com '`'.");

        if (aliasRaw.StartsWith('[') && !allowBracket)
            throw new NotSupportedException($"Dialeto '{dialect.Name}' não permite alias/identificadores com '['.");

        if (aliasRaw.StartsWith('"') && !allowDqIdent && !dqIsString)
            throw new NotSupportedException($"Dialeto '{dialect.Name}' não permite alias/identificadores com '\"'.");

        // Strip outer identifier quotes (only those permitted for identifiers).
        if (allowBacktick && aliasRaw.Length >= 2 && aliasRaw[0] == '`' && aliasRaw[^1] == '`')
            return aliasRaw[1..^1];

        if (allowDqIdent && aliasRaw.Length >= 2 && aliasRaw[0] == '"' && aliasRaw[^1] == '"')
            return aliasRaw[1..^1];

        if (allowBracket && aliasRaw.Length >= 2 && aliasRaw[0] == '[' && aliasRaw[^1] == ']')
        {
            // SQL Server escape: ]] => ]
            var inner = aliasRaw[1..^1].Replace("]]", "]");
            return inner;
        }

        return aliasRaw;
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
    private int ExpectNumberInt()
    {
        var t = Consume();
        return int.Parse(t.Text, CultureInfo.InvariantCulture);
    }
    private void ExpectEndOrUnionBoundary()
    {
        // Após um SELECT completo, só é válido terminar o statement ou seguir com UNION.
        // No MySQL, quando SELECT está dentro de INSERT ... SELECT, pode haver ON DUPLICATE KEY UPDATE depois.
        var t = Peek();
        if (IsEnd(t) || IsWord(t, "UNION")) return;

        // boundary especial: INSERT ... SELECT ... ON DUPLICATE ...
        if (_allowOnDuplicateBoundary && IsWord(t, "ON")) return;

        // tolera ';' final se o split top-level não removeu
        if (IsSymbol(t, ";")) { Consume(); return; }

        throw new InvalidOperationException($"Token inesperado após SELECT: {t.Kind} '{t.Text}'");
    }

    private static readonly HashSet<string> JoinStart = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "OUTER"
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

    private static readonly HashSet<string> ClauseKeywordToken = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "FROM"   ,
        "WHERE"  ,
        "GROUP"  ,
        "HAVING" ,
        "ORDER"  ,
        "LIMIT"  ,
        "UNION"  ,
        "ON"     ,
        "JOIN"   ,
        "INNER"  ,
        "LEFT"   ,
        "RIGHT"  ,
        "CROSS"  ,
        "OUTER"  ,
        "OFFSET" ,
        "FETCH"  ,
        "SET"    ,  // UPDATE
        "VALUES" ,  // INSERT
        "SELECT" ,  // INSERT...SELECT (e derived cases)
        "INTO"   , // útil em variações/dialetos
        "USING"  ,
        "WHEN"   ,
        "MATCHED",
        "THEN"
    };

    private static bool IsClauseKeywordToken(SqlToken t)
        => ClauseKeywordToken.Contains(t.Text);

    // Helpers estáticos de split (mantidos do original)
    internal static List<string> SplitStatementsTopLevel(string sql, ISqlDialect dialect)
    {
        // Split por ';' respeitando strings/identificadores quoted, parênteses e (Postgres) dollar-quoted strings.
        var res = new List<string>();
        if (string.IsNullOrWhiteSpace(sql))
            return res;

        var start = 0;
        var depth = 0;

        bool inSingle = false;
        bool inStringDouble = false; // only if dialect uses " as string quote
        bool inIdentDouble = false;
        bool inBacktick = false;
        bool inBracket = false;

        string? dollarTag = null; // Postgres: $$...$$ or $tag$...$tag$

        for (int i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];

            // Dollar-quoted string (Postgres)
            if (dollarTag is not null)
            {
                if (i + dollarTag.Length <= sql.Length && sql.AsSpan(i, dollarTag.Length).SequenceEqual(dollarTag))
                {
                    i += dollarTag.Length - 1;
                    dollarTag = null;
                }
                continue;
            }

            if (inSingle)
            {
                if (dialect.StringEscapeStyle == SqlStringEscapeStyle.doubled_quote
                    && ch == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    i++; // escape ''
                    continue;
                }

                if (dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash
                    && ch == '\'' && i > 0 && sql[i - 1] == '\\')
                {
                    continue;
                }

                if (ch == '\'') inSingle = false;
                continue;
            }

            if (inStringDouble)
            {
                if (dialect.StringEscapeStyle == SqlStringEscapeStyle.doubled_quote
                    && ch == '"' && i + 1 < sql.Length && sql[i + 1] == '"')
                {
                    i++; // escape ""
                    continue;
                }

                if (dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash
                    && ch == '"' && i > 0 && sql[i - 1] == '\\')
                {
                    continue;
                }

                if (ch == '"') inStringDouble = false;
                continue;
            }

            if (inIdentDouble)
            {
                // "" escape inside quoted identifier
                if (ch == '"' && i + 1 < sql.Length && sql[i + 1] == '"')
                {
                    i++;
                    continue;
                }
                if (ch == '"') inIdentDouble = false;
                continue;
            }

            if (inBacktick)
            {
                if (ch == '`' && i + 1 < sql.Length && sql[i + 1] == '`')
                {
                    i++;
                    continue;
                }
                if (ch == '`') inBacktick = false;
                continue;
            }

            if (inBracket)
            {
                if (ch == ']' && i + 1 < sql.Length && sql[i + 1] == ']')
                {
                    i++;
                    continue;
                }
                if (ch == ']') inBracket = false;
                continue;
            }

            // Start dollar-quote?
            if (dialect.SupportsDollarQuotedStrings && ch == '$')
            {
                // find next '$' to close tag
                int j = i + 1;
                while (j < sql.Length && (char.IsLetterOrDigit(sql[j]) || sql[j] == '_')) j++;
                if (j < sql.Length && sql[j] == '$')
                {
                    dollarTag = sql.Substring(i, j - i + 1);
                    i = j;
                    continue;
                }
            }

            // enter string / identifier quoted
            if (ch == '\'') { inSingle = true; continue; }

            if (ch == '"')
            {
                // MySQL dialect treats " as string; others treat it as identifier quote.
                if (dialect.IsStringQuote('"')) inStringDouble = true;
                else if (dialect.AllowsDoubleQuoteIdentifiers) inIdentDouble = true;
                continue;
            }

            if (ch == '`' && dialect.AllowsBacktickIdentifiers) { inBacktick = true; continue; }
            if (ch == '[' && dialect.AllowsBracketIdentifiers) { inBracket = true; continue; }

            if (ch == '(') { depth++; continue; }
            if (ch == ')') { depth = Math.Max(0, depth - 1); continue; }

            if (ch == ';' && depth == 0)
            {
                var stmt = sql[start..i].Trim();
                if (stmt.Length > 0)
                    res.Add(stmt);
                start = i + 1;
            }
        }

        var last = sql[start..].Trim();
        if (last.Length > 0)
            res.Add(last);

        return res;
    }


    private SqlExpr? TryParseScalar(string raw)
    {
#pragma warning disable CA1031 // Do not catch general exception types
        try { return SqlExpressionParser.ParseScalar(raw, _dialect); }
        catch { return null; }
#pragma warning restore CA1031 // Do not catch general exception types
    }
    // Stub para método que verifica subquery escalar (removido para brevidade, adicione se precisar da validação estrita)
    public static SubqueryExpr ParseSubqueryExprOrThrow(
        string sql,
        SqlToken t,
        string ctx,
        ISqlDialect dialect)
    {
        var q = Parse(sql, dialect);
        if (q is SqlSelectQuery sq) return new SubqueryExpr(sql, sq);
        throw new InvalidOperationException("Subquery deve ser SELECT " + ctx);
    }
}
