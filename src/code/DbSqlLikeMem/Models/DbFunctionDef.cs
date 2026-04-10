namespace DbSqlLikeMem.Models;

/// <summary>
/// EN: Describes the capability flags supported by a SQL function definition.
/// PT: Descreve as flags de capacidade suportadas por uma definicao de funcao SQL.
/// </summary>
[Flags]
public enum DbFunctionCapability
{
    /// <summary>
    /// EN: Indicates that no capability flags are set.
    /// PT: Indica que nenhuma flag de capacidade esta definida.
    /// </summary>
    None = 0,

    /// <summary>
    /// EN: Marks a scalar function definition.
    /// PT: Marca uma definicao de funcao escalar.
    /// </summary>
    Scalar = 1 << 0,
    /// <summary>
    /// EN: Marks an aggregate function definition.
    /// PT: Marca uma definicao de funcao agregada.
    /// </summary>
    Aggregate = 1 << 1,
    /// <summary>
    /// EN: Marks a window function definition.
    /// PT: Marca uma definicao de funcao de janela.
    /// </summary>
    Window = 1 << 2,
    /// <summary>
    /// EN: Marks a table function definition.
    /// PT: Marca uma definicao de funcao de tabela.
    /// </summary>
    Table = 1 << 3,

    /// <summary>
    /// EN: Indicates that the function supports an OVER clause.
    /// PT: Indica que a funcao suporta a clausula OVER.
    /// </summary>
    SupportsOver = 1 << 4,
    /// <summary>
    /// EN: Indicates that the function requires an OVER clause.
    /// PT: Indica que a funcao exige a clausula OVER.
    /// </summary>
    RequiresOver = 1 << 5,

    /// <summary>
    /// EN: Indicates that the function supports WITHIN GROUP.
    /// PT: Indica que a funcao suporta WITHIN GROUP.
    /// </summary>
    SupportsWithinGroup = 1 << 6,
    /// <summary>
    /// EN: Indicates that the function supports a FILTER clause.
    /// PT: Indica que a funcao suporta uma clausula FILTER.
    /// </summary>
    SupportsFilterClause = 1 << 7,
    /// <summary>
    /// EN: Indicates that the function supports ORDER BY arguments.
    /// PT: Indica que a funcao suporta argumentos ORDER BY.
    /// </summary>
    SupportsOrderByArguments = 1 << 8,

    /// <summary>
    /// EN: Marks a niladic function definition.
    /// PT: Marca uma definicao de funcao niladica.
    /// </summary>
    Niladic = 1 << 9,
    /// <summary>
    /// EN: Marks a variadic function definition.
    /// PT: Marca uma definicao de funcao variadica.
    /// </summary>
    Variadic = 1 << 10
}

/// <summary>
/// EN: Describes the functional category used to group SQL functions.
/// PT: Descreve a categoria funcional usada para agrupar funcoes SQL.
/// </summary>
public enum DbFunctionCategory
{
    /// <summary>
    /// EN: Uses the default general-purpose category.
    /// PT: Usa a categoria geral padrao.
    /// </summary>
    General,
    /// <summary>
    /// EN: Groups string-oriented functions.
    /// PT: Agrupa funcoes orientadas a texto.
    /// </summary>
    String,
    /// <summary>
    /// EN: Groups numeric functions.
    /// PT: Agrupa funcoes numericas.
    /// </summary>
    Numeric,
    /// <summary>
    /// EN: Groups date and time functions.
    /// PT: Agrupa funcoes de data e hora.
    /// </summary>
    DateTime,
    /// <summary>
    /// EN: Groups JSON functions.
    /// PT: Agrupa funcoes JSON.
    /// </summary>
    Json,
    /// <summary>
    /// EN: Groups system-provided functions.
    /// PT: Agrupa funcoes fornecidas pelo sistema.
    /// </summary>
    System,
    /// <summary>
    /// EN: Groups control-flow functions.
    /// PT: Agrupa funcoes de controle de fluxo.
    /// </summary>
    ControlFlow,
    /// <summary>
    /// EN: Groups type-conversion functions.
    /// PT: Agrupa funcoes de conversao de tipo.
    /// </summary>
    Conversion,
    /// <summary>
    /// EN: Groups analytic functions.
    /// PT: Agrupa funcoes analiticas.
    /// </summary>
    Analytic
}

/// <summary>
/// EN: Describes how a SQL function is invoked by the parser and runtime.
/// PT: Descreve como uma funcao SQL e invocada pelo parser e pelo runtime.
/// </summary>
[Flags]
public enum DbInvocationStyle
{
    /// <summary>
    /// EN: Indicates that no invocation style is enabled.
    /// PT: Indica que nenhum estilo de invocacao esta habilitado.
    /// </summary>
    None = 0,
    /// <summary>
    /// EN: Indicates that call syntax is supported.
    /// PT: Indica que a sintaxe de chamada e suportada.
    /// </summary>
    Call = 1 << 0,
    /// <summary>
    /// EN: Indicates that identifier syntax is supported.
    /// PT: Indica que a sintaxe de identificador e suportada.
    /// </summary>
    Identifier = 1 << 1
}

internal delegate TableResultMock AstQueryTableFunctionHandler(
    AstQueryTableFunctionExecutor executor,
    SqlTableSource tableSource,
    IDictionary<string, AstQueryExecutorBase.Source> ctes,
    AstQueryExecutorBase.EvalRow? outerRow);

/// <summary>
/// EN: Describes one parameter declared by a SQL function signature.
/// PT: Descreve um parametro declarado por uma assinatura de funcao SQL.
/// </summary>
/// <param name="Name">EN: Parameter name. PT: Nome do parametro.</param>
/// <param name="TypeSql">EN: SQL type text associated with the parameter. PT: Texto do tipo SQL associado ao parametro.</param>
/// <param name="Required">EN: Indicates whether the parameter is required. PT: Indica se o parametro e obrigatorio.</param>
/// <param name="IsVariadic">EN: Indicates whether the parameter accepts variadic values. PT: Indica se o parametro aceita valores variadicos.</param>
/// <param name="IsOrderByClause">EN: Indicates whether the parameter represents an ORDER BY clause. PT: Indica se o parametro representa uma clausula ORDER BY.</param>
/// <param name="IsFrameClause">EN: Indicates whether the parameter represents a frame clause. PT: Indica se o parametro representa uma clausula de frame.</param>
/// <param name="DefaultValue">EN: Default value used when the argument is omitted. PT: Valor padrao usado quando o argumento e omitido.</param>
public sealed record DbFunctionParameterDef(
    string Name,
    string? TypeSql,
    bool Required = true,
    bool IsVariadic = false,
    bool IsOrderByClause = false,
    bool IsFrameClause = false,
    object? DefaultValue = null)
{
    internal string NormalizedName => NormalizeParamName(Name);

    private static string NormalizeParamName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value!.Trim();
        if (normalized.StartsWith("@", StringComparison.Ordinal))
            normalized = normalized[1..];

        return normalized;
    }
}

/// <summary>
/// EN: Describes one callable signature accepted by a SQL function definition.
/// PT: Descreve uma assinatura chamavel aceita por uma definicao de funcao SQL.
/// </summary>
/// <param name="Parameters">EN: Parameters declared by the signature. PT: Parametros declarados pela assinatura.</param>
/// <param name="MinArguments">EN: Minimum accepted argument count. PT: Numero minimo de argumentos aceitos.</param>
/// <param name="MaxArguments">EN: Maximum accepted argument count. PT: Numero maximo de argumentos aceitos.</param>
/// <param name="AcceptsStar">EN: Indicates whether the signature accepts star syntax. PT: Indica se a assinatura aceita sintaxe de asterisco.</param>
/// <param name="RequiresOrderBy">EN: Indicates whether the signature requires ORDER BY. PT: Indica se a assinatura exige ORDER BY.</param>
public sealed record DbFunctionSignature(
    IReadOnlyList<DbFunctionParameterDef> Parameters,
    int MinArguments,
    int MaxArguments,
    bool AcceptsStar = false,
    bool RequiresOrderBy = false)
{
    internal bool AllowsArgumentCount(int count)
        => count >= MinArguments && count <= MaxArguments;
}

/// <summary>
/// EN: Describes a SQL function definition used by the mock runtime and parser.
/// PT: Descreve uma definicao de funcao SQL usada pelo runtime simulado e pelo parser.
/// </summary>
/// <param name="Name">EN: Function name. PT: Nome da funcao.</param>
/// <param name="ReturnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
/// <param name="Capabilities">EN: Capability flags exposed by the function. PT: Flags de capacidade expostas pela funcao.</param>
/// <param name="Category">EN: Functional category associated with the function. PT: Categoria funcional associada a funcao.</param>
public sealed record DbFunctionDef(
    string Name,
    string? ReturnTypeSql,
    DbFunctionCapability Capabilities,
    DbFunctionCategory Category = DbFunctionCategory.General)
    : ProcessDef(Name)
{
    /// <summary>
    /// EN: Gets the declared parameters for the function.
    /// PT: Obtem os parametros declarados para a funcao.
    /// </summary>
    public IReadOnlyList<DbFunctionParameterDef> Parameters { get; init; } = [];

    internal SqlExpr? Body { get; init; }

    /// <summary>
    /// EN: Gets the accepted callable signatures for the function.
    /// PT: Obtem as assinaturas chamaveis aceitas pela funcao.
    /// </summary>
    public IReadOnlyList<DbFunctionSignature> Signatures { get; init; } = [];

    /// <summary>
    /// EN: Gets the temporal kind associated with the function when applicable.
    /// PT: Obtem o tipo temporal associado a funcao quando aplicavel.
    /// </summary>
    public SqlTemporalFunctionKind? TemporalKind { get; init; }

    /// <summary>
    /// EN: Indicates whether the function is marked as a string aggregate.
    /// PT: Indica se a funcao esta marcada como agregacao de texto.
    /// </summary>
    public bool IsStringAggregate { get; init; }

    /// <summary>
    /// EN: Indicates whether integral inputs should be promoted to decimal for aggregate inference.
    /// PT: Indica se entradas inteiras devem ser promovidas para decimal na inferencia de agregados.
    /// </summary>
    public bool PromotesIntegralInputsToDecimal { get; init; }

    /// <summary>
    /// EN: Gets the invocation styles supported by the function.
    /// PT: Obtem os estilos de invocacao suportados pela funcao.
    /// </summary>
    public DbInvocationStyle InvocationStyle { get; init; } = DbInvocationStyle.Call;

    internal AstQueryGeneralScalarFunctionHandler? AstExecutor { get; init; }

    internal AstQueryTableFunctionHandler? TableExecutor { get; init; }

    /// <summary>
    /// EN: Creates a window function definition with a single signature.
    /// PT: Cria uma definicao de funcao de janela com uma unica assinatura.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="minArguments">EN: Minimum accepted argument count. PT: Numero minimo de argumentos aceitos.</param>
    /// <param name="maxArguments">EN: Maximum accepted argument count. PT: Numero maximo de argumentos aceitos.</param>
    /// <param name="requiresOver">EN: Indicates whether OVER is required. PT: Indica se OVER e obrigatorio.</param>
    /// <param name="requiresOrderBy">EN: Indicates whether ORDER BY is required. PT: Indica se ORDER BY e obrigatorio.</param>
    /// <param name="acceptsStar">EN: Indicates whether star syntax is accepted. PT: Indica se a sintaxe de asterisco e aceita.</param>
    public DbFunctionDef(
        string name,
        int minArguments,
        int maxArguments,
        bool requiresOver = true,
        bool requiresOrderBy = false,
        bool acceptsStar = false)
        : this(
            name,
            null,
            DbFunctionCapability.Window
            | (requiresOver ? DbFunctionCapability.RequiresOver : DbFunctionCapability.None),
            DbFunctionCategory.Analytic)
    {
        if (minArguments < 0)
            throw new ArgumentOutOfRangeException(nameof(minArguments));

        if (maxArguments < minArguments)
            throw new ArgumentOutOfRangeException(nameof(maxArguments));

        Signatures = [new DbFunctionSignature([], minArguments, maxArguments, acceptsStar, requiresOrderBy)];
    }

    /// <summary>
    /// EN: Creates a general function definition with the supplied signatures.
    /// PT: Cria uma definicao geral de funcao com as assinaturas fornecidas.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, DbFunctionCategory.General)
    {
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a categorized function definition with the supplied signatures.
    /// PT: Cria uma definicao de funcao categorizada com as assinaturas fornecidas.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="category">EN: Functional category for the function. PT: Categoria funcional da funcao.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        DbFunctionCategory category,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, category)
    {
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a function definition with the supplied invocation style.
    /// PT: Cria uma definicao de funcao com o estilo de invocacao fornecido.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="invocationStyle">EN: Supported invocation style. PT: Estilo de invocacao suportado.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        DbInvocationStyle invocationStyle,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, DbFunctionCategory.General)
    {
        InvocationStyle = invocationStyle;
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a categorized function definition with the supplied invocation style.
    /// PT: Cria uma definicao de funcao categorizada com o estilo de invocacao fornecido.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="category">EN: Functional category for the function. PT: Categoria funcional da funcao.</param>
    /// <param name="invocationStyle">EN: Supported invocation style. PT: Estilo de invocacao suportado.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        DbFunctionCategory category,
        DbInvocationStyle invocationStyle,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, category)
    {
        InvocationStyle = invocationStyle;
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a temporal function definition.
    /// PT: Cria uma definicao de funcao temporal.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="temporalKind">EN: Temporal kind associated with the function. PT: Tipo temporal associado a funcao.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        SqlTemporalFunctionKind temporalKind,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, DbFunctionCategory.DateTime)
    {
        TemporalKind = temporalKind;
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a categorized temporal function definition.
    /// PT: Cria uma definicao de funcao temporal categorizada.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="category">EN: Functional category for the function. PT: Categoria funcional da funcao.</param>
    /// <param name="temporalKind">EN: Temporal kind associated with the function. PT: Tipo temporal associado a funcao.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        DbFunctionCategory category,
        SqlTemporalFunctionKind temporalKind,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, category)
    {
        TemporalKind = temporalKind;
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Creates a categorized temporal function definition with an explicit invocation style.
    /// PT: Cria uma definicao de funcao temporal categorizada com estilo de invocacao explicito.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="capabilities">EN: Capability flags for the function. PT: Flags de capacidade da funcao.</param>
    /// <param name="category">EN: Functional category for the function. PT: Categoria funcional da funcao.</param>
    /// <param name="invocationStyle">EN: Supported invocation style. PT: Estilo de invocacao suportado.</param>
    /// <param name="temporalKind">EN: Temporal kind associated with the function. PT: Tipo temporal associado a funcao.</param>
    /// <param name="isStringAggregate">EN: Indicates whether the function is a string aggregate. PT: Indica se a funcao e uma agregacao de texto.</param>
    /// <param name="signatures">EN: Accepted signatures. PT: Assinaturas aceitas.</param>
    public DbFunctionDef(
        string name,
        string? returnTypeSql,
        DbFunctionCapability capabilities,
        DbFunctionCategory category,
        DbInvocationStyle invocationStyle,
        SqlTemporalFunctionKind? temporalKind,
        bool isStringAggregate,
        params DbFunctionSignature[] signatures)
        : this(name, returnTypeSql, capabilities, category)
    {
        InvocationStyle = invocationStyle;
        TemporalKind = temporalKind;
        IsStringAggregate = isStringAggregate;
        Signatures = signatures ?? [];
    }

    /// <summary>
    /// EN: Checks whether the function exposes the specified capability flag.
    /// PT: Verifica se a funcao expõe a flag de capacidade informada.
    /// </summary>
    public bool HasCapability(DbFunctionCapability capability)
        => (Capabilities & capability) == capability;

    /// <summary>
    /// EN: Indicates whether the function supports call syntax.
    /// PT: Indica se a funcao suporta sintaxe de chamada.
    /// </summary>
    public bool AllowsCall
        => (InvocationStyle & DbInvocationStyle.Call) != 0;

    /// <summary>
    /// EN: Indicates whether the function supports identifier syntax.
    /// PT: Indica se a funcao suporta sintaxe de identificador.
    /// </summary>
    public bool AllowsIdentifier
        => (InvocationStyle & DbInvocationStyle.Identifier) != 0;

    /// <summary>
    /// EN: Gets the minimum accepted argument count across the signatures.
    /// PT: Obtem o numero minimo de argumentos aceitos entre as assinaturas.
    /// </summary>
    public int MinArguments
        => Signatures.Count == 0
            ? Parameters.Count(parameter => parameter.Required)
            : Signatures.Min(signature => signature.MinArguments);

    /// <summary>
    /// EN: Gets the maximum accepted argument count across the signatures.
    /// PT: Obtem o numero maximo de argumentos aceitos entre as assinaturas.
    /// </summary>
    public int MaxArguments
        => Signatures.Count == 0
            ? Parameters.Count
            : Signatures.Max(signature => signature.MaxArguments);

    /// <summary>
    /// EN: Indicates whether any signature requires ORDER BY.
    /// PT: Indica se alguma assinatura exige ORDER BY.
    /// </summary>
    public bool RequiresOrderBy
        => Signatures.Any(signature => signature.RequiresOrderBy);

    /// <summary>
    /// EN: Indicates whether any signature accepts star syntax.
    /// PT: Indica se alguma assinatura aceita sintaxe de asterisco.
    /// </summary>
    public bool AcceptsStar
        => Signatures.Any(signature => signature.AcceptsStar);

    internal bool AllowsArgumentCount(int count)
        => Signatures.Count == 0
            ? count >= MinArguments && count <= MaxArguments
            : Signatures.Any(signature => signature.AllowsArgumentCount(count));

    /// <summary>
    /// EN: Creates a scalar function definition with the provided signatures.
    /// PT: Cria uma definicao de funcao escalar com as assinaturas fornecidas.
    /// </summary>
    public static DbFunctionDef CreateScalar(
        string name,
        string? returnTypeSql,
        DbFunctionCategory category = DbFunctionCategory.General,
        DbInvocationStyle invocationStyle = DbInvocationStyle.Call,
        params DbFunctionSignature[] signatures)
        => new(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar,
            category,
            invocationStyle,
            null,
            false,
            signatures);

    internal static DbFunctionDef CreateScalar(
        string name,
        string? returnTypeSql,
        AstQueryGeneralScalarFunctionHandler AstExecutor,
        DbFunctionCategory category = DbFunctionCategory.General,
        DbInvocationStyle invocationStyle = DbInvocationStyle.Call,
        params DbFunctionSignature[] signatures)
    {
        var definition = new DbFunctionDef(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar,
            category,
            invocationStyle,
            null,
            false,
            signatures)
        {
            AstExecutor = AstExecutor
        };

        return definition;
    }

    internal static DbFunctionDef CreateUserDefined(
        string name,
        string? returnTypeSql,
        IReadOnlyList<DbFunctionParameterDef>? parameters,
        SqlExpr body)
        => new(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar,
            DbFunctionCategory.General,
            DbInvocationStyle.Call)
        {
            Parameters = parameters ?? [],
            Body = body
        };

    /// <summary>
    /// EN: Creates a user-defined scalar function by parsing the SQL body in the provided database context.
    /// PT: Cria uma funcao escalar definida pelo usuario fazendo o parsing do corpo SQL no contexto de banco informado.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="parameters">EN: Declared function parameters. PT: Parametros declarados da funcao.</param>
    /// <param name="bodySql">EN: Scalar SQL expression used as the function body. PT: Expressao SQL escalar usada como corpo da funcao.</param>
    /// <param name="db">EN: Database context used to parse the body. PT: Contexto de banco usado para fazer o parsing do corpo.</param>
    /// <param name="customFunctionSupported">EN: Optional resolver for schema-defined functions referenced by the body. PT: Resolvedor opcional para funcoes definidas no schema referenciadas pelo corpo.</param>
    /// <returns>EN: Created function definition. PT: Definicao de funcao criada.</returns>
    public static DbFunctionDef CreateUserDefined(
        string name,
        string? returnTypeSql,
        IReadOnlyList<DbFunctionParameterDef>? parameters,
        string bodySql,
        DbMock db,
        Func<string, bool>? customFunctionSupported = null)
        => CreateUserDefined(name, returnTypeSql, parameters, bodySql, db, db.Dialect, customFunctionSupported);

    /// <summary>
    /// EN: Creates a user-defined scalar function by parsing the SQL body with an explicit dialect.
    /// PT: Cria uma funcao escalar definida pelo usuario fazendo o parsing do corpo SQL com um dialeto explicito.
    /// </summary>
    /// <param name="name">EN: Function name. PT: Nome da funcao.</param>
    /// <param name="returnTypeSql">EN: SQL text for the return type. PT: Texto SQL do tipo de retorno.</param>
    /// <param name="parameters">EN: Declared function parameters. PT: Parametros declarados da funcao.</param>
    /// <param name="bodySql">EN: Scalar SQL expression used as the function body. PT: Expressao SQL escalar usada como corpo da funcao.</param>
    /// <param name="db">EN: Database context used to parse the body. PT: Contexto de banco usado para fazer o parsing do corpo.</param>
    /// <param name="dialect">EN: Optional dialect override used while parsing. PT: Dialeto opcional de substituicao usado durante o parsing.</param>
    /// <param name="customFunctionSupported">EN: Optional resolver for schema-defined functions referenced by the body. PT: Resolvedor opcional para funcoes definidas no schema referenciadas pelo corpo.</param>
    /// <returns>EN: Created function definition. PT: Definicao de funcao criada.</returns>
    internal static DbFunctionDef CreateUserDefined(
        string name,
        string? returnTypeSql,
        IReadOnlyList<DbFunctionParameterDef>? parameters,
        string bodySql,
        DbMock db,
        ISqlDialect? dialect = null,
        Func<string, bool>? customFunctionSupported = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(bodySql, nameof(bodySql));
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));

        var parsedBody = SqlExpressionParser.ParseScalar(
            bodySql,
            db,
            dialect ?? db.Dialect,
            null,
            customFunctionSupported);

        return CreateUserDefined(name, returnTypeSql, parameters, parsedBody);
    }

    /// <summary>
    /// EN: Creates a table function definition with the provided signatures.
    /// PT: Cria uma definicao de funcao de tabela com as assinaturas fornecidas.
    /// </summary>
    public static DbFunctionDef CreateTable(
        string name,
        DbFunctionCategory category = DbFunctionCategory.General,
        params DbFunctionSignature[] signatures)
        => new(
            name,
            null,
            DbFunctionCapability.Table,
            category,
            DbInvocationStyle.Call,
            null,
            false,
            signatures);

    /// <summary>
    /// EN: Creates a temporal scalar function definition with the provided signatures.
    /// PT: Cria uma definicao de funcao escalar temporal com as assinaturas fornecidas.
    /// </summary>
    public static DbFunctionDef CreateTemporal(
        string name,
        string? returnTypeSql,
        SqlTemporalFunctionKind temporalKind,
        DbInvocationStyle invocationStyle = DbInvocationStyle.Call,
        params DbFunctionSignature[] signatures)
        => new(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar,
            DbFunctionCategory.DateTime,
            invocationStyle,
            temporalKind,
            false,
            signatures);

    /// <summary>
    /// EN: Creates a niladic scalar function definition that is invoked as an identifier.
    /// PT: Cria uma definicao de funcao escalar niladica invocada como identificador.
    /// </summary>
    public static DbFunctionDef CreateIdentifier(
        string name,
        string? returnTypeSql,
        DbFunctionCategory category = DbFunctionCategory.General)
        => new(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar | DbFunctionCapability.Niladic,
            category,
            DbInvocationStyle.Identifier,
            null,
            false);

    /// <summary>
    /// EN: Creates a scalar function definition that accepts both call and identifier syntax.
    /// PT: Cria uma definicao de funcao escalar que aceita sintaxe de chamada e identificador.
    /// </summary>
    public static DbFunctionDef CreateCallOrIdentifier(
        string name,
        string? returnTypeSql,
        DbFunctionCategory category = DbFunctionCategory.General,
        params DbFunctionSignature[] signatures)
        => new(
            name,
            returnTypeSql,
            DbFunctionCapability.Scalar,
            category,
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            null,
            false,
            signatures);
}
