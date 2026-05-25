namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;
    private readonly Lazy<Dialect.FunctionDictionaryProcess> _functions;
    private Dialect.FunctionDictionaryProcess? _functionRegistry;

    /// <summary>
    /// EN: Stores the shared state for concrete SQL dialect implementations.
    /// PT-br: Armazena o estado compartilhado das implementacoes concretas de dialeto SQL.
    /// </summary>
    protected SqlDialectBase(
        string name,
        int version,
        IEnumerable<string> keywords,
        IEnumerable<KeyValuePair<string, SqlBinaryOp>> binOps,
        IEnumerable<string> operators)
    {
        Name = name;
        Version = version;
        _keywords = new HashSet<string>(keywords, StringComparer.OrdinalIgnoreCase);
        _binOps = new Dictionary<string, SqlBinaryOp>(StringComparer.OrdinalIgnoreCase);
        _functions = new Lazy<Dialect.FunctionDictionaryProcess>(CreateFunctionRegistry);
        foreach (var kv in binOps)
            _binOps[kv.Key] = kv.Value;

        Operators = [.. operators
            .Concat(["*", "/", "+", "-"])
            .Concat(binOps.Select(kv => kv.Key))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(s => s.Length)
            .ThenBy(s => s, StringComparer.Ordinal)];
    }

    /// <summary>
    /// EN: Gets the canonical dialect name.
    /// PT-br: Obtém o nome canonico do dialeto.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// EN: Gets the compatibility version used by the dialect.
    /// PT-br: Obtém a versao de compatibilidade usada pelo dialeto.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// EN: Gets the function registry populated by this dialect.
    /// PT-br: Obtém o registry de funcoes populado por este dialeto.
    /// </summary>
    public Dialect.FunctionDictionaryProcess Functions
        => _functionRegistry ?? _functions.Value;

    /// <summary>
    /// EN: Gets the stored procedure registry populated by this dialect.
    /// PT-br: Obtém o registry de procedimentos armazenados populado por este dialeto.
    /// </summary>
    public IDictionaryProcess<ProcedureDef> Procedures
        { get; } = new Dialect.DictionaryProcess<ProcedureDef>();

    /// <summary>
    /// EN: Builds the lazy function registry for the dialect instance.
    /// PT-br: Constrói o registry de funcoes lazy para a instancia do dialeto.
    /// </summary>
    /// <returns>EN: Populated function registry. PT-br: Registry de funcoes populado.</returns>
    protected virtual Dialect.FunctionDictionaryProcess CreateFunctionRegistry()
    {
        var functions = new Dialect.FunctionDictionaryProcess();
        _functionRegistry = functions;
        try
        {
            InitializeFunctionRegistry();
        }
        finally
        {
            _functionRegistry = null;
        }

        return functions;
    }

    /// <summary>
    /// EN: Populates the function registry when it is first created.
    /// PT-br: Popula o registry de funcoes quando ele e criado pela primeira vez.
    /// </summary>
    protected virtual void InitializeFunctionRegistry()
    {
    }
}
