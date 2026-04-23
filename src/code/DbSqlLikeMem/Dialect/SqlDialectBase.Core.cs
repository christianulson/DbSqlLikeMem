using System;
using System.Collections.Generic;
using System.Linq;
using DbSqlLikeMem.Dialect;

namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;
    private readonly Lazy<DbSqlLikeMem.Dialect.FunctionDictionaryProcess> _functions;
    private DbSqlLikeMem.Dialect.FunctionDictionaryProcess? _functionRegistry;

    /// <summary>
    /// EN: Stores the shared state for concrete SQL dialect implementations.
    /// PT: Armazena o estado compartilhado das implementacoes concretas de dialeto SQL.
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
        _functions = new Lazy<DbSqlLikeMem.Dialect.FunctionDictionaryProcess>(CreateFunctionRegistry);
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
    /// PT: Obtém o nome canonico do dialeto.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// EN: Gets the compatibility version used by the dialect.
    /// PT: Obtém a versao de compatibilidade usada pelo dialeto.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// EN: Gets the function registry populated by this dialect.
    /// PT: Obtém o registry de funcoes populado por este dialeto.
    /// </summary>
    public DbSqlLikeMem.Dialect.FunctionDictionaryProcess Functions
        => _functionRegistry ?? _functions.Value;

    /// <summary>
    /// EN: Gets the stored procedure registry populated by this dialect.
    /// PT: Obtém o registry de procedimentos armazenados populado por este dialeto.
    /// </summary>
    public DbSqlLikeMem.Interfaces.IDictionaryProcess<ProcedureDef> Procedures
        { get; } = new DbSqlLikeMem.Dialect.DictionaryProcess<ProcedureDef>();

    /// <summary>
    /// EN: Builds the lazy function registry for the dialect instance.
    /// PT: Constrói o registry de funcoes lazy para a instancia do dialeto.
    /// </summary>
    /// <returns>EN: Populated function registry. PT: Registry de funcoes populado.</returns>
    protected virtual DbSqlLikeMem.Dialect.FunctionDictionaryProcess CreateFunctionRegistry()
    {
        var functions = new DbSqlLikeMem.Dialect.FunctionDictionaryProcess();
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
    /// PT: Popula o registry de funcoes quando ele e criado pela primeira vez.
    /// </summary>
    protected virtual void InitializeFunctionRegistry()
    {
    }
}
