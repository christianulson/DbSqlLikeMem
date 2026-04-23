using DbSqlLikeMem.Dialect;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes scalar, table-valued, window, and temporal function lookup for a SQL dialect.
/// PT: Expõe a resolucao de funcoes escalares, com retorno em tabela, de janela e temporais de um dialeto SQL.
/// </summary>
internal interface ISqlDialectFunctions
{
    /// <summary>
    /// EN: Resolves a scalar function definition by name.
    /// PT: Resolve uma definicao de funcao escalar pelo nome.
    /// </summary>
    bool TryGetScalarFunctionDefinition(string functionName, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a scalar function definition from a parsed function call.
    /// PT: Resolve uma definicao de funcao escalar a partir de uma chamada analisada.
    /// </summary>
    bool TryGetScalarFunctionDefinition(FunctionCallExpr functionCall, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a scalar function definition from a legacy call expression.
    /// PT: Resolve uma definicao de funcao escalar a partir de uma expressao de chamada legada.
    /// </summary>
    bool TryGetScalarFunctionDefinition(CallExpr functionCall, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Gets the aggregate function definition registered for the specified name.
    /// PT: Obtém a definicao de funcao agregada registrada para o nome informado.
    /// </summary>
    bool TryGetAggregateFunctionDefinition(string functionName, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a table-valued function definition by name.
    /// PT: Resolve uma definicao de funcao com retorno em tabela pelo nome.
    /// </summary>
    bool TryGetTableFunctionDefinition(string functionName, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a table-valued function definition from a parsed function call.
    /// PT: Resolve uma definicao de funcao com retorno em tabela a partir de uma chamada analisada.
    /// </summary>
    bool TryGetTableFunctionDefinition(FunctionCallExpr functionCall, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a window function definition by name.
    /// PT: Resolve uma definicao de funcao de janela pelo nome.
    /// </summary>
    bool TryGetWindowFunctionDefinition(string functionName, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves a window function definition from a parsed window expression.
    /// PT: Resolve uma definicao de funcao de janela a partir de uma expressao de janela analisada.
    /// </summary>
    bool TryGetWindowFunctionDefinition(WindowFunctionExpr windowFunction, out DbFunctionDef? definition);
    /// <summary>
    /// EN: Resolves the temporal function kind associated with a function name.
    /// PT: Resolve o tipo de funcao temporal associado a um nome de funcao.
    /// </summary>
    bool TryGetTemporalFunctionKind(string functionName, out SqlTemporalFunctionKind kind);
    /// <summary>
    /// EN: Indicates whether the dialect accepts a temporal identifier with the given name.
    /// PT: Indica se o dialeto aceita um identificador temporal com o nome informado.
    /// </summary>
    bool AllowsTemporalIdentifier(string functionName);
    /// <summary>
    /// EN: Indicates whether the dialect accepts a temporal call with the given name.
    /// PT: Indica se o dialeto aceita uma chamada temporal com o nome informado.
    /// </summary>
    bool AllowsTemporalCall(string functionName);
}
