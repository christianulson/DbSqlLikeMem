namespace DbSqlLikeMem;

/// <summary>
/// EN: Describes a scalar function mapping for a method annotated by the source generator.
/// PT-br: Descreve um mapeamento de funcao escalar para um metodo anotado pelo gerador de codigo.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
internal sealed class ScalarFunctionAttribute(
    string name,
    string returnTypeSql
    ) : Attribute
{
    /// <summary>
    /// EN: Gets the scalar function name used by the generated mapping.
    /// PT-br: Obtem o nome da funcao escalar usado pelo mapeamento gerado.
    /// </summary>
    public string Name { get; } = name;

    /// <summary>
    /// EN: Gets the SQL return type declared for the scalar function.
    /// PT-br: Obtem o tipo SQL de retorno declarado para a funcao escalar.
    /// </summary>
    public string ReturnTypeSql { get; } = returnTypeSql;

    /// <summary>
    /// EN: Gets or sets the invocation style used by the generated scalar function mapping.
    /// PT-br: Obtem ou define o estilo de invocacao usado pelo mapeamento gerado da funcao escalar.
    /// </summary>
    public DbInvocationStyle InvocationStyle { get; init; } = DbInvocationStyle.Call;

    /// <summary>
    /// EN: Gets or sets the temporal kind associated with the scalar function.
    /// PT-br: Obtem ou define o tipo temporal associado a funcao escalar.
    /// </summary>
    public int TemporalKind { get; init; } = -1;

    /// <summary>
    /// EN: Gets or sets the minimum version supported by the scalar function mapping.
    /// PT-br: Obtem ou define a versao minima suportada pelo mapeamento da funcao escalar.
    /// </summary>
    public int MinVersion { get; init; }
}

