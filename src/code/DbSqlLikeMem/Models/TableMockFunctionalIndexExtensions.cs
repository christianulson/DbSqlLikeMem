namespace DbSqlLikeMem;

/// <summary>
/// EN: Extension methods for setting up functional (expression-based) index columns on ITableMock.
/// PT-br: Metodos de extensao para configurar colunas de indice funcional (baseado em expressao) em ITableMock.
/// </summary>
public static class TableMockFunctionalIndexExtensions
{
    /// <summary>
    /// EN: Adds a hidden computed column that evaluates the given SQL expression at index-build time.
    ///     The column is marked as non-persisted (PersistComputedValue = false), matching MySQL/MariaDB
    ///     functional index semantics.
    /// PT-br: Adiciona uma coluna computada oculta que avalia a expressao SQL informada no momento da
    ///        construcao do indice. A coluna e marcada como nao persistida (PersistComputedValue = false),
    ///        seguindo a semantica de indices funcionais do MySQL/MariaDB.
    /// </summary>
    /// <param name="table">EN: Target table. PT-br: Tabela alvo.</param>
    /// <param name="columnName">EN: Name for the hidden column (e.g. __func_idx_0__). PT-br: Nome da coluna oculta.</param>
    /// <param name="expression">EN: SQL expression text. PT-br: Texto da expressao SQL.</param>
    /// <param name="db">EN: Database mock instance used for dialect resolution. PT-br: Instancia do banco simulado para resolucao do dialeto.</param>
    /// <returns>EN: The created ColumnDef with GetGenValue set. PT-br: O ColumnDef criado com GetGenValue configurado.</returns>
    public static ColumnDef AddFunctionalIndexColumn(
        this ITableMock table,
        string columnName,
        string expression,
        DbMock db)
    {
        var col = table.AddColumn(columnName, DbType.Object, nullable: true);
        col.GetGenValue = FunctionalIndexExpressionEvaluator.CreateGetGenValue(
            expression, db, db.Dialect, table);
        col.PersistComputedValue = false;
        return col;
    }
}
