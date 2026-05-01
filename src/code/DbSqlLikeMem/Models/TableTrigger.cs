namespace DbSqlLikeMem;

/// <summary>
/// EN: Trigger moments supported by in-memory tables.
/// PT-br: Momentos de trigger suportados pelas tabelas em memória.
/// </summary>
public enum TableTriggerEvent
{
    /// <summary>
    /// EN: Fired before inserting a row.
    /// PT-br: Disparado antes de inserir uma linha.
    /// </summary>
    BeforeInsert,

    /// <summary>
    /// EN: Fired after inserting a row.
    /// PT-br: Disparado após inserir uma linha.
    /// </summary>
    AfterInsert,

    /// <summary>
    /// EN: Fired before updating a row.
    /// PT-br: Disparado antes de atualizar uma linha.
    /// </summary>
    BeforeUpdate,

    /// <summary>
    /// EN: Fired after updating a row.
    /// PT-br: Disparado após atualizar uma linha.
    /// </summary>
    AfterUpdate,

    /// <summary>
    /// EN: Fired before deleting a row.
    /// PT-br: Disparado antes de remover uma linha.
    /// </summary>
    BeforeDelete,

    /// <summary>
    /// EN: Fired after deleting a row.
    /// PT-br: Disparado após remover uma linha.
    /// </summary>
    AfterDelete
}

/// <summary>
/// EN: Trigger callback payload.
/// PT-br: Payload do callback de trigger.
/// </summary>
/// <param name="Table">EN: Trigger table. PT-br: Tabela da trigger.</param>
/// <param name="OldRow">EN: Previous row values. PT-br: Valores anteriores da linha.</param>
/// <param name="NewRow">EN: New row values. PT-br: Novos valores da linha.</param>
public sealed record TableTriggerContext(
    ITableMock Table,
    IReadOnlyDictionary<int, object?>? OldRow,
    IReadOnlyDictionary<int, object?>? NewRow);
