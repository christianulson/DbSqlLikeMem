using System.Collections.Generic;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Trigger moments supported by in-memory tables.
/// PT: Momentos de trigger suportados pelas tabelas em memória.
/// </summary>
public enum TableTriggerEvent
{
    BeforeInsert,
    AfterInsert,
    BeforeUpdate,
    AfterUpdate,
    BeforeDelete,
    AfterDelete
}

/// <summary>
/// EN: Trigger callback payload.
/// PT: Payload do callback de trigger.
/// </summary>
/// <param name="Table">EN: Trigger table. PT: Tabela da trigger.</param>
/// <param name="OldRow">EN: Previous row values. PT: Valores anteriores da linha.</param>
/// <param name="NewRow">EN: New row values. PT: Novos valores da linha.</param>
public sealed record TableTriggerContext(
    ITableMock Table,
    IReadOnlyDictionary<int, object?>? OldRow,
    IReadOnlyDictionary<int, object?>? NewRow);
