namespace DbSqlLikeMem;

internal enum TableMutationKind
{
    Insert,
    Update,
    Delete
}

internal sealed record TableMutationNotification(
    TableMock Table,
    TableMutationKind Kind,
    int RowIndex,
    object?[] Row,
    object?[]? OldRowSnapshot,
    int PreviousNextIdentity);
