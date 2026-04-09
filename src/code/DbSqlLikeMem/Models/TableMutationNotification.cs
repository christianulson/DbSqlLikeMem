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
    Dictionary<int, object?> Row,
    Dictionary<int, object?>? OldRowSnapshot,
    int PreviousNextIdentity);
