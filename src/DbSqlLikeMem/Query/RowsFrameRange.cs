namespace DbSqlLikeMem;

internal readonly record struct RowsFrameRange(int StartIndex, int EndIndex, bool IsEmpty)
{
    public static RowsFrameRange Empty => new(0, -1, IsEmpty: true);
}
