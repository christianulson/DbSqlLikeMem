namespace DbSqlLikeMem;
internal abstract record SqlExpr;
internal sealed record IdentifierExpr(string Name) : SqlExpr;              // "col" ou "alias.col"
internal sealed record ColumnExpr(string Qualifier, string Name) : SqlExpr;

// fallback se vier db.tbl.col (3 partes) e você não quer modelar agora
internal sealed record RawSqlExpr(string Sql) : SqlExpr;
internal sealed record LiteralExpr(object? Value) : SqlExpr;              // null, bool, decimal, string
internal sealed record ParameterExpr(string Name) : SqlExpr;              // @p1, :x, ?
internal sealed record UnaryExpr(SqlUnaryOp Op, SqlExpr Expr) : SqlExpr;  // NOT x
internal sealed record BinaryExpr(SqlBinaryOp Op, SqlExpr Left, SqlExpr Right) : SqlExpr; // AND/OR e comparações
internal sealed record InExpr(SqlExpr Left, IReadOnlyList<SqlExpr> Items) : SqlExpr;
internal sealed record LikeExpr(SqlExpr Left, SqlExpr Pattern) : SqlExpr;
internal sealed record IsNullExpr(SqlExpr Expr, bool Negated) : SqlExpr;  // IS NULL / IS NOT NULL
internal sealed record SubqueryExpr(string Sql, SqlSelectQuery Parsed) : SqlExpr;
internal sealed record RowExpr(IReadOnlyList<SqlExpr> Items) : SqlExpr;
internal sealed record ExistsExpr(SubqueryExpr Subquery) : SqlExpr;
internal sealed record FunctionCallExpr(string Name, IReadOnlyList<SqlExpr> Args) : SqlExpr;
internal sealed record JsonAccessExpr(SqlExpr Target, SqlExpr Path, bool Unquote) : SqlExpr;
internal sealed record CallExpr(string Name, IReadOnlyList<SqlExpr> Args, bool Distinct = false) : SqlExpr;
internal sealed record WindowFunctionExpr(string Name, IReadOnlyList<SqlExpr> Args, WindowSpec Spec, bool Distinct = false) : SqlExpr;
internal sealed record WindowSpec(
    IReadOnlyList<SqlExpr> PartitionBy,
    IReadOnlyList<WindowOrderItem> OrderBy,
    WindowFrameSpec? Frame = null);
internal sealed record WindowOrderItem(SqlExpr Expr, bool Desc);
internal enum WindowFrameUnit { Rows, Range, Groups }
internal enum WindowFrameBoundKind { UnboundedPreceding, Preceding, CurrentRow, Following, UnboundedFollowing }
internal sealed record WindowFrameBound(WindowFrameBoundKind Kind, int? Offset);
internal sealed record WindowFrameSpec(WindowFrameUnit Unit, WindowFrameBound Start, WindowFrameBound End);
internal sealed record BetweenExpr(
    SqlExpr Expr,
    SqlExpr Low,
    SqlExpr High,
    bool Negated) : SqlExpr; // [NOT] BETWEEN

// CASE [base] WHEN ... THEN ... [ELSE ...] END
internal sealed record CaseExpr(
    SqlExpr? BaseExpr,
    IReadOnlyList<CaseWhenThen> Whens,
    SqlExpr? ElseExpr) : SqlExpr;

internal sealed record CaseWhenThen(SqlExpr When, SqlExpr Then);

internal sealed record StarExpr() : SqlExpr;
/// <summary>
/// EN: Unary operators represented in the SQL AST.
/// PT: Operadores unários representados na AST SQL.
/// </summary>
internal enum SqlUnaryOp { Not }
/// <summary>
/// EN: Binary operators represented in the SQL AST.
/// PT: Operadores binários representados na AST SQL.
/// </summary>
internal enum SqlBinaryOp
{
    And, Or,

    // arithmetic
    Add, Subtract, Multiply, Divide,

    // comparisons
    Eq, Neq, Greater, GreaterOrEqual, Less, LessOrEqual,
    NullSafeEq, // ✅ <=> (MySQL)

    // pattern / misc
    Regexp
}
