namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers ExpressionReferencesInnerColumns and ExpressionUsesOnlyInnerColumnsOrConstants
///     via correlated subquery execution paths that exercise AstQueryInnerColumnAnalysisHelper.
/// PT-br: Cobre ExpressionReferencesInnerColumns e ExpressionUsesOnlyInnerColumnsOrConstants
///     via caminhos de subquery correlacionada que exercitam AstQueryInnerColumnAnalysisHelper.
/// </summary>
public sealed class AstQueryInnerColumnAnalysisHelperTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private static FirebirdConnectionMock CreateConnection()
    {
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        var cnn = new FirebirdConnectionMock(db);
        cnn.Open();
        return cnn;
    }

    private static object? Scalar(FirebirdConnectionMock cnn, string sql)
    {
        using var cmd = cnn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static void Exec(FirebirdConnectionMock cnn, string sql)
    {
        using var cmd = cnn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Verifies that a correlated EXISTS with a simple column predicate resolves correctly
    ///     through the inner-column analysis path.
    /// PT-br: Verifica que um EXISTS correlacionado com predicado de coluna simples resolve corretamente
    ///     pelo caminho de análise de coluna interna.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_SimpleColumnPredicate_ShouldReturnMatchingRows()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE dept (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "CREATE TABLE emp  (dept_id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO dept VALUES (1, 'Sales')");
        Exec(cnn, "INSERT INTO dept VALUES (2, 'IT')");
        Exec(cnn, "INSERT INTO emp  VALUES (1, 'Alice')");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a NOT EXISTS correlated subquery correctly excludes rows that have no inner match.
    /// PT-br: Verifica que NOT EXISTS correlacionado exclui corretamente linhas sem correspondência interna.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedNotExists_ShouldExcludeMatchedRows()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE dept (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "CREATE TABLE emp  (dept_id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO dept VALUES (1, 'Sales')");
        Exec(cnn, "INSERT INTO dept VALUES (2, 'IT')");
        Exec(cnn, "INSERT INTO emp  VALUES (1, 'Alice')");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept_id = d.id)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a correlated EXISTS with a qualified table alias resolves correctly.
    /// PT-br: Verifica que EXISTS correlacionado com alias de tabela qualificado resolve corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_WithQualifiedAlias_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE orders (customer_id INTEGER, total DECIMAL(10,2))");
        Exec(cnn, "CREATE TABLE customers (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO customers VALUES (10, 'Bob')");
        Exec(cnn, "INSERT INTO customers VALUES (20, 'Carol')");
        Exec(cnn, "INSERT INTO orders VALUES (10, 99.9)");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM customers c WHERE EXISTS " +
            "(SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 50)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a correlated subquery with multiple inner predicates (AND) resolves correctly.
    /// PT-br: Verifica que subquery correlacionada com múltiplos predicados internos (AND) resolve corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_WithBinaryAndPredicate_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE products (category_id INTEGER, price DECIMAL(10,2))");
        Exec(cnn, "CREATE TABLE categories (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO categories VALUES (1, 'Electronics')");
        Exec(cnn, "INSERT INTO categories VALUES (2, 'Books')");
        Exec(cnn, "INSERT INTO products VALUES (1, 500.0)");
        Exec(cnn, "INSERT INTO products VALUES (2, 15.0)");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM categories c WHERE EXISTS " +
            "(SELECT 1 FROM products p WHERE p.category_id = c.id AND p.price > 100)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies correlated EXISTS with a column-only expression (non-compound) correctly identifies inner columns.
    /// PT-br: Verifica que EXISTS correlacionado com expressão apenas de coluna (não composta) identifica corretamente colunas internas.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_WithLikeOnInnerColumn_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE tags (item_id INTEGER, tag VARCHAR(50))");
        Exec(cnn, "CREATE TABLE items (id INTEGER, label VARCHAR(50))");
        Exec(cnn, "INSERT INTO items VALUES (1, 'Widget')");
        Exec(cnn, "INSERT INTO items VALUES (2, 'Gadget')");
        Exec(cnn, "INSERT INTO tags VALUES (1, 'popular')");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM items i WHERE EXISTS " +
            "(SELECT 1 FROM tags t WHERE t.item_id = i.id AND t.tag LIKE 'pop%')");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies correlated subquery with BETWEEN on inner column resolves correctly.
    /// PT-br: Verifica que subquery correlacionada com BETWEEN na coluna interna resolve corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_WithBetweenOnInnerColumn_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE scores (student_id INTEGER, score INTEGER)");
        Exec(cnn, "CREATE TABLE students (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO students VALUES (1, 'Ana')");
        Exec(cnn, "INSERT INTO students VALUES (2, 'Ben')");
        Exec(cnn, "INSERT INTO scores VALUES (1, 85)");
        Exec(cnn, "INSERT INTO scores VALUES (2, 40)");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM students s WHERE EXISTS " +
            "(SELECT 1 FROM scores sc WHERE sc.student_id = s.id AND sc.score BETWEEN 70 AND 100)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies correlated subquery where the outer column is on the right side of the equality also resolves correctly.
    /// PT-br: Verifica que subquery correlacionada com coluna externa no lado direito da igualdade também resolve corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_OuterColumnOnRightSide_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE inv (pid INTEGER, qty INTEGER)");
        Exec(cnn, "CREATE TABLE prods (id INTEGER, name VARCHAR(50))");
        Exec(cnn, "INSERT INTO prods VALUES (5, 'Bolt')");
        Exec(cnn, "INSERT INTO prods VALUES (6, 'Nut')");
        Exec(cnn, "INSERT INTO inv VALUES (5, 10)");

        // outer column on right side: p.id = i.pid
        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM prods p WHERE EXISTS " +
            "(SELECT 1 FROM inv i WHERE p.id = i.pid)");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies correlated subquery with IN list on inner column resolves correctly.
    /// PT-br: Verifica que subquery correlacionada com lista IN na coluna interna resolve corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void CorrelatedExists_WithInListOnInnerColumn_ShouldResolveCorrectly()
    {
        using var cnn = CreateConnection();
        Exec(cnn, "CREATE TABLE reviews (book_id INTEGER, rating INTEGER)");
        Exec(cnn, "CREATE TABLE books (id INTEGER, title VARCHAR(80))");
        Exec(cnn, "INSERT INTO books VALUES (1, 'Learn SQL')");
        Exec(cnn, "INSERT INTO books VALUES (2, 'Advanced SQL')");
        Exec(cnn, "INSERT INTO reviews VALUES (1, 5)");
        Exec(cnn, "INSERT INTO reviews VALUES (2, 3)");

        var result = Scalar(cnn,
            "SELECT COUNT(*) FROM books b WHERE EXISTS " +
            "(SELECT 1 FROM reviews r WHERE r.book_id = b.id AND r.rating IN (4, 5))");

        Convert.ToInt32(result, CultureInfo.InvariantCulture).Should().Be(1);
    }
}
