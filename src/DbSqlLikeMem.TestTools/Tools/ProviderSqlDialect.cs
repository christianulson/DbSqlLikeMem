namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides provider-specific SQL snippets used by the benchmark session workflows.
/// PT-br: Fornece trechos SQL específicos do provedor usados pelos fluxos das sessões de benchmark.
/// </summary>
public abstract class ProviderSqlDialect
{
    /// <summary>
    /// EN: Gets the provider identifier for the dialect.
    /// PT: Obtem o identificador do provedor para o dialeto.
    /// </summary>
    public abstract ProviderId Provider { get; }

    /// <summary>
    /// EN: Gets the display name used for the provider in logs and benchmark output.
    /// PT: Obtem o nome exibido do provedor em logs e saida de benchmark.
    /// </summary>
    public abstract string DisplayName { get; }

    /// <summary>
    /// EN: Indicates whether the provider supports upsert statements in the benchmark flow.
    /// PT: Indica se o provedor suporta instrucoes de upsert no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsUpsert => false;

    /// <summary>
    /// EN: Indicates whether the provider supports sequences in the benchmark flow.
    /// PT: Indica se o provedor suporta sequencias no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsSequence => false;

    /// <summary>
    /// EN: Indicates whether the provider supports string aggregation in the benchmark flow.
    /// PT: Indica se o provedor suporta agregacao de strings no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsStringAggregate => true;

    /// <summary>
    /// EN: Indicates whether the provider supports savepoints in the benchmark flow.
    /// PT: Indica se o provedor suporta savepoints no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsSavepoints => true;

    /// <summary>
    /// EN: Indicates whether the provider supports JSON scalar reads in the benchmark flow.
    /// PT: Indica se o provedor suporta leitura escalar de JSON no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsJsonScalarRead => false;

    /// <summary>
    /// EN: Returns the CREATE TABLE statement for the users table.
    /// PT: Retorna a instrucao CREATE TABLE para a tabela de usuarios.
    /// </summary>
    public abstract string CreateUsersTable(string tableName, string uId);

    /// <summary>
    /// EN: Returns the CREATE TABLE statement for the orders table.
    /// PT: Retorna a instrucao CREATE TABLE para a tabela de pedidos.
    /// </summary>
    public abstract string CreateOrdersTable(string tableName, string usersTableName, string uId);

    /// <summary>
    /// EN: Returns the INSERT statement used to add a single user row.
    /// PT: Retorna a instrucao INSERT usada para adicionar uma linha de usuario.
    /// </summary>
    public abstract string InsertUser(string tableName, int id, string name);

    /// <summary>
    /// EN: Returns the INSERT statement used by the returning benchmark when the provider supports returning rows.
    /// PT-br: Retorna a instrução INSERT usada pelo benchmark de returning quando o provider suporta linhas retornadas.
    /// </summary>
    public virtual string InsertUserReturning(string tableName, int id, string name) => InsertUser(tableName, id, name);

    /// <summary>
    /// EN: Returns the INSERT statement used to add multiple user rows.
    /// PT: Retorna a instrucao INSERT usada para adicionar multiplas linhas de usuario.
    /// </summary>
    public abstract string InsertUsers(string tableName, params (int id, string name)[] values);

    /// <summary>
    /// EN: Returns the INSERT statement used to add an order row.
    /// PT: Retorna a instrucao INSERT usada para adicionar uma linha de pedido.
    /// </summary>
    public abstract string InsertOrder(string tableName, string usersTableName, int id, int userId, string note);

    /// <summary>
    /// EN: Returns the SELECT statement used to read a user name by primary key.
    /// PT: Retorna a instrucao SELECT usada para ler um nome de usuario pela chave primaria.
    /// </summary>
    public abstract string SelectUserNameById(string tableName, int id);

    /// <summary>
    /// EN: Returns the SELECT statement used to count joined user and order rows.
    /// PT: Retorna a instrucao SELECT usada para contar linhas combinadas de usuarios e pedidos.
    /// </summary>
    public abstract string CountJoinForUser(string usersTable, string ordersTable, int userId);

    /// <summary>
    /// EN: Returns the UPDATE statement used to change a user name by primary key.
    /// PT: Retorna a instrucao UPDATE usada para alterar o nome de um usuario pela chave primaria.
    /// </summary>
    public abstract string UpdateUserNameById(string tableName, int id, string newName);

    /// <summary>
    /// EN: Returns the DELETE statement used to remove a user by primary key.
    /// PT: Retorna a instrucao DELETE usada para remover um usuario pela chave primaria.
    /// </summary>
    public abstract string DeleteUserById(string tableName, int id);

    /// <summary>
    /// EN: Returns the SELECT statement used to count rows in a table.
    /// PT: Retorna a instrucao SELECT usada para contar linhas em uma tabela.
    /// </summary>
    public abstract string CountRows(string tableName);

    /// <summary>
    /// EN: Returns the scalar SQL statement used for the provider date/time benchmark.
    /// PT: Retorna a instrucao SQL escalar usada no benchmark de data/hora do provedor.
    /// </summary>
    public abstract string DateScalar();

    /// <summary>
    /// EN: Returns the SQL statement used for the string aggregation benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de agregacao de strings.
    /// </summary>
    public abstract string StringAggregate(string tableName);

    /// <summary>
    /// EN: Returns the SQL statement used for ordered string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao ordenada de strings.
    /// </summary>
    public virtual string StringAggregateOrdered(string tableName) => StringAggregate(tableName);

    /// <summary>
    /// EN: Returns the SQL statement used for distinct string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao distinta de strings.
    /// </summary>
    public virtual string StringAggregateDistinct(string tableName) => StringAggregateOrdered(tableName);

    /// <summary>
    /// EN: Returns the SQL statement used for custom-separator string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao de strings com separador customizado.
    /// </summary>
    public virtual string StringAggregateCustomSeparator(string tableName, string separator) => StringAggregateOrdered(tableName);

    /// <summary>
    /// EN: Returns the SQL statement used for large-group string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao de strings em grupo grande.
    /// </summary>
    public virtual string StringAggregateLargeGroup(string tableName) => StringAggregateOrdered(tableName);

    /// <summary>
    /// EN: Returns the SQL statement used for the upsert benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de upsert.
    /// </summary>
    public virtual string Upsert(string tableName, int id, string newName) =>
        throw new NotSupportedException($"{DisplayName} does not support the configured upsert benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to create a sequence.
    /// PT: Retorna a instrucao SQL usada para criar uma sequencia.
    /// </summary>
    public virtual string CreateSequence(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to read the next sequence value.
    /// PT: Retorna a instrucao SQL usada para ler o proximo valor da sequencia.
    /// </summary>
    public virtual string NextSequenceValue(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to create a savepoint.
    /// PT: Retorna a instrucao SQL usada para criar um savepoint.
    /// </summary>
    public virtual string Savepoint(string savepointName) => $"SAVEPOINT {savepointName}";

    /// <summary>
    /// EN: Returns the SQL statement used to roll back to a savepoint.
    /// PT: Retorna a instrucao SQL usada para desfazer ate um savepoint.
    /// </summary>
    public virtual string RollbackToSavepoint(string savepointName) => $"ROLLBACK TO SAVEPOINT {savepointName}";

    /// <summary>
    /// EN: Returns the SQL statement used to release a savepoint.
    /// PT: Retorna a instrucao SQL usada para liberar um savepoint.
    /// </summary>
    public virtual string ReleaseSavepoint(string savepointName) => $"RELEASE SAVEPOINT {savepointName}";

    /// <summary>
    /// EN: Returns the SQL statement used for the JSON scalar benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark escalar de JSON.
    /// </summary>
    public virtual string JsonScalarRead(string jsonLiteral) =>
        throw new NotSupportedException($"{DisplayName} does not support the JSON scalar benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used for the JSON path benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de caminho JSON.
    /// </summary>
    public virtual string JsonPathRead(string jsonLiteral) =>
        throw new NotSupportedException($"{DisplayName} does not support the JSON path benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used for the current timestamp benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de timestamp atual.
    /// </summary>
    public virtual string TemporalCurrentTimestamp() => DateScalar();

    /// <summary>
    /// EN: Returns the SQL statement used for the date-add benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de soma de data.
    /// </summary>
    public virtual string TemporalDateAdd() => throw new NotSupportedException($"{DisplayName} does not support the temporal DATEADD benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used for the current-time WHERE benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark com WHERE por tempo atual.
    /// </summary>
    public virtual string TemporalNowWhere(string tableName) => $"SELECT COUNT(*) FROM {tableName} WHERE 1 = 1";

    /// <summary>
    /// EN: Returns the SQL statement used for the current-time ORDER BY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark com ORDER BY por tempo atual.
    /// </summary>
    public virtual string TemporalNowOrderBy(string tableName) => $"SELECT Name FROM {tableName} ORDER BY Name";

    /// <summary>
    /// EN: Returns the SQL statement used for the CTE benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de CTE.
    /// </summary>
    public virtual string CteSimple(string tableName) => $"WITH cte AS (SELECT Name FROM {tableName} WHERE Id = 1) SELECT COUNT(*) FROM cte";

    /// <summary>
    /// EN: Returns the SQL statement used for the ROW_NUMBER benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de ROW_NUMBER.
    /// </summary>
    public virtual string WindowRowNumber(string tableName) => $"SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY Name) AS rn FROM {tableName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the LAG benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de LAG.
    /// </summary>
    public virtual string WindowLag(string tableName) => $"SELECT COUNT(*) FROM (SELECT LAG(Name) OVER (ORDER BY Id) AS PrevName FROM {tableName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the EXISTS benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de EXISTS.
    /// </summary>
    public virtual string SelectExistsPredicate(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE EXISTS (SELECT 1 FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id)";

    /// <summary>
    /// EN: Returns the SQL statement used for the correlated COUNT benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de COUNT correlacionado.
    /// </summary>
    public virtual string SelectCorrelatedCount(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id) > 0";

    /// <summary>
    /// EN: Returns the SQL statement used for the GROUP BY HAVING benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de GROUP BY HAVING.
    /// </summary>
    public virtual string GroupByHaving(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM (SELECT u.Id FROM {usersTable} u INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id GROUP BY u.Id HAVING COUNT(*) >= 2) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the UNION ALL projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projecao UNION ALL.
    /// </summary>
    public virtual string UnionAllProjection(string tableName) =>
        $"SELECT COUNT(*) FROM (SELECT Name FROM {tableName} WHERE Id = 1 UNION ALL SELECT Name FROM {tableName} WHERE Id = 2) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the DISTINCT projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projecao DISTINCT.
    /// </summary>
    public virtual string DistinctProjection(string tableName) =>
        $"SELECT COUNT(*) FROM (SELECT DISTINCT Name FROM {tableName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the multi-join aggregate benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de agregacao com multiplos joins.
    /// </summary>
    public virtual string MultiJoinAggregate(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o1 ON o1.{usersTable}Id = u.Id INNER JOIN {ordersTable} o2 ON o2.{usersTable}Id = u.Id WHERE u.Id = 1";

    /// <summary>
    /// EN: Returns the SQL statement used for the scalar subquery benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de subconsulta escalar.
    /// </summary>
    public virtual string SelectScalarSubquery(string usersTable, string ordersTable) =>
        $"SELECT (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = 1)";

    /// <summary>
    /// EN: Returns the SQL statement used for the IN subquery benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de subconsulta IN.
    /// </summary>
    public virtual string SelectInSubquery(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} WHERE Id IN (SELECT {usersTable}Id FROM {ordersTable})";

    /// <summary>
    /// EN: Returns the SQL statement used for the CROSS APPLY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de CROSS APPLY.
    /// </summary>
    public virtual string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE EXISTS (SELECT 1 FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id)";

    /// <summary>
    /// EN: Returns the SQL statement used for the OUTER APPLY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de OUTER APPLY.
    /// </summary>
    public virtual string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u";

    /// <summary>
    /// EN: Returns the DROP TABLE statement for the users or orders table variant.
    /// PT: Retorna a instrucao DROP TABLE para a variacao da tabela de usuarios ou pedidos.
    /// </summary>
    public virtual string DropTable(string tableName, string uId) => $"DROP TABLE {tableName}_{uId}";

    /// <summary>
    /// EN: Returns the DROP SEQUENCE statement for a sequence.
    /// PT: Retorna a instrucao DROP SEQUENCE para uma sequencia.
    /// </summary>
    public virtual string DropSequence(string sequenceName) => $"DROP SEQUENCE {sequenceName}";
}

