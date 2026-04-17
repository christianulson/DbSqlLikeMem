namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Describes provider-specific SQL snippets used by the benchmark session workflows.
/// PT-br: Descreve trechos SQL especificos do provedor usados pelos fluxos das sessoes de benchmark.
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
    /// EN: Indicates whether global temporary table definitions are visible across connections.
    /// PT: Indica se as definicoes de tabelas temporarias globais ficam visiveis entre conexoes.
    /// </summary>
    public virtual bool GlobalTemporaryTablesShareDefinitionAcrossConnections => false;

    /// <summary>
    /// EN: Indicates whether global temporary table rows are visible across connections.
    /// PT: Indica se as linhas de tabelas temporarias globais ficam visiveis entre conexoes.
    /// </summary>
    public virtual bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <summary>
    /// EN: Indicates whether the provider supports releasing savepoints in the benchmark flow.
    /// PT: Indica se o provedor suporta liberar savepoints no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsReleaseSavepoints => true;

    /// <summary>
    /// EN: Indicates whether the provider supports JSON scalar reads in the benchmark flow.
    /// PT: Indica se o provedor suporta leitura escalar de JSON no fluxo de benchmark.
    /// </summary>
    public virtual bool SupportsJsonScalarRead => false;

    /// <summary>
    /// EN: Returns the CREATE TABLE statement for the users table.
    /// PT: Retorna a instrucao CREATE TABLE para a tabela de usuarios.
    /// </summary>
    public abstract string CreateUsersTable(FidelityTestContext context);

    /// <summary>
    /// EN: Returns the provider-specific temporary users table name used by rollback and isolation workflows.
    /// PT: Retorna o nome da tabela temporaria de usuarios especifico do provedor usado pelos fluxos de rollback e isolamento.
    /// </summary>
    public virtual string TemporaryUsersTableName(FidelityTestContext context) => context.TempTbFullName ?? throw new ArgumentException(nameof(context.TempTbFullName));

    /// <summary>
    /// EN: Returns the CREATE statement for the temporary users table used by rollback and isolation workflows.
    /// PT: Retorna a instrucao CREATE para a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
    /// </summary>
    public virtual string CreateTemporaryUsersTable(FidelityTestContext context) =>
        $@"
CREATE TEMPORARY TABLE {TemporaryUsersTableName(context)} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    TenantId INT NOT NULL
)";

    /// <summary>
    /// EN: Returns the INSERT statement for the temporary users table used by rollback and isolation workflows.
    /// PT: Retorna a instrucao INSERT para a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
    /// </summary>
    public virtual string InsertTemporaryUsersTable(FidelityTestContext context, int id, string name, int tenantId) =>
        $"INSERT INTO {TemporaryUsersTableName(context)} (Id, Name, TenantId) VALUES ({id}, '{name}', {tenantId})";

    /// <summary>
    /// EN: Returns the DROP statement for the temporary users table used by rollback and isolation workflows.
    /// PT: Retorna a instrucao DROP para a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
    /// </summary>
    public virtual string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE {TemporaryUsersTableName(context)}";

    /// <summary>
    /// EN: Returns the SQL parameter placeholder for the specified parameter name.
    /// PT: Retorna o marcador de parametro SQL para o nome de parametro especificado.
    /// </summary>
    public virtual string Parameter(string name) =>
        $"@{name}";

    /// <summary>
    /// EN: Returns the SQL parameter placeholder used for a JSON value bound to a JSON-capable column.
    /// PT: Retorna o marcador de parametro SQL usado para um valor JSON vinculado a uma coluna com suporte a JSON.
    /// </summary>
    public virtual string JsonParameter(string name) =>
        Parameter(name);

    /// <summary>
    /// EN: Returns a scalar SELECT projection statement for parameter roundtrip tests.
    /// PT: Retorna uma instrucao SELECT escalar para testes de roundtrip de parametros.
    /// </summary>
    public virtual string SelectParameterProjection(string projectionList) =>
        $"SELECT {projectionList}";

    /// <summary>
    /// EN: Returns the CREATE TABLE statement for the orders table.
    /// PT: Retorna a instrucao CREATE TABLE para a tabela de pedidos.
    /// </summary>
    public abstract string CreateOrdersTable(FidelityTestContext context);

    /// <summary>
    /// EN: Returns the INSERT statement used to add a single user row.
    /// PT: Retorna a instrucao INSERT usada para adicionar uma linha de usuario.
    /// </summary>
    public abstract string InsertUser(FidelityTestContext context, int id, string name);

    /// <summary>
    /// EN: Returns the INSERT statement used by the returning benchmark when the provider supports returning rows.
    /// PT-br: Retorna a instrução INSERT usada pelo benchmark de returning quando o provider suporta linhas retornadas.
    /// </summary>
    public virtual string InsertUserReturning(FidelityTestContext context, int id, string name) => InsertUser(context, id, name);

    /// <summary>
    /// EN: Returns the INSERT statement used to add multiple user rows.
    /// PT: Retorna a instrucao INSERT usada para adicionar multiplas linhas de usuario.
    /// </summary>
    public abstract string InsertUsers(FidelityTestContext context, params (int id, string name)[] values);

    /// <summary>
    /// EN: Returns the INSERT statement used to add an order row.
    /// PT: Retorna a instrucao INSERT usada para adicionar uma linha de pedido.
    /// </summary>
    public abstract string InsertOrder(
        FidelityTestContext context,
        int id,
        int userId,
        string note,
        string orderNumber,
        decimal amount,
        int quantity,
        bool isPaid,
        string orderedAtLiteral);

    /// <summary>
    /// EN: Returns the SELECT statement used to read a user name by primary key.
    /// PT: Retorna a instrucao SELECT usada para ler um nome de usuario pela chave primaria.
    /// </summary>
    public abstract string SelectUserNameById(FidelityTestContext context, int id);

    /// <summary>
    /// EN: Returns the SELECT statement used to count joined user and order rows.
    /// PT: Retorna a instrucao SELECT usada para contar linhas combinadas de usuarios e pedidos.
    /// </summary>
    public abstract string CountJoinForUser(FidelityTestContext context, int userId);

    /// <summary>
    /// EN: Returns the UPDATE statement used to change a user name by primary key.
    /// PT: Retorna a instrucao UPDATE usada para alterar o nome de um usuario pela chave primaria.
    /// </summary>
    public abstract string UpdateUserNameById(FidelityTestContext context, int id, string newName);

    /// <summary>
    /// EN: Returns the DELETE statement used to remove a user by primary key.
    /// PT: Retorna a instrucao DELETE usada para remover um usuario pela chave primaria.
    /// </summary>
    public abstract string DeleteUserById(FidelityTestContext context, int id);

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
    public abstract string StringAggregate(FidelityTestContext context);

    /// <summary>
    /// EN: Returns the SQL statement used for ordered string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao ordenada de strings.
    /// </summary>
    public virtual string StringAggregateOrdered(FidelityTestContext context) => StringAggregate(context);

    /// <summary>
    /// EN: Returns the SQL statement used for distinct string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao distinta de strings.
    /// </summary>
    public virtual string StringAggregateDistinct(FidelityTestContext context) => StringAggregateOrdered(context);

    /// <summary>
    /// EN: Returns the SQL statement used for custom-separator string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao de strings com separador customizado.
    /// </summary>
    public virtual string StringAggregateCustomSeparator(FidelityTestContext context, string separator) => StringAggregateOrdered(context);

    /// <summary>
    /// EN: Returns the SQL statement used for large-group string aggregation.
    /// PT: Retorna a instrucao SQL usada para agregacao de strings em grupo grande.
    /// </summary>
    public virtual string StringAggregateLargeGroup(FidelityTestContext context) => StringAggregateOrdered(context);

    /// <summary>
    /// EN: Returns the SQL statement used for the upsert benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de upsert.
    /// </summary>
    public virtual string Upsert(FidelityTestContext context, int id, string newName) =>
        throw new NotSupportedException($"{DisplayName} does not support the configured upsert benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to create a sequence.
    /// PT: Retorna a instrucao SQL usada para criar uma sequencia.
    /// </summary>
    public virtual string CreateSequence(FidelityTestContext context) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to read the next sequence value.
    /// PT: Retorna a instrucao SQL usada para ler o proximo valor da sequencia.
    /// </summary>
    public virtual string NextSequenceValue(FidelityTestContext context) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL expression used to consume the next sequence value inside an INSERT or SELECT.
    /// PT: Retorna a expressao SQL usada para consumir o proximo valor da sequence dentro de um INSERT ou SELECT.
    /// </summary>
    public virtual string NextSequenceValueExpression(FidelityTestContext context) =>
        throw new NotSupportedException($"{DisplayName} does not support sequence expressions in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to read the current sequence value.
    /// PT: Retorna a instrucao SQL usada para ler o valor corrente da sequence.
    /// </summary>
    public virtual string CurrentSequenceValue(FidelityTestContext context) =>
        throw new NotSupportedException($"{DisplayName} does not support current sequence values in this benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used to project the next sequence value inside a SELECT.
    /// PT: Retorna a instrucao SQL usada para projetar o proximo valor da sequence dentro de um SELECT.
    /// </summary>
    public virtual string SelectNextSequenceValue(FidelityTestContext context) =>
        $"SELECT {NextSequenceValueExpression(context)} AS SeqValue";

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
    /// EN: Returns the SQL expression used to read the profile name from a JSON column.
    /// PT: Retorna a expressao SQL usada para ler o nome do perfil de uma coluna JSON.
    /// </summary>
    public virtual string JsonProfileNameExpression(string jsonColumn) =>
        throw new NotSupportedException($"{DisplayName} does not support JSON column extraction.");

    /// <summary>
    /// EN: Returns the SQL expression used to extract a left substring from an expression.
    /// PT: Retorna a expressao SQL usada para extrair um prefixo textual de uma expressao.
    /// </summary>
    public virtual string StringPrefixExpression(string expression, int length) =>
        $"SUBSTRING({expression}, 1, {length})";

    /// <summary>
    /// EN: Returns the SQL expression used to measure the length of a string expression.
    /// PT: Retorna a expressao SQL usada para medir o comprimento de uma expressao de texto.
    /// </summary>
    public virtual string StringLengthExpression(string expression) =>
        $"LENGTH({expression})";

    /// <summary>
    /// EN: Returns the SQL expression used to cast a value to text for fidelity checks.
    /// PT: Retorna a expressao SQL usada para converter um valor em texto nos testes de fidelidade.
    /// </summary>
    public virtual string StringCastExpression(string expression, int length = 10) =>
        $"CAST({expression} AS CHAR({length}))";

    /// <summary>
    /// EN: Returns the SQL expression used to cast a value to integer for fidelity checks.
    /// PT: Retorna a expressao SQL usada para converter um valor em inteiro nos testes de fidelidade.
    /// </summary>
    public virtual string IntCastExpression(string expression) =>
        $"CAST({expression} AS INT)";

    /// <summary>
    /// EN: Returns the SQL statement used for the current timestamp benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de timestamp atual.
    /// </summary>
    public virtual string TemporalCurrentTimestamp() => DateScalar();

    /// <summary>
    /// EN: Returns the SQL expression used for the current timestamp inside a SELECT projection.
    /// PT: Retorna a expressao SQL usada para timestamp atual dentro de uma projeção SELECT.
    /// </summary>
    public virtual string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <summary>
    /// EN: Returns the SQL statement used for the date-add benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de soma de data.
    /// </summary>
    public virtual string TemporalDateAdd() => throw new NotSupportedException($"{DisplayName} does not support the temporal DATEADD benchmark.");

    /// <summary>
    /// EN: Returns the SQL expression used to add one day inside a SELECT projection.
    /// PT: Retorna a expressao SQL usada para somar um dia dentro de uma projeção SELECT.
    /// </summary>
    public virtual string TemporalDateAddExpression() => throw new NotSupportedException($"{DisplayName} does not support the temporal DATEADD expression benchmark.");

    /// <summary>
    /// EN: Returns the SQL statement used for the current-time WHERE benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark com WHERE por tempo atual.
    /// </summary>
    public virtual string TemporalNowWhere(FidelityTestContext context) => $"SELECT * FROM {context.TbUsersFullName} WHERE 1 = 1";

    /// <summary>
    /// EN: Returns the SQL statement used for the current-time ORDER BY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark com ORDER BY por tempo atual.
    /// </summary>
    public virtual string TemporalNowOrderBy(FidelityTestContext context) => $"SELECT Name FROM {context.TbUsersFullName} ORDER BY Name";

    /// <summary>
    /// EN: Returns the SQL statement used for the paged name projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projeção paginada de nomes.
    /// </summary>
    public virtual string PagedNameProjection(string tableName, int offset, int fetch) =>
        $"SELECT Name FROM {tableName} ORDER BY Name OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY";

    /// <summary>
    /// EN: Returns the SQL statement used for the CTE benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de CTE.
    /// </summary>
    public virtual string CteSimple(FidelityTestContext context) => $"WITH cte AS (SELECT Name FROM {context.TbUsersFullName} WHERE Id = 1) SELECT * FROM cte";

    /// <summary>
    /// EN: Returns the SQL statement used for the ROW_NUMBER benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de ROW_NUMBER.
    /// </summary>
    public virtual string WindowRowNumber(FidelityTestContext context) => $"SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY Name) AS rn FROM {context.TbUsersFullName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the LAG benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de LAG.
    /// </summary>
    public virtual string WindowLag(FidelityTestContext context) => $"SELECT * FROM (SELECT LAG(Name) OVER (ORDER BY Id) AS PrevName FROM {context.TbUsersFullName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the LEAD benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de LEAD.
    /// </summary>
    public virtual string WindowLead(FidelityTestContext context) => $"SELECT * FROM (SELECT LEAD(Name) OVER (ORDER BY Id) AS NextName FROM {context.TbUsersFullName}) q WHERE NextName IS NOT NULL";

    /// <summary>
    /// EN: Returns the SQL statement used for the EXISTS benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de EXISTS.
    /// </summary>
    public virtual string SelectExistsPredicate(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u WHERE EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)";

    /// <summary>
    /// EN: Returns the SQL statement used for the NOT EXISTS benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de NOT EXISTS.
    /// </summary>
    public virtual string SelectNotExistsPredicate(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u WHERE NOT EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)";

    /// <summary>
    /// EN: Returns the SQL statement used for the LEFT JOIN anti-join benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de anti-join com LEFT JOIN.
    /// </summary>
    public virtual string SelectLeftJoinAntiJoin(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u LEFT JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id WHERE o.{context.TbUsers}Id IS NULL";

    /// <summary>
    /// EN: Returns the SQL statement used for the correlated COUNT benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de COUNT correlacionado.
    /// </summary>
    public virtual string SelectCorrelatedCount(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u WHERE (SELECT COUNT(*) FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id) > 0";

    /// <summary>
    /// EN: Returns the SQL statement used for the GROUP BY HAVING benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de GROUP BY HAVING.
    /// </summary>
    public virtual string GroupByHaving(FidelityTestContext context) =>
        $"SELECT * FROM (SELECT u.Id FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id GROUP BY u.Id HAVING COUNT(*) >= 2) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the UNION ALL projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projecao UNION ALL.
    /// </summary>
    public virtual string UnionAllProjection(FidelityTestContext context) =>
        $"SELECT * FROM (SELECT Name FROM {context.TbUsersFullName} WHERE Id = 1 UNION ALL SELECT Name FROM {context.TbUsersFullName} WHERE Id = 2) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the UNION projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projecao UNION.
    /// </summary>
    public virtual string UnionDistinctProjection(FidelityTestContext context) =>
        $"SELECT * FROM (SELECT Name FROM {context.TbUsersFullName} WHERE Id IN (1, 2) UNION SELECT Name FROM {context.TbUsersFullName} WHERE Id IN (2, 3)) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the DISTINCT projection benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de projecao DISTINCT.
    /// </summary>
    public virtual string DistinctProjection(FidelityTestContext context) =>
        $"SELECT * FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) q";

    /// <summary>
    /// EN: Returns the SQL statement used for the multi-join aggregate benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de agregacao com multiplos joins.
    /// </summary>
    public virtual string MultiJoinAggregate(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o1 ON o1.{context.TbUsers}Id = u.Id INNER JOIN {context.TbOrdersFullName} o2 ON o2.{context.TbUsers}Id = u.Id WHERE u.Id = 1";

    /// <summary>
    /// EN: Returns the SQL statement used for the scalar subquery benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de subconsulta escalar.
    /// </summary>
    public virtual string SelectScalarSubquery(FidelityTestContext context) =>
        $"SELECT (SELECT * FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = 1)";

    /// <summary>
    /// EN: Returns the SQL statement used for the IN subquery benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de subconsulta IN.
    /// </summary>
    public virtual string SelectInSubquery(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} WHERE Id IN (SELECT {context.TbUsers}Id FROM {context.TbOrdersFullName})";

    /// <summary>
    /// EN: Returns the SQL statement used for the NOT IN subquery benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de subconsulta NOT IN.
    /// </summary>
    public virtual string SelectNotInSubquery(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} WHERE Id NOT IN (SELECT {context.TbUsers}Id FROM {context.TbOrdersFullName})";

    /// <summary>
    /// EN: Returns the SQL statement used for the SELECT * benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de SELECT *.
    /// </summary>
    public virtual string SelectAll(string usersTable) =>
        $"SELECT * FROM {usersTable}";

    /// <summary>
    /// EN: Returns the SQL statement used for the SELECT * with JOIN benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de SELECT * com JOIN.
    /// </summary>
    public virtual string SelectAllJoin(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id";

    /// <summary>
    /// EN: Returns the SQL statement used for the CROSS APPLY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de CROSS APPLY.
    /// </summary>
    public virtual string CrossApplyProjection(FidelityTestContext context) =>
        $"SELECT * FROM {context.TbUsersFullName} u WHERE EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)";

    /// <summary>
    /// EN: Returns the SQL statement used for the OUTER APPLY benchmark.
    /// PT: Retorna a instrucao SQL usada no benchmark de OUTER APPLY.
    /// </summary>
    public virtual string OuterApplyProjection(FidelityTestContext context) =>
        throw new NotSupportedException($"{DisplayName} does not support OUTER APPLY");

    /// <summary>
    /// EN: Returns the DROP TABLE statement for the users or orders table variant.
    /// PT: Retorna a instrucao DROP TABLE para a variacao da tabela de usuarios ou pedidos.
    /// </summary>
    public virtual string DropTable(string tableName) => $"DROP TABLE {tableName}";

    /// <summary>
    /// EN: Returns the DROP SEQUENCE statement for a sequence.
    /// PT: Retorna a instrucao DROP SEQUENCE para uma sequencia.
    /// </summary>
    public virtual string DropSequence(FidelityTestContext context) => $"DROP SEQUENCE {context.Seq}";
}
