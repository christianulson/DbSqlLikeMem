using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents a mock database command used to execute SQL text and stored procedures in memory.
/// PT: Representa um comando de banco de dados simulado usado para executar SQL e procedures em memória.
/// </summary>
public class SqlAzureCommandMock(
    SqlAzureConnectionMock? connection,
    SqlServerTransactionMock? transaction = null
    ) : SqlServerCommandMock(connection, transaction)
{
    private readonly SqlAzureDataParameterCollectionMock collectionMock = [];

    /// <summary>
    /// EN: Creates an empty SQL Azure command mock without connection and transaction.
    /// PT: Cria um comando simulado vazio do SQL Azure sem conexao e sem transacao.
    /// </summary>
    public SqlAzureCommandMock() : this(null, null)
    {
    }

    /// <summary>
    /// EN: Gets the parameter collection used by this SQL Azure command mock.
    /// PT: Obtem a colecao de parametros usada por este comando simulado do SQL Azure.
    /// </summary>
    protected override DbParameterCollection DbParameterCollection => collectionMock;
}