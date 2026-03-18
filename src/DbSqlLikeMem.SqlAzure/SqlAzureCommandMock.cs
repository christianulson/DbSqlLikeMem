using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSqlLikeMem.SqlServer;
namespace DbSqlLikeMem.SqlAzure;

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