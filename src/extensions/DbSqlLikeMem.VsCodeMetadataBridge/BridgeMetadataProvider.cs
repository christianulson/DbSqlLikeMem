using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VsCodeMetadataBridge;

internal sealed class BridgeMetadataProvider(
    BridgeSqlQueryExecutor queryExecutor)
{
    private readonly SqlDatabaseMetadataProvider _provider = new(queryExecutor);

    public async Task<IReadOnlyList<BridgeDatabaseObjectReference>> GetObjectsAsync(
        ConnectionDefinition connection,
        CancellationToken cancellationToken)
    {
        var objects = await _provider.ListObjectsAsync(connection, cancellationToken);
        var result = new List<BridgeDatabaseObjectReference>(objects.Count);

        foreach (var reference in objects)
        {
            DatabaseObjectReference detailed;
            try
            {
                detailed = await _provider.GetObjectDetailsAsync(connection, reference, cancellationToken) ?? reference;
            }
            catch
            {
                detailed = reference;
            }

            result.Add(BridgeMetadataMapper.ToBridgeObject(detailed));
        }

        return result;
    }

    public Task TestConnectionAsync(ConnectionDefinition connection, CancellationToken cancellationToken)
        => queryExecutor.TestConnectionAsync(connection, cancellationToken);
}
