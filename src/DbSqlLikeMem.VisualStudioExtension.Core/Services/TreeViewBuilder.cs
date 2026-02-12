using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class TreeViewBuilder
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public TreeNode Build(ConnectionDefinition connection, IReadOnlyCollection<DatabaseObjectReference> objects)
    {
        var root = new TreeNode(connection.DatabaseType) { ContextKey = "database-type" };
        var dbNode = new TreeNode(connection.DatabaseName) { ContextKey = "database-name" };
        root.Children.Add(dbNode);

        foreach (DatabaseObjectType objectType in Enum.GetValues(typeof(DatabaseObjectType)))
        {
            var typeNode = new TreeNode(GetGroupLabel(objectType)) { ContextKey = "object-type", ObjectType = objectType };

            foreach (var item in objects.Where(o => o.Type == objectType).OrderBy(o => o.Name, StringComparer.OrdinalIgnoreCase))
            {
                typeNode.Children.Add(new TreeNode(item.Name)
                {
                    ContextKey = "object",
                    ObjectType = objectType
                });
            }

            dbNode.Children.Add(typeNode);
        }

        return root;
    }

    private static string GetGroupLabel(DatabaseObjectType type) => type switch
    {
        DatabaseObjectType.Table => "Tables",
        DatabaseObjectType.View => "Views",
        DatabaseObjectType.Procedure => "Procedures",
        _ => type.ToString()
    };
}
