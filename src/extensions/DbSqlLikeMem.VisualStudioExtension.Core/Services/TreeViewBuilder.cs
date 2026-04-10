using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Builds the hierarchical tree used to display connections and database objects.
/// PT: Monta a árvore hierarquica usada para exibir conexoes e objetos de banco.
/// </summary>
public sealed class TreeViewBuilder
{
    /// <summary>
    /// EN: Builds a tree rooted at the connection and grouped by object type.
    /// PT: Monta uma árvore enraizada na conexao e agrupada por tipo de objeto.
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
        DatabaseObjectType.Function => "Functions",
        DatabaseObjectType.Sequence => "Sequences",
        _ => type.ToString()
    };
}
