using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

public sealed class TreeViewBuilder
{
    public TreeNode Build(ConnectionDefinition connection, IReadOnlyCollection<DatabaseObjectReference> objects)
    {
        var root = new TreeNode { Label = connection.DatabaseType, ContextKey = "database-type" };
        var dbNode = new TreeNode { Label = connection.DatabaseName, ContextKey = "database-name" };
        root.Children.Add(dbNode);

        foreach (var objectType in Enum.GetValues<DatabaseObjectType>())
        {
            var typeNode = new TreeNode { Label = GetGroupLabel(objectType), ContextKey = "object-type", ObjectType = objectType };

            foreach (var item in objects.Where(o => o.Type == objectType).OrderBy(o => o.Name, StringComparer.OrdinalIgnoreCase))
            {
                typeNode.Children.Add(new TreeNode
                {
                    Label = item.Name,
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
