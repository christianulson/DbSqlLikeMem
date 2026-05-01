using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Builds the hierarchical tree used to display connections and database objects.
/// PT-br: Monta a árvore hierarquica usada para exibir conexoes e objetos de banco.
/// </summary>
public sealed class TreeViewBuilder
{
    /// <summary>
    /// EN: Builds a tree rooted at the connection and grouped by object type.
    /// PT-br: Monta uma árvore enraizada na conexao e agrupada por tipo de objeto.
    /// </summary>
    public TreeNode Build(ConnectionDefinition connection, IReadOnlyCollection<DatabaseObjectReference> objects)
    {
        var root = new TreeNode(connection.DatabaseType)
        {
            ContextKey = "database-type",
            NodeGlyph = "🗃"
        };
        var dbNode = new TreeNode(connection.DatabaseName)
        {
            ContextKey = "database-name",
            NodeGlyph = "🧩"
        };
        root.AddChild(dbNode);

        foreach (DatabaseObjectType objectType in Enum.GetValues(typeof(DatabaseObjectType)))
        {
            var typeNode = new TreeNode(DatabaseObjectTypeLabels.GetGroupLabel(objectType))
            {
                ContextKey = "object-type",
                ObjectType = objectType,
                NodeGlyph = GetGroupGlyph(objectType)
            };

            foreach (var item in objects.Where(o => o.Type == objectType).OrderBy(o => o.Name, StringComparer.OrdinalIgnoreCase))
            {
                typeNode.AddChild(new TreeNode(item.Name)
                {
                    ContextKey = "object",
                    ObjectType = objectType,
                    NodeGlyph = GetObjectGlyph(objectType)
                });
            }

            dbNode.AddChild(typeNode);
        }

        return root;
    }

    private static string GetGroupGlyph(DatabaseObjectType objectType)
        => objectType switch
        {
            DatabaseObjectType.Table => "🗂",
            DatabaseObjectType.View => "👁",
            DatabaseObjectType.Procedure => "⚙",
            DatabaseObjectType.Function => "λ",
            DatabaseObjectType.Sequence => "🔢",
            _ => "📁"
        };

    private static string GetObjectGlyph(DatabaseObjectType objectType)
        => objectType switch
        {
            DatabaseObjectType.Table => "▦",
            DatabaseObjectType.View => "◫",
            DatabaseObjectType.Procedure => "ƒ",
            DatabaseObjectType.Function => "λ",
            DatabaseObjectType.Sequence => "🔢",
            _ => "•"
        };
}
