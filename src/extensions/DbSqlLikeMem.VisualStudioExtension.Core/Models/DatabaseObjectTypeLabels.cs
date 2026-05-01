namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// EN: Provides canonical plural labels for database object types used by the UI and tree builders.
/// PT-br: Fornece rótulos plurais canônicos para tipos de objeto de banco usados pela UI e pelos construtores de árvore.
/// </summary>
public static class DatabaseObjectTypeLabels
{
    /// <summary>
    /// EN: Returns the plural label used for the informed database object type.
    /// PT-br: Retorna o rótulo plural usado para o tipo de objeto de banco informado.
    /// </summary>
    public static string GetGroupLabel(DatabaseObjectType objectType) => objectType switch
    {
        DatabaseObjectType.Table => "Tables",
        DatabaseObjectType.View => "Views",
        DatabaseObjectType.Procedure => "Procedures",
        DatabaseObjectType.Function => "Functions",
        DatabaseObjectType.Sequence => "Sequences",
        _ => objectType.ToString()
    };
}
