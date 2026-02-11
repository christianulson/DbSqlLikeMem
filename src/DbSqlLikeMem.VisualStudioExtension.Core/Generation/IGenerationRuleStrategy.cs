namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public interface IGenerationRuleStrategy
{
    string MapDbType(GenerationTypeContext context);
}

public readonly record struct GenerationTypeContext(
    string DataType,
    long? CharMaxLen,
    int? NumPrecision,
    string ColumnName);
