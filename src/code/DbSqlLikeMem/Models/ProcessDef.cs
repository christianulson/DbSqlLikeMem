namespace DbSqlLikeMem.Models;

/// <summary>
/// EN: Defines a base record for database process definitions.
/// PT: Define um registro base para definicoes de processos do banco de dados.
/// </summary>
/// <param name="Name">EN: Process name. PT: Nome do processo.</param>
public abstract record class ProcessDef(string Name);
