using DbSqlLikeMem.TestTools.Json;

namespace DbSqlLikeMem.TestTools.FidelityServicesTests.DML;

/// <summary>
/// EN: Base class for DML mutation tests that require typed parameter insertion and verification of inserted values.
/// PT: Classe base para testes de mutação DML que requerem inserção de parâmetros tipados e verificação dos valores inseridos.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public abstract class DmlMutationCrudServiceTestBase(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context)
{
    /// <summary>
    /// EN: Normalizes a string value for Oracle when the provider is Oracle and the value is null or empty. This is necessary because Oracle treats empty strings as null values, so this helper ensures that any empty string values are converted to null before being used in tests that involve Oracle. For other providers, the value is returned unchanged.
    /// PT: Normaliza um valor de string para Oracle quando o provedor é Oracle e o valor é nulo ou vazio. Isso é necessário porque o Oracle trata strings vazias como valores nulos, então este helper garante que quaisquer valores de string vazios sejam convertidos para nulos antes de serem usados em testes que envolvem Oracle. Para outros provedores, o valor é retornado sem alterações.
    /// </summary>
    /// <param name="value">EN: The text value to normalize. PT: O valor de texto a normalizar.</param>
    /// <returns>EN: Null for empty Oracle text values; otherwise, the original value. PT: Null para valores de texto vazios no Oracle; caso contrario, o valor original.</returns>
    protected string? NormalizeOracleNullableText(string? value)
    => Repo.Dialect.Provider == ProviderId.Oracle && string.IsNullOrEmpty(value) ? null : value;

    /// <summary>
    /// EN: Inserts a user record into the specified table with the provided values using typed parameters. This helper method constructs an INSERT statement with parameter placeholders and adds the parameters to the command with the appropriate data types. It also includes special handling for JSON parameters and accounts for provider-specific nuances in parameter creation. After executing the command, it checks the affected row count to ensure that the insert operation was successful.
    /// PT: Insere um registro de usuário na tabela especificada com os valores fornecidos usando parâmetros tipados. Este método auxiliar constrói uma instrução INSERT com espaços reservados para parâmetros e adiciona os parâmetros ao comando com os tipos de dados apropriados. Ele também inclui tratamento especial para parâmetros JSON e leva em consideração as nuances específicas do provedor na criação de parâmetros. Após executar o comando, ele verifica a contagem de linhas afetadas para garantir que a operação de inserção foi bem-sucedida.
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="email"></param>
    /// <param name="isActive"></param>
    /// <param name="age"></param>
    /// <param name="balance"></param>
    /// <param name="createdAt"></param>
    /// <param name="updatedAt"></param>
    /// <param name="profileJson"></param>
    /// <param name="transaction"></param>
    /// <returns>EN: A completed task once the insert has been verified. PT: Uma tarefa concluida quando o insert e validado.</returns>
    /// <exception cref="InvalidOperationException"></exception>
    protected async Task InsertTypedUserAsync(
        int id,
        string name,
        string email,
        bool isActive,
        short age,
        decimal balance,
        DateTime createdAt,
        DateTime updatedAt,
        string profileJson,
        DbTransaction? transaction = null)
    {
        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")},
    {Repo.Dialect.Parameter("email")},
    {Repo.Dialect.Parameter("isActive")},
    {Repo.Dialect.Parameter("age")},
    {Repo.Dialect.Parameter("balance")},
    {Repo.Dialect.Parameter("createdAt")},
    {Repo.Dialect.Parameter("updatedAt")},
    {Repo.Dialect.JsonParameter("profileJson")}
)
""", transaction,
 addParameters: command =>
 {
     AddParameter(command, "id", DbType.Int32, id);
     AddParameter(command, "name", DbType.String, name);
     AddParameter(command, "email", DbType.AnsiString, email);
     AddParameter(command, "isActive", DbType.Boolean, isActive);
     AddParameter(command, "age", DbType.Int16, age);
     AddParameter(command, "balance", DbType.Currency, balance);
     AddParameter(command, "createdAt", DbType.DateTime, createdAt);
     AddParameter(command, "updatedAt", DbType.DateTime, updatedAt);
     AddParameter(command, "profileJson", DbType.String, profileJson);
 });
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }
    }

    /// <summary>
    /// EN: Verifies that a user record with the specified values has been inserted into the database. This helper method executes a SELECT query to retrieve the inserted values based on the provided ID and compares them against the expected values passed as parameters. It accounts for potential variations in how different providers may return certain data types (e.g., DateTime) and uses assertions to ensure that the retrieved values match the expected ones, including a JSON text comparison for the profileJson field.
    /// PT: Verifica se um registro de usuário com os valores especificados foi inserido no banco de dados. Este método auxiliar executa uma consulta SELECT para recuperar os valores inseridos com base no ID fornecido e os compara com os valores esperados passados como parâmetros. Ele leva em consideração as variações potenciais em como diferentes provedores podem retornar certos tipos de dados (por exemplo, DateTime) e usa asserções para garantir que os valores recuperados correspondam aos esperados, incluindo uma comparação de texto JSON para o campo profileJson.
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="email"></param>
    /// <param name="isActive"></param>
    /// <param name="age"></param>
    /// <param name="balance"></param>
    /// <param name="createdAt"></param>
    /// <param name="updatedAt"></param>
    /// <param name="profileJson"></param>
    protected async Task VerifyInsertedTypedUserAsync(
        int id,
        string name,
        string email,
        bool isActive,
        short age,
        decimal balance,
        DateTime createdAt,
        DateTime updatedAt,
        string profileJson)
    {
        var reader = await Repo.ExecuteReaderAsync($"""
SELECT
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
FROM {Context.TbUsersFullName}
WHERE Id = {id}
"""
        );

        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        row.Length.Should().Be(8);

        NormalizeNullableText(row[0]).Should().Be(name);
        NormalizeNullableText(row[1]).Should().Be(email);
        Convert.ToBoolean(row[2], CultureInfo.InvariantCulture).Should().Be(isActive);
        Convert.ToInt16(row[3], CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(row[4], CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeDateTimeValue(row[5]).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        NormalizeDateTimeValue(row[6]).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(updatedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        JsonTextAssertions.ShouldMatchJsonText(
            NormalizeNullableText(row[7]),
            profileJson);
    }

    /// <summary>
    /// EN: Normalizes a value that may be returned from the database as part of a DateTime column. This helper accounts for potential variations in how different providers may return date/time values, such as returning DateTimeOffset instead of DateTime, or returning date-only values as DateOnly. The goal is to ensure that the value can be consistently compared to an expected DateTime value in tests, regardless of the provider's specific behavior.
    /// PT: Normaliza um valor que pode ser retornado do banco de dados como parte de uma coluna DateTime. Este helper leva em consideração as variações potenciais em como diferentes provedores podem retornar valores de data/hora, como retornar DateTimeOffset em vez de DateTime, ou retornar valores apenas de data como DateOnly. O objetivo é garantir que o valor possa ser comparado de forma consistente a um valor DateTime esperado em testes, independentemente do comportamento específico do provedor.
    /// </summary>
    /// <param name="value">EN: The database value to normalize. PT: O valor do banco de dados a normalizar.</param>
    /// <returns>EN: The normalized string value, or null when the input is null. PT: O valor de string normalizado, ou null quando a entrada e null.</returns>
    protected static string? NormalizeNullableText(object? value)
        => value is null or DBNull
            ? null
            : Convert.ToString(value, CultureInfo.InvariantCulture);

    /// <summary>
    /// EN: Normalizes a nullable DateTime value returned from the database into a readable string or null.
    /// PT: Normaliza um valor DateTime anulavel retornado do banco em uma string legivel ou null.
    /// </summary>
    /// <param name="value">EN: The database value to normalize. PT: O valor do banco de dados a normalizar.</param>
    /// <returns>EN: The normalized date-time text, or null when the input is null. PT: O texto normalizado da data/hora, ou null quando a entrada e null.</returns>
    protected static string? NormalizeNullableDateTimeText(object? value)
        => value is null or DBNull
            ? null
            : NormalizeDateTimeValue(value).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>
    /// EN: Normalizes a value that may be returned from the database as part of a DateTime column into a DateTime object. This helper accounts for potential variations in how different providers may return date/time values, such as returning DateTimeOffset instead of DateTime, or returning date-only values as DateOnly. The goal is to ensure that the value can be consistently compared to an expected DateTime value in tests, regardless of the provider's specific behavior.
    /// PT: Normaliza um valor que pode ser retornado do banco de dados como parte de uma coluna DateTime em um objeto DateTime. Este helper leva em consideração as variações potenciais em como diferentes provedores podem retornar valores de data/hora, como retornar DateTimeOffset em vez de DateTime, ou retornar valores apenas de data como DateOnly. O objetivo é garantir que o valor possa ser comparado de forma consistente a um valor DateTime esperado em testes, independentemente do comportamento específico do provedor.
    /// </summary>
    /// <param name="value">EN: The database value to normalize. PT: O valor do banco de dados a normalizar.</param>
    /// <returns>EN: The normalized DateTime value. PT: O valor DateTime normalizado.</returns>
    protected static DateTime NormalizeDateTimeValue(object? value) => value switch
    {
        DateTime dateTime => dateTime,
        DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
        string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        null => throw new InvalidOperationException("DateTime parameter returned a null value."),
        _ when TryNormalizeDateOnlyValue(value, out var dateOnly) => dateOnly,
        _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
    };

    /// <summary>
    /// EN: Attempts to normalize a value of type System.DateOnly to a DateTime with the time component set to midnight. This is necessary because some providers may return DateOnly values for date columns, and this helper allows for consistent comparison in tests that expect DateTime values.
    /// PT: Tenta normalizar um valor do tipo System.DateOnly para um DateTime com o componente de hora definido para meia-noite. Isso é necessário porque alguns provedores podem retornar valores DateOnly para colunas de data, e este helper permite uma comparação consistente em testes que esperam valores DateTime.
    /// </summary>
    /// <param name="value">EN: The value to inspect for a DateOnly payload. PT: O valor a inspecionar para um payload DateOnly.</param>
    /// <param name="dateTime">EN: Receives the normalized DateTime when the value is DateOnly. PT: Recebe o DateTime normalizado quando o valor e DateOnly.</param>
    /// <returns>EN: True when the value was normalized from DateOnly; otherwise, false. PT: True quando o valor foi normalizado a partir de DateOnly; caso contrario, false.</returns>
    protected static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }

    /// <summary>
    /// EN: Attempts to strip a scenario token suffix from the table name if present. The expected format for the suffix is an underscore followed by an 8-character hexadecimal string (e.g., "_1A2B3C4D"). This allows for dynamic table naming in test scenarios while maintaining a consistent base name for SQL statements.
    /// PT: Tenta remover um sufixo de token de cenário do nome da tabela, se presente. O formato esperado para o sufixo é um sublinhado seguido por uma string hexadecimal de 8 caracteres (por exemplo, "_1A2B3C4D"). Isso permite a nomeação dinâmica de tabelas em cenários de teste, mantendo um nome base consistente para as instruções SQL.
    /// </summary>
    /// <param name="tableName">EN: The table name to inspect. PT: O nome da tabela a inspecionar.</param>
    /// <param name="stripped">EN: Receives the base table name when a scenario suffix is found. PT: Recebe o nome base da tabela quando um sufixo de cenario e encontrado.</param>
    /// <returns>EN: True when a valid scenario suffix was removed; otherwise, false. PT: True quando um sufixo de cenario valido foi removido; caso contrario, false.</returns>
    protected static bool TryStripScenarioTokenSuffix(string tableName, out string stripped)
    {
        stripped = tableName;

        var underscoreIndex = tableName.LastIndexOf('_');
        if (underscoreIndex < 0)
        {
            return false;
        }

        var suffixLength = tableName.Length - underscoreIndex - 1;
        if (suffixLength != 8)
        {
            return false;
        }

        for (var i = underscoreIndex + 1; i < tableName.Length; i++)
        {
            var ch = tableName[i];
            var isHexUpper = ch is >= 'A' and <= 'F';
            var isHexDigit = ch is >= '0' and <= '9';
            if (!isHexUpper && !isHexDigit)
            {
                return false;
            }
        }

        stripped = tableName[..underscoreIndex];
        return true;
    }
}
