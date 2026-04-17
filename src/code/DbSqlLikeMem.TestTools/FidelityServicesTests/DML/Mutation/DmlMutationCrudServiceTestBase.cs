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
    /// <param name="value"></param>
    /// <returns></returns>
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
    /// <returns></returns>
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
    /// <returns></returns>
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
WHERE Id = {Repo.Dialect.Parameter("id")}
""",
        addParameters: command => AddParameter(command, "id", DbType.Int32, id));

        reader.Should().NotBeEmpty();

        var lst = reader[0];

        lst.Count.Should().Be(1);

        Convert.ToString(lst[0], CultureInfo.InvariantCulture).Should().Be(name);
        Convert.ToString(lst[1], CultureInfo.InvariantCulture).Should().Be(email);
        Convert.ToBoolean(lst[2], CultureInfo.InvariantCulture).Should().Be(isActive);
        Convert.ToInt16(lst[3], CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(lst[4], CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeDateTimeValue(lst[5]).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        NormalizeDateTimeValue(lst[6]).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(updatedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        JsonTextAssertions.ShouldMatchJsonText(
            Convert.ToString(lst[7], CultureInfo.InvariantCulture),
            profileJson);
    }

    /// <summary>
    /// EN: Adds a parameter to the provided DbCommand with the specified name, data type, and value. This helper method abstracts away provider-specific nuances in parameter creation, such as handling of currency types for Db2 or special considerations for Npgsql when dealing with DateTime values. It ensures that parameters are added correctly to the command's parameter collection, accounting for any provider-specific requirements or limitations.
    /// PT: Adiciona um parâmetro ao DbCommand fornecido com o nome, tipo de dados e valor especificados. Este método auxiliar abstrai as nuances específicas do provedor na criação de parâmetros, como o tratamento de tipos de moeda para Db2 ou considerações especiais para Npgsql ao lidar com valores DateTime. Ele garante que os parâmetros sejam adicionados corretamente à coleção de parâmetros do comando, levando em consideração quaisquer requisitos ou limitações específicos do provedor.
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="dbType"></param>
    /// <param name="value"></param>
    protected override void AddParameter(DbCommand command, string name, DbType dbType, object? value)
    {
        if (Repo.Dialect.Provider == ProviderId.Db2 && dbType == DbType.Currency)
        {
            AddDb2CurrencyParameter(command, value);
            return;
        }

        var parameter = command.CreateParameter();
        parameter.ParameterName = Repo.Dialect.Provider == ProviderId.Db2 ? "?" : name;
        if (Repo.Dialect.Provider == ProviderId.Oracle)
        {
            // ODP.NET can reject DbType assignments for OracleParameter in this mock flow.
            // Keep the default DbType and rely on the value payload for this shared test helper.
        }
        else if (Repo.Dialect.Provider == ProviderId.Firebird && dbType == DbType.Currency)
        {
            // Firebird parameters accept the decimal payload, but FbParameter rejects DbType.Currency.
            // Keep the default DbType and let the value carry the numeric type.
        }
        else
        {
            parameter.DbType = dbType;
        }
        if (Repo.Dialect.Provider == ProviderId.Npgsql
            && dbType == DbType.DateTime
            && value is DateTime dateTime
            && dateTime.Kind == DateTimeKind.Unspecified)
        {
            value = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
        }
        parameter.Value = value ?? DBNull.Value;
        if (Repo.Dialect.Provider == ProviderId.Db2)
        {
            SetDb2ParameterSize(parameter, parameter.Value);
        }
        AddParameterToCollection(command, parameter);
    }

    /// <summary>
    /// EN: Adds a parameter specifically for Db2 when the data type is Currency. Db2 may have specific requirements for handling currency parameters, and this helper ensures that the parameter is created with the appropriate settings for Db2. The parameter name is set to "?" as a placeholder, and the DbType is set to Decimal since Db2 treats currency values as decimals. The value is assigned directly, allowing for proper handling of null values.
    /// PT: Adiciona um parâmetro especificamente para Db2 quando o tipo de dados é Currency. O Db2 pode ter requisitos específicos para lidar com parâmetros de moeda, e este helper garante que o parâmetro seja criado com as configurações apropriadas para Db2. O nome do parâmetro é definido como "?" como um espaço reservado, e o DbType é definido como Decimal, pois o Db2 trata os valores de moeda como decimais. O valor é atribuído diretamente, permitindo o manuseio adequado de valores nulos.
    /// </summary>
    /// <param name="command"></param>
    /// <param name="value"></param>
    protected static void AddDb2CurrencyParameter(DbCommand command, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "?";
        parameter.DbType = DbType.Decimal;
        parameter.Value = value ?? DBNull.Value;
        AddParameterToCollection(command, parameter);
    }

    /// <summary>
    /// EN: Sets the Size property for Db2 parameters when the value is a string or byte array. This is necessary because Db2 may require the Size to be set for certain data types to ensure proper handling of the parameter value. For string values, the Size is set to the length of the string, and for byte arrays, it is set to the length of the array.
    /// PT: Define a propriedade Size para parâmetros Db2 quando o valor for uma string ou array de bytes. Isso é necessário porque o Db2 pode exigir que o Size seja definido para certos tipos de dados para garantir o manuseio adequado do valor do parâmetro. Para valores de string, o Size é definido como o comprimento da string, e para arrays de bytes, é definido como o comprimento do array.
    /// </summary>
    /// <param name="parameter"></param>
    /// <param name="value"></param>
    protected static void SetDb2ParameterSize(DbParameter parameter, object? value)
    {
        if (value is string stringValue)
        {
            parameter.Size = stringValue.Length;
            return;
        }

        if (value is byte[] binaryValue)
        {
            parameter.Size = binaryValue.Length;
        }
    }

    /// <summary>
    /// EN: Adds a parameter to the command's parameter collection using reflection to handle potential differences in how providers implement the Add method. Some providers may have specific overloads for their parameter types, and this helper attempts to invoke the most specific overload available. If no specific overload is found, it falls back to the general Add method.
    /// PT: Adiciona um parâmetro à coleção de parâmetros do comando usando reflexão para lidar com possíveis diferenças em como os provedores implementam o método Add. Alguns provedores podem ter sobrecargas específicas para seus tipos de parâmetro, e este helper tenta invocar a sobrecarga mais específica disponível. Se nenhuma sobrecarga específica for encontrada, ele recorre ao método Add geral.
    /// </summary>
    /// <param name="command"></param>
    /// <param name="parameter"></param>
    protected static void AddParameterToCollection(DbCommand command, DbParameter parameter)
    {
        var addMethod = command.Parameters.GetType().GetMethod(nameof(DbParameterCollection.Add), [parameter.GetType()]);
        if (addMethod is not null)
        {
            addMethod.Invoke(command.Parameters, [parameter]);
            return;
        }

        command.Parameters.Add(parameter);
    }

    /// <summary>
    /// EN: Normalizes a value that may be returned from the database as part of a DateTime column. This helper accounts for potential variations in how different providers may return date/time values, such as returning DateTimeOffset instead of DateTime, or returning date-only values as DateOnly. The goal is to ensure that the value can be consistently compared to an expected DateTime value in tests, regardless of the provider's specific behavior.
    /// PT: Normaliza um valor que pode ser retornado do banco de dados como parte de uma coluna DateTime. Este helper leva em consideração as variações potenciais em como diferentes provedores podem retornar valores de data/hora, como retornar DateTimeOffset em vez de DateTime, ou retornar valores apenas de data como DateOnly. O objetivo é garantir que o valor possa ser comparado de forma consistente a um valor DateTime esperado em testes, independentemente do comportamento específico do provedor.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
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
    /// <param name="value"></param>
    /// <param name="dateTime"></param>
    /// <returns></returns>
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
    /// <param name="tableName"></param>
    /// <param name="stripped"></param>
    /// <returns></returns>
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
