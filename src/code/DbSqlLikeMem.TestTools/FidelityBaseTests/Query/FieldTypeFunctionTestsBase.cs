using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared fidelity tests for typed columns and SQL function calculations across mock and container runs.
/// PT: Fornece testes de fidelidade compartilhados para colunas tipadas e calculos de funcoes entre execucoes mock e container.
/// </summary>
public abstract class FieldTypeFunctionTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that typed columns and common SQL functions keep consistent results for the current provider.
    /// PT: Verifica se colunas tipadas e funcoes SQL comuns mantem resultados consistentes para o provedor atual.
    /// </summary>
    [Fact]
    public void TypedFieldAndFunctionBlendTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldAndFunctionBlend(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldAndFunctionBlend(users, uId);
                    resultMock.Should().Be(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a single large projection query over typed columns and common SQL functions for the current provider.
    /// PT: Verifica uma unica consulta grande sobre colunas tipadas e funcoes SQL comuns para o provedor atual.
    /// </summary>
    [Fact]
    public void TypedFieldFunctionMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldFunctionMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldFunctionMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a second large projection query over typed columns with casts, arithmetic, and predicates for the current provider.
    /// PT: Verifica uma segunda consulta grande sobre colunas tipadas com casts, aritmetica e predicados para o provedor atual.
    /// </summary>
    [Fact]
    public void TypedFieldCalculationMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldCalculationMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldCalculationMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies text, boolean, integer, exact and approximate numeric, fixed-length text, bigint, GUID, binary, time, DateTimeOffset, and temporal columns round-trip consistently for the current provider, including boundary values such as empty strings and zero-length binary payloads.
    /// PT: Verifica se colunas de texto, booleano, inteiro, numerico exato e aproximado, texto de tamanho fixo, bigint, GUID, binario, time, DateTimeOffset e colunas temporais retornam valores consistentes para o provedor atual, incluindo valores de borda como strings vazias e binarios de tamanho zero.
    /// </summary>
    [Fact]
    public void TypedFieldStorageMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldStorageMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldStorageMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large JSON projection query over typed columns for the current provider.
    /// PT: Verifica uma consulta grande de JSON sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void JsonTypedFieldMatrixTest()
    {
        if (!dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunJsonTypedFieldMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJsonTypedFieldMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies that JSON scalar reads return the expected text for the current provider.
    /// PT: Verifica se leituras escalares de JSON retornam o texto esperado para o provedor atual.
    /// </summary>
    [Fact]
    public void JsonScalarReadTest()
    {
        if (!dialect.SupportsJsonScalarRead)
        {
            return;
        }

        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new InsertUsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeScalarText(serviceTest.RunJsonScalarRead());
        resultMock.Should().Be("Alice");

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new InsertUsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeScalarText(serviceTestContainer.RunJsonScalarRead());
            resultMock.Should().Be(resultContainer);
        }
    }

    /// <summary>
    /// EN: Verifies that nested JSON path reads return the expected text for the current provider.
    /// PT: Verifica se leituras de caminho JSON aninhado retornam o texto esperado para o provedor atual.
    /// </summary>
    [Fact]
    public void JsonPathReadTest()
    {
        if (!dialect.SupportsJsonScalarRead)
        {
            return;
        }

        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new InsertUsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeScalarText(serviceTest.RunJsonPathRead());
        resultMock.Should().Be("Alice");

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new InsertUsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeScalarText(serviceTestContainer.RunJsonPathRead());
            resultMock.Should().Be(resultContainer);
        }
    }

    /// <summary>
    /// EN: Verifies that a missing JSON path returns a null value for the current provider.
    /// PT: Verifica se um caminho JSON ausente retorna um valor nulo para o provedor atual.
    /// </summary>
    [Fact]
    public void JsonMissingPathReturnsNullTest()
    {
        if (!dialect.SupportsJsonScalarRead)
        {
            return;
        }

        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new InsertUsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeScalarText(serviceTest.RunJsonInsertCast());
        resultMock.Should().BeNull();

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new InsertUsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeScalarText(serviceTestContainer.RunJsonInsertCast());
            resultMock.Should().Be(resultContainer);
        }
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query over typed columns for the current provider.
    /// PT: Verifica uma consulta temporal grande sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalFieldMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTemporalFieldMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTemporalFieldMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query that blends timestamp comparisons and fallback logic for the current provider.
    /// PT: Verifica uma consulta temporal grande que mistura comparacoes de timestamp e logica de fallback para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalComparisonMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTemporalComparisonMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTemporalComparisonMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a larger temporal arithmetic matrix over typed columns for the current provider.
    /// PT: Verifica uma matriz maior de aritmetica temporal sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalArithmeticMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTemporalArithmeticMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTemporalArithmeticMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends casts, arithmetic, and text formatting over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura casts, aritmetica e formatacao textual sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void CastCalculationMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunCastCalculationMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunCastCalculationMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends null handling and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura tratamento de null e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void NullComparisonMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunNullComparisonMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunNullComparisonMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large typed-field predicate query that blends LIKE, NOT LIKE, BETWEEN, and null checks for the current provider.
    /// PT: Verifica uma consulta grande de predicados em campos tipados que mistura LIKE, NOT LIKE, BETWEEN e verificacoes de null para o provedor atual.
    /// </summary>
    [Fact]
    public void TypedFieldPredicateMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldPredicateMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldPredicateMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a compound typed-field predicate query that blends OR, AND, LIKE, and null checks for the current provider.
    /// PT: Verifica uma consulta de predicado composto em campos tipados que mistura OR, AND, LIKE e verificacoes de null para o provedor atual.
    /// </summary>
    [Fact]
    public void TypedFieldCompoundPredicateMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTypedFieldCompoundPredicateMatrix(users, uId);
            resultMock.Should().Be(2);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTypedFieldCompoundPredicateMatrix(users, uId);
                    resultMock.Should().Be(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends string lengths, trimming, and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura comprimentos de texto, trim e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void TextLengthMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTextLengthMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTextLengthMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends case conversion, trimming, prefix extraction, and text predicates over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura conversao de caixa, trim, extracao de prefixo e predicados de texto sobre colunas tipadas para o provedor atual.
    /// </summary>
    [Fact]
    public void TextCaseMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunTextCaseMatrix(users, uId);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTextCaseMatrix(users, uId);
                    resultMock.ShouldMatch(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    private static string? NormalizeScalarText(object? value)
        => value is null or DBNull
            ? null
            : Convert.ToString(value, CultureInfo.InvariantCulture);
}
