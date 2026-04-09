using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Validates Oracle service collection registration helpers.
/// PT: Valida os helpers de registro de Oracle na coleção de serviços.
/// </summary>
public sealed class OracleServiceCollectionExtensionsTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
    /// <summary>
    /// EN: Verifies AddOracleDbMockTransient registers a transient factory that creates new configured OracleDbMock instances on each resolution.
    /// PT: Verifica se AddOracleDbMockTransient registra uma factory transient que cria novas instancias configuradas de OracleDbMock em cada resolucao.
    /// </summary>
    [Fact]
    public void AddOracleDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddOracleDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 21);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(OracleDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (OracleDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (OracleDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(21);
        second.Version.Should().Be(21);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}

