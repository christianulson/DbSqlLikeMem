using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates MySql service collection registration helpers.
/// PT: Valida os helpers de registro de MySql na coleção de serviços.
/// </summary>
public sealed class MySqlServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>
    /// EN: Verifies AddMySqlDbMockTransient registers a transient factory that creates new configured MySqlDbMock instances on each resolution.
    /// PT: Verifica se AddMySqlDbMockTransient registra uma factory transient que cria novas instancias configuradas de MySqlDbMock em cada resolucao.
    /// </summary>
    [Fact]
    public void AddMySqlDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddMySqlDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 8);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(MySqlDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(8);
        second.Version.Should().Be(8);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
