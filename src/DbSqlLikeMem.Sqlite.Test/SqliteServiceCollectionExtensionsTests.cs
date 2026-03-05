using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates Sqlite service collection registration helpers.
/// PT: Valida os helpers de registro de Sqlite na coleção de serviços.
/// </summary>
public sealed class SqliteServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    [Fact]
    public void AddSqliteDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddSqliteDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 3);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(SqliteDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (SqliteDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (SqliteDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(3);
        second.Version.Should().Be(3);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
