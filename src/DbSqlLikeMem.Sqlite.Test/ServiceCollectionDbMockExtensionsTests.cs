using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates generic DbMock service collection registration helpers.
/// PT: Valida os helpers genéricos de registro de DbMock na coleção de serviços.
/// </summary>
public sealed class ServiceCollectionDbMockExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    [Fact]
    public void AddDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddDbMockTransient<SqliteDbMock>(db =>
        {
            var table = db.AddTable("Users");
            table.AddColumn("Id", DbType.Int32, false);
            table.Add(new Dictionary<int, object?> { [0] = 1 });
        });

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(SqliteDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (SqliteDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (SqliteDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
