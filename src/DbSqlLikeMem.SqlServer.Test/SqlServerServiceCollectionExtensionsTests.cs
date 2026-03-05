using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Validates SqlServer service collection registration helpers.
/// PT: Valida os helpers de registro de SqlServer na coleção de serviços.
/// </summary>
public sealed class SqlServerServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    [Fact]
    public void AddSqlServerDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddSqlServerDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 15);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(SqlServerDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (SqlServerDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (SqlServerDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(15);
        second.Version.Should().Be(15);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
