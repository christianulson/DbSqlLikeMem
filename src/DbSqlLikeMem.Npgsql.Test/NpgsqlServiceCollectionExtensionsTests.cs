using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates Npgsql service collection registration helpers.
/// PT: Valida os helpers de registro de Npgsql na coleção de serviços.
/// </summary>
public sealed class NpgsqlServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    [Fact]
    public void AddNpgsqlDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddNpgsqlDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 16);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(NpgsqlDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (NpgsqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (NpgsqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(16);
        second.Version.Should().Be(16);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
