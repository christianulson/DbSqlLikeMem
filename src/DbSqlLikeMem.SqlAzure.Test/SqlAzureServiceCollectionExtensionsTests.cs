using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Validates SQL Azure service collection registration helpers.
/// PT: Valida os helpers de registro de SQL Azure na coleção de serviços.
/// </summary>
public sealed class SqlAzureServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>
    /// EN: Ensures singleton registration keeps one instance and applies configuration callback.
    /// PT: Garante que o registro singleton mantenha uma instância e aplique o callback de configuração.
    /// </summary>
    [Fact]
    public void AddSqlAzureDbMockSingleton_ShouldRegisterSingleConfiguredInstance()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddSqlAzureDbMockSingleton(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            compatibilityLevel: SqlAzureDbCompatibilityLevels.SqlServer2019);
        services.Should().ContainSingle();

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(SqlAzureDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (SqlAzureDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (SqlAzureDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeNull();
        first.Version.Should().Be(SqlAzureDbCompatibilityLevels.SqlServer2019);
        first.GetTable("Users").Should().HaveCount(1);
        second.Version.Should().Be(SqlAzureDbCompatibilityLevels.SqlServer2019);
    }

    /// <summary>
    /// EN: Ensures scoped registration creates one instance per scope.
    /// PT: Garante que o registro scoped crie uma instância por escopo.
    /// </summary>
    [Fact]
    public void AddSqlAzureDbMockScoped_ShouldCreateOneInstancePerScope()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddSqlAzureDbMockScoped(compatibilityLevel: SqlAzureDbCompatibilityLevels.SqlServer2016);
        services.Should().ContainSingle();

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(SqlAzureDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (SqlAzureDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (SqlAzureDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeNull();
        second.Should().NotBeNull();
        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(SqlAzureDbCompatibilityLevels.SqlServer2016);
        second.Version.Should().Be(SqlAzureDbCompatibilityLevels.SqlServer2016);
    }
}
