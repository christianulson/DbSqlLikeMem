using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates Db2 service collection registration helpers.
/// PT: Valida os helpers de registro de Db2 na coleção de serviços.
/// </summary>
public sealed class Db2ServiceCollectionExtensionsTests
{
    private sealed class ServiceCollectionStub : List<ServiceDescriptor>, IServiceCollection
    {
    }

    private sealed class NullServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>
    /// EN: Verifies AddDb2DbMockTransient registers a transient factory that creates new configured Db2DbMock instances on each resolution.
    /// PT: Verifica se AddDb2DbMockTransient registra uma factory transient que cria novas instancias configuradas de Db2DbMock em cada resolucao.
    /// </summary>
    [Fact]
    public void AddDb2DbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddDb2DbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 11);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(Db2DbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (Db2DbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (Db2DbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(11);
        second.Version.Should().Be(11);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }
}
