using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates MySql service collection registration helpers.
/// PT: Valida os helpers de registro de MySql na coleção de serviços.
/// </summary>
public sealed class MySqlServiceCollectionExtensionsTests
{
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
        IServiceCollection services = new ServiceCollection();
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

    /// <summary>
    /// EN: Verifies AddMySqlDbMockSingleton registers one configured instance reused across resolutions.
    /// PT: Verifica se AddMySqlDbMockSingleton registra uma instancia configurada reutilizada entre resolucoes.
    /// </summary>
    [Fact]
    public void AddMySqlDbMockSingleton_ShouldReuseConfiguredInstance()
    {
        IServiceCollection services = new ServiceCollection();
        services.AddMySqlDbMockSingleton(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 7 });
            },
            version: 5);

        var descriptor = services.Single();

        descriptor.ServiceType.Should().Be(typeof(MySqlDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Version.Should().Be(5);
        first.GetTable("Users").Single()[0].Should().Be(7);
        second.Version.Should().Be(5);
        second.GetTable("Users").Single()[0].Should().Be(7);
    }

    /// <summary>
    /// EN: Verifies AddMySqlDbMockScoped registers a scoped factory that creates configured instances.
    /// PT: Verifica se AddMySqlDbMockScoped registra uma factory com escopo que cria instancias configuradas.
    /// </summary>
    [Fact]
    public void AddMySqlDbMockScoped_ShouldCreateConfiguredInstance()
    {
        IServiceCollection services = new ServiceCollection();
        services.AddMySqlDbMockScoped(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Name", DbType.String, false);
                table.Add(new Dictionary<int, object?> { [0] = "Ana" });
            },
            version: 8);

        var descriptor = services.Single();

        descriptor.ServiceType.Should().Be(typeof(MySqlDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (MySqlDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Version.Should().Be(8);
        first.GetTable("Users").Single()[0].Should().Be("Ana");
        second.Version.Should().Be(8);
        second.GetTable("Users").Single()[0].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies default registration overloads return the same service collection and still register MySqlDbMock.
    /// PT: Verifica se as sobrecargas padrão retornam a mesma coleção de serviços e ainda registram MySqlDbMock.
    /// </summary>
    [Fact]
    public void DefaultOverloads_ShouldReturnSameServiceCollectionAndRegisterService()
    {
        IServiceCollection singletonServices = new ServiceCollection();
        IServiceCollection scopedServices = new ServiceCollection();
        IServiceCollection transientServices = new ServiceCollection();

        singletonServices.AddMySqlDbMockSingleton().Should().BeSameAs(singletonServices);
        scopedServices.AddMySqlDbMockScoped().Should().BeSameAs(scopedServices);
        transientServices.AddMySqlDbMockTransient().Should().BeSameAs(transientServices);

        singletonServices.Single().ServiceType.Should().Be(typeof(MySqlDbMock));
        scopedServices.Single().ServiceType.Should().Be(typeof(MySqlDbMock));
        transientServices.Single().ServiceType.Should().Be(typeof(MySqlDbMock));
    }
}
