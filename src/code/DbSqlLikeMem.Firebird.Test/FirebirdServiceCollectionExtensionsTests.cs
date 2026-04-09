using Microsoft.Extensions.DependencyInjection;

namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Validates Firebird service collection registration helpers.
/// PT: Valida os helpers de registro de Firebird na coleção de serviços.
/// </summary>
public sealed class FirebirdServiceCollectionExtensionsTests(
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
    /// EN: Verifies AddFirebirdDbMockTransient registers a transient factory that creates new configured FirebirdDbMock instances on each resolution.
    /// PT: Verifica se AddFirebirdDbMockTransient registra uma factory transient que cria novas instancias configuradas de FirebirdDbMock em cada resolucao.
    /// </summary>
    [Fact]
    public void AddFirebirdDbMockTransient_ShouldCreateNewConfiguredInstanceEachResolution()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddFirebirdDbMockTransient(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 1 });
            },
            version: 30);

        var descriptor = services.Single();
        descriptor.ServiceType.Should().Be(typeof(FirebirdDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Should().NotBeSameAs(second);
        first.Version.Should().Be(30);
        second.Version.Should().Be(30);
        first.GetTable("Users").Should().HaveCount(1);
        second.GetTable("Users").Should().HaveCount(1);
    }

    /// <summary>
    /// EN: Verifies AddFirebirdDbMockSingleton registers one configured instance reused across resolutions.
    /// PT: Verifica se AddFirebirdDbMockSingleton registra uma instancia configurada reutilizada entre resolucoes.
    /// </summary>
    [Fact]
    public void AddFirebirdDbMockSingleton_ShouldReuseConfiguredInstance()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddFirebirdDbMockSingleton(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Id", DbType.Int32, false);
                table.Add(new Dictionary<int, object?> { [0] = 7 });
            },
            version: 50);

        var descriptor = services.Single();

        descriptor.ServiceType.Should().Be(typeof(FirebirdDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Version.Should().Be(50);
        first.GetTable("Users").Single()[0].Should().Be(7);
        second.Version.Should().Be(50);
        second.GetTable("Users").Single()[0].Should().Be(7);
    }

    /// <summary>
    /// EN: Verifies AddFirebirdDbMockScoped registers a scoped factory that creates configured instances.
    /// PT: Verifica se AddFirebirdDbMockScoped registra uma factory com escopo que cria instancias configuradas.
    /// </summary>
    [Fact]
    public void AddFirebirdDbMockScoped_ShouldCreateConfiguredInstance()
    {
        IServiceCollection services = new ServiceCollectionStub();
        services.AddFirebirdDbMockScoped(
            acRegister: db =>
            {
                var table = db.AddTable("Users");
                table.AddColumn("Name", DbType.String, false);
                table.Add(new Dictionary<int, object?> { [0] = "Ana" });
            },
            version: 30);

        var descriptor = services.Single();

        descriptor.ServiceType.Should().Be(typeof(FirebirdDbMock));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
        descriptor.ImplementationFactory.Should().NotBeNull();

        var first = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());
        var second = (FirebirdDbMock)descriptor.ImplementationFactory!(new NullServiceProvider());

        first.Version.Should().Be(30);
        first.GetTable("Users").Single()[0].Should().Be("Ana");
        second.Version.Should().Be(30);
        second.GetTable("Users").Single()[0].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies default registration overloads return the same service collection and still register FirebirdDbMock.
    /// PT: Verifica se as sobrecargas padrão retornam a mesma coleção de serviços e ainda registram FirebirdDbMock.
    /// </summary>
    [Fact]
    public void DefaultOverloads_ShouldReturnSameServiceCollectionAndRegisterService()
    {
        IServiceCollection singletonServices = new ServiceCollectionStub();
        IServiceCollection scopedServices = new ServiceCollectionStub();
        IServiceCollection transientServices = new ServiceCollectionStub();

        singletonServices.AddFirebirdDbMockSingleton().Should().BeSameAs(singletonServices);
        scopedServices.AddFirebirdDbMockScoped().Should().BeSameAs(scopedServices);
        transientServices.AddFirebirdDbMockTransient().Should().BeSameAs(transientServices);

        singletonServices.Single().ServiceType.Should().Be(typeof(FirebirdDbMock));
        scopedServices.Single().ServiceType.Should().Be(typeof(FirebirdDbMock));
        transientServices.Single().ServiceType.Should().Be(typeof(FirebirdDbMock));
    }
}
