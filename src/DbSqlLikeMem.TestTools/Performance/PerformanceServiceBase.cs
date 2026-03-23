namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Provides shared reflection helpers for benchmark services that inspect provider diagnostics.
/// PT: Fornece helpers de reflexao compartilhados para services de benchmark que inspecionam diagnosticos do provedor.
/// </summary>
public abstract class PerformanceServiceBase<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Reads a property or field from a diagnostic object when it is available.
    /// PT: Lê uma propriedade ou campo de um objeto de diagnostico quando ele esta disponivel.
    /// </summary>
    protected static object? TryReadDiagnosticValue(object target, string memberName)
    {
        var type = target.GetType();
        var property = type.GetProperty(memberName);
        if (property is not null && property.GetIndexParameters().Length == 0)
        {
            return property.GetValue(target);
        }

        var field = type.GetField(memberName);
        if (field is not null)
        {
            return field.GetValue(target);
        }

        return null;
    }

    /// <summary>
    /// EN: Invokes a parameterless method when it exists on the target object.
    /// PT: Invoca um metodo sem parametros quando ele existe no objeto alvo.
    /// </summary>
    protected static bool TryInvokeIfExists(object target, string methodName)
    {
        var method = target.GetType().GetMethod(methodName, Type.EmptyTypes);
        if (method is null)
        {
            return false;
        }

        method.Invoke(target, null);
        return true;
    }

    /// <summary>
    /// EN: Invokes a single-parameter method when it exists on the target object.
    /// PT: Invoca um metodo com um parametro quando ele existe no objeto alvo.
    /// </summary>
    protected static object? TryInvokeWithArgIfExists(object target, string methodName, object? arg)
    {
        var methods = target.GetType().GetMethods().Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal)).ToArray();
        foreach (var method in methods)
        {
            var parameters = method.GetParameters();
            if (parameters.Length == 1)
            {
                return method.Invoke(target, [arg]);
            }
        }

        return null;
    }

    /// <summary>
    /// EN: Invokes a parameterless snapshot method when it exists on the target object.
    /// PT: Invoca um metodo de snapshot sem parametros quando ele existe no objeto alvo.
    /// </summary>
    protected static object? TryInvokeSnapshot(object target, string methodName)
    {
        var method = target.GetType().GetMethod(methodName, Type.EmptyTypes);
        return method?.Invoke(target, null);
    }
}
