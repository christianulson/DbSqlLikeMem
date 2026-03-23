using System.Collections.Concurrent;
using System.Reflection;

namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Describes shared reflection helpers for benchmark services that inspect provider diagnostics.
/// PT: Descreve helpers de reflexao compartilhados para services de benchmark que inspecionam diagnosticos do provedor.
/// </summary>
public abstract class PerformanceServiceBase<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    private static readonly ConcurrentDictionary<(Type Type, string MemberName), MemberInfo?> DiagnosticMemberCache = [];
    private static readonly ConcurrentDictionary<(Type Type, string MethodName, int ParameterCount), MethodInfo?> DiagnosticMethodCache = [];

    /// <summary>
    /// EN: Reads a property or field from a diagnostic object when it is available.
    /// PT: Lê uma propriedade ou campo de um objeto de diagnostico quando ele esta disponivel.
    /// </summary>
    protected static object? TryReadDiagnosticValue(object target, string memberName)
    {
        var type = target.GetType();
        var member = DiagnosticMemberCache.GetOrAdd(
            (type, memberName),
            static key =>
            {
                var property = key.Type.GetProperty(key.MemberName);
                if (property is not null && property.GetIndexParameters().Length == 0)
                {
                    return property;
                }

                return key.Type.GetField(key.MemberName);
            });

        return member switch
        {
            PropertyInfo property => property.GetValue(target),
            FieldInfo field => field.GetValue(target),
            _ => null
        };
    }

    /// <summary>
    /// EN: Invokes a parameterless method when it exists on the target object.
    /// PT: Invoca um metodo sem parametros quando ele existe no objeto alvo.
    /// </summary>
    protected static bool TryInvokeIfExists(object target, string methodName)
    {
        var method = DiagnosticMethodCache.GetOrAdd(
            (target.GetType(), methodName, 0),
            static key =>
            {
                return key.Type
                    .GetMethods()
                    .FirstOrDefault(m =>
                        string.Equals(m.Name, key.MethodName, StringComparison.Ordinal)
                        && m.GetParameters().Length == key.ParameterCount);
            });

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
        var method = DiagnosticMethodCache.GetOrAdd(
            (target.GetType(), methodName, 1),
            static key =>
            {
                return key.Type
                    .GetMethods()
                    .FirstOrDefault(m =>
                        string.Equals(m.Name, key.MethodName, StringComparison.Ordinal)
                        && m.GetParameters().Length == key.ParameterCount);
            });

        if (method is null)
        {
            return null;
        }

        return method.Invoke(target, [arg]);
    }

    /// <summary>
    /// EN: Invokes a parameterless snapshot method when it exists on the target object.
    /// PT: Invoca um metodo de snapshot sem parametros quando ele existe no objeto alvo.
    /// </summary>
    protected static object? TryInvokeSnapshot(object target, string methodName)
    {
        var method = DiagnosticMethodCache.GetOrAdd(
            (target.GetType(), methodName, 0),
            static key =>
            {
                return key.Type
                    .GetMethods()
                    .FirstOrDefault(m =>
                        string.Equals(m.Name, key.MethodName, StringComparison.Ordinal)
                        && m.GetParameters().Length == key.ParameterCount);
            });

        return method?.Invoke(target, null);
    }
}
