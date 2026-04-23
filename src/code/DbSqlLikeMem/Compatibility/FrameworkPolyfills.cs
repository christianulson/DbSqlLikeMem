using System.Diagnostics;

#if NET462 || NETSTANDARD2_0

namespace System.Runtime.CompilerServices
{
    /// <summary>
    /// EN: Provides the compiler marker type used by init-only setters on older target frameworks.
    /// PT: Fornece o tipo marcador do compilador usado por setters init-only em frameworks mais antigos.
    /// </summary>
    internal static class IsExternalInit
    {
    }
}
#endif

#if NET462 || NETSTANDARD2_0

namespace System.Runtime.CompilerServices
{
    /// <summary>
    /// EN: Provides the compiler marker attribute used for required members on older target frameworks.
    /// PT: Fornece o atributo marcador do compilador usado para membros obrigatorios em frameworks mais antigos.
    /// </summary>
    [AttributeUsage(AttributeTargets.All, Inherited = false)]
    public sealed class RequiredMemberAttribute : Attribute
    {
    }

    /// <summary>
    /// EN: Provides the compiler marker attribute used to annotate required language features on older target frameworks.
    /// PT: Fornece o atributo marcador do compilador usado para anotar recursos de linguagem obrigatorios em frameworks mais antigos.
    /// </summary>
    /// <remarks>
    /// EN: Stores the name of the required compiler feature.
    /// PT: Armazena o nome do recurso de compilador requerido.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    public sealed class CompilerFeatureRequiredAttribute(string featureName) : Attribute
    {

        /// <summary>
        /// EN: Gets the name of the required compiler feature.
        /// PT: Obtém o nome do recurso de compilador requerido.
        /// </summary>
        public string FeatureName { get; } = featureName;

        /// <summary>
        /// EN: Gets or sets whether the compiler feature is optional.
        /// PT: Obtém ou define se o recurso de compilador é opcional.
        /// </summary>
        public bool IsOptional { get; init; }

        /// <summary>
        /// EN: Identifies the ref structs compiler feature.
        /// PT: Identifica o recurso de compilador ref structs.
        /// </summary>
        public const string RefStructs = nameof(RefStructs);

        /// <summary>
        /// EN: Identifies the required members compiler feature.
        /// PT: Identifica o recurso de compilador required members.
        /// </summary>
        public const string RequiredMembers = nameof(RequiredMembers);
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// EN: Provides the compiler marker attribute used to indicate that a constructor sets all required members.
    /// PT: Fornece o atributo marcador do compilador usado para indicar que um construtor define todos os membros obrigatorios.
    /// </summary>
    [AttributeUsage(AttributeTargets.Constructor, Inherited = false)]
    public sealed class SetsRequiredMembersAttribute : Attribute
    {
    }
}
#endif

#if NET462 || NETSTANDARD2_0
namespace System
{
    /// <summary>
    /// EN: Provides a HashCode implementation for target frameworks that do not include the BCL type.
    /// PT: Fornece uma implementacao de HashCode para frameworks alvo que nao incluem o tipo da BCL.
    /// </summary>
    public struct HashCode
    {
        private int _value;

        /// <summary>
        /// EN: Adds a value to the running hash code.
        /// PT: Adiciona um valor ao hash code acumulado.
        /// </summary>
        public void Add<T>(T value)
        {
            var itemHash = value?.GetHashCode() ?? 0;
            unchecked
            {
                _value = (_value * 31) + itemHash;
            }
        }

        /// <summary>
        /// EN: Gets the final hash code value.
        /// PT: Obtém o valor final do hash code.
        /// </summary>
        public readonly int ToHashCode() => _value;
    }
}
#endif

#if NET462 || NETSTANDARD2_0

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// EN: Provides the compiler attribute used to allow null values in annotated members on older target frameworks.
    /// PT: Fornece o atributo do compilador usado para permitir valores nulos em membros anotados em frameworks mais antigos.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Parameter | AttributeTargets.Property, Inherited = false)]
    public sealed class AllowNullAttribute : Attribute
    {
    }
}
#endif

#if NET462

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// EN: Provides the compiler attribute used to indicate that a value may be null for a specific return condition.
    /// PT: Fornece o atributo do compilador usado para indicar que um valor pode ser nulo em uma condicao de retorno especifica.
    /// </summary>
    /// <remarks>
    /// EN: Stores the return-value condition that controls the nullability annotation.
    /// PT: Armazena a condicao de retorno que controla a anotacao de nulabilidade.
    /// </remarks>
    /// <param name="returnValue">EN: Return condition used by the annotation. PT: Condicao de retorno usada pela anotacao.</param>
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    public sealed class MaybeNullWhenAttribute(bool returnValue) : Attribute
    {

        /// <summary>
        /// EN: Gets the return condition used by the annotation.
        /// PT: Obtém a condicao de retorno usada pela anotacao.
        /// </summary>
        public bool ReturnValue { get; } = returnValue;
    }
}
#endif

namespace DbSqlLikeMem
{
    /// <summary>
    /// EN: Provides helper methods for validating arguments to ensure they are not null.
    /// PT: Fornece helpers para validar argumentos e garantir que nao sejam nulos.
    /// </summary>
    /// <remarks>This class is intended to support argument validation in a manner compatible with .NET's
    /// standard exception-throwing patterns. It can be used to enforce null checks in APIs that target multiple .NET
    /// versions or require explicit argument validation.</remarks>
    public static class ArgumentNullExceptionCompatible
    {
        /// <summary>
        /// EN: Throws an ArgumentNullException when the value is null.
        /// PT: Lanca ArgumentNullException quando o valor e nulo.
        /// </summary>
        /// <param name="value">EN: Object value to validate. PT: Valor do objeto a validar.</param>
        /// <param name="paramName">EN: Parameter name used in the exception. PT: Nome do parametro usado na excecao.</param>
        public static void ThrowIfNull(object? value, string paramName)
        {
#if NET6_0_OR_GREATER
            ArgumentNullException.ThrowIfNull(value, paramName);
#else
            if (value is null)
                throw new ArgumentNullException(paramName);
#endif
        }
    }

    /// <summary>
    /// EN: Provides helper methods for validating method arguments in a manner compatible with standard .NET exception patterns.
    /// PT: Fornece helpers para validar argumentos de metodo de forma compativel com os padroes de excecao do .NET.
    /// </summary>
    /// <remarks>This class contains static methods that throw exceptions when argument values do not meet
    /// expected conditions. These methods are intended to simplify argument validation and ensure consistent exception
    /// handling across different codebases.</remarks>
    public static class ArgumentExceptionCompatible
    {
        /// <summary>
        /// EN: Throws an ArgumentException when the string is null, empty, or white-space.
        /// PT: Lanca ArgumentException quando a string e nula, vazia ou apenas espacos.
        /// </summary>
        /// <param name="value">EN: String value to validate. PT: Valor de string a validar.</param>
        /// <param name="paramName">EN: Parameter name used in the exception. PT: Nome do parametro usado na excecao.</param>
        public static void ThrowIfNullOrWhiteSpace(string? value, string paramName)
        {
#if NET6_0_OR_GREATER
            ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
#else
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException(paramName);
#endif
        }
    }

    /// <summary>
    /// EN: Provides clamp helpers for comparable values.
    /// PT: Fornece helpers de clamp para valores comparaveis.
    /// </summary>
    public static class ClampExtensions
    {
        /// <summary>
        /// EN: Clamps a value to the inclusive range defined by the minimum and maximum values.
        /// PT: Limita um valor ao intervalo inclusivo definido pelos valores minimo e maximo.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public static T Clamp<T>(this T value, T min, T max)
            where T : IComparable<T>
        {
            if (value.CompareTo(min) < 0) return min;
            if (value.CompareTo(max) > 0) return max;
            return value;
        }
    }

    /// <summary>
    /// EN: Provides framework-compatible elapsed-ticks calculations from Stopwatch timestamps.
    /// PT: Fornece calculos de ticks decorridos compativeis com o framework a partir de timestamps do Stopwatch.
    /// </summary>
    public static class StopwatchCompatible
    {
        /// <summary>
        /// EN: Converts a start timestamp from Stopwatch.GetTimestamp to elapsed TimeSpan ticks.
        /// PT: Converte um timestamp inicial de Stopwatch.GetTimestamp em ticks decorridos de TimeSpan.
        /// </summary>
        public static long GetElapsedTicks(long startTimestamp)
        {
            var delta = Stopwatch.GetTimestamp() - startTimestamp;
            return (delta * TimeSpan.TicksPerSecond) / Stopwatch.Frequency;
        }
    }
}


