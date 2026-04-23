using System.Diagnostics;

#if NET462 || NETSTANDARD2_0

namespace System.Runtime.CompilerServices
{
    /// <summary>
    /// IsExternalInit - Compatibility
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
    /// RequiredMemberAttribute - Compatibility
    /// </summary>
    [AttributeUsage(AttributeTargets.All, Inherited = false)]
    public sealed class RequiredMemberAttribute : Attribute
    {
    }

    /// <summary>
    /// CompilerFeatureRequiredAttribute - Compatibility
    /// </summary>
    /// <remarks>
    /// CompilerFeatureRequiredAttribute - Compatibility
    /// </remarks>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    public sealed class CompilerFeatureRequiredAttribute(string featureName) : Attribute
    {

        /// <summary>
        /// FeatureName - Compatibility
        /// </summary>
        public string FeatureName { get; } = featureName;

        /// <summary>
        /// IsOptional - Compatibility
        /// </summary>
        public bool IsOptional { get; init; }

        /// <summary>
        /// RefStructs - Compatibility
        /// </summary>
        public const string RefStructs = nameof(RefStructs);

        /// <summary>
        /// RequiredMembers - Compatibility
        /// </summary>
        public const string RequiredMembers = nameof(RequiredMembers);
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// SetsRequiredMembersAttribute - Compatibility
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
    /// HashCode - Compatibility
    /// </summary>
    public struct HashCode
    {
        private int _value;

        /// <summary>
        /// HashCode.Add - Compatibility
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
        /// HashCode.ToHashCode - Compatibility
        /// </summary>
        public readonly int ToHashCode() => _value;
    }
}
#endif

#if NET462 || NETSTANDARD2_0

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// AllowNullAttribute - Compatibility
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
    /// MaybeNullWhenAttribute - Compatibility
    /// </summary>
    /// <remarks>
    /// MaybeNullWhenAttribute - Compatibility
    /// </remarks>
    /// <param name="returnValue"></param>
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    public sealed class MaybeNullWhenAttribute(bool returnValue) : Attribute
    {

        /// <summary>
        /// ReturnValue - Compatibility
        /// </summary>
        public bool ReturnValue { get; } = returnValue;
    }
}
#endif

namespace DbSqlLikeMem
{
    /// <summary>
    /// Provides helper methods for validating arguments to ensure they are not null.
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
    /// Provides helper methods for validating method arguments in a manner compatible with standard .NET exception
    /// patterns.
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
    /// Clamp
    /// </summary>
    public static class ClampExtensions
    {
        /// <summary>
        /// Clamp
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
    /// Provides framework-compatible elapsed-ticks calculations from Stopwatch timestamps.
    /// </summary>
    public static class StopwatchCompatible
    {
        /// <summary>
        /// Converts a start timestamp (from Stopwatch.GetTimestamp) to elapsed TimeSpan ticks.
        /// </summary>
        public static long GetElapsedTicks(long startTimestamp)
        {
            var delta = Stopwatch.GetTimestamp() - startTimestamp;
            return (delta * TimeSpan.TicksPerSecond) / Stopwatch.Frequency;
        }
    }
}


