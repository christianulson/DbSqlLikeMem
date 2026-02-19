#if NET48 || NETSTANDARD2_1

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

#if NET48 || NETSTANDARD2_1 || NET6_0

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
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    public sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        /// <summary>
        /// CompilerFeatureRequiredAttribute - Compatibility
        /// </summary>
        public CompilerFeatureRequiredAttribute(string featureName)
        {
            FeatureName = featureName;
        }

        /// <summary>
        /// FeatureName - Compatibility
        /// </summary>
        public string FeatureName { get; }

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

#if NET48

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

#if NET48

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// MaybeNullWhenAttribute - Compatibility
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    public sealed class MaybeNullWhenAttribute : Attribute
    {
        /// <summary>
        /// MaybeNullWhenAttribute - Compatibility
        /// </summary>
        /// <param name="returnValue"></param>
        public MaybeNullWhenAttribute(bool returnValue)
        {
            ReturnValue = returnValue;
        }

        /// <summary>
        /// ReturnValue - Compatibility
        /// </summary>
        public bool ReturnValue { get; }
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
        /// Throws an ArgumentNullException if the specified value is null.
        /// </summary>
        /// <param name="value">The object to validate for nullity.</param>
        /// <param name="paramName">The name of the parameter being validated. Used in the exception if thrown.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="value"/> is null.</exception>
        public static void ThrowIfNull(object? value, string paramName)
        {
            if (value is null)
                throw new ArgumentNullException(paramName);
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
        /// Throws an exception if the specified string is null, empty, or consists only of white-space characters.
        /// </summary>
        /// <param name="value">The string value to validate. Can be null, empty, or contain only white-space characters.</param>
        /// <param name="paramName">The name of the parameter to include in the exception if <paramref name="value"/> is null, empty, or white
        /// space.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="value"/> is null, empty, or consists only of white-space characters.</exception>
        public static void ThrowIfNullOrWhiteSpace(string? value, string paramName)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException(paramName);
        }
    }
}
