using DbSqlLikeMem.Models;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers serialization contract behavior for <see cref="ReadOnlyHashSet{T}"/>.
/// PT: Cobre o comportamento do contrato de serialização para <see cref="ReadOnlyHashSet{T}"/>.
/// </summary>
public sealed class ReadOnlyHashSetTests
{
    /// <summary>
    /// EN: Verifies GetObjectData fills serialization metadata for the wrapped set.
    /// PT: Verifica que GetObjectData preenche metadados de serialização para o conjunto encapsulado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void GetObjectData_ShouldPopulateSerializationInfo()
    {
        var set = new ReadOnlyHashSet<string>(["a", "b", "a"], StringComparer.OrdinalIgnoreCase);
#pragma warning disable SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
        var info = new SerializationInfo(typeof(HashSet<string>), new FormatterConverter());
        var context = new StreamingContext(StreamingContextStates.All);
#pragma warning restore SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.

        set.GetObjectData(info, context);

        Assert.True(info.MemberCount > 0);
    }

    /// <summary>
    /// EN: Verifies GetObjectData throws ArgumentNullException when SerializationInfo is null.
    /// PT: Verifica que GetObjectData lança ArgumentNullException quando SerializationInfo é nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void GetObjectData_ShouldThrow_WhenInfoIsNull()
    {
        var set = new ReadOnlyHashSet<int>([1, 2, 3]);

#pragma warning disable SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
        Assert.Throws<ArgumentNullException>(() => set.GetObjectData(null!, new StreamingContext(StreamingContextStates.All)));
#pragma warning restore SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
    }
}
