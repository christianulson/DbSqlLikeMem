using DbSqlLikeMem.Models;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers serialization contract behavior for <see cref="ReadOnlyHashSet{T}"/>.
/// PT: Cobre o comportamento do contrato de serialização para <see cref="ReadOnlyHashSet{T}"/>.
/// </summary>
public sealed class ReadOnlyHashSetTests
{
    [Fact]
    [Trait("Category", "Core")]
    public void GetObjectData_ShouldPopulateSerializationInfo()
    {
        var set = new ReadOnlyHashSet<string>(["a", "b", "a"], StringComparer.OrdinalIgnoreCase);
        var info = new SerializationInfo(typeof(HashSet<string>), new FormatterConverter());
        var context = new StreamingContext(StreamingContextStates.All);

        set.GetObjectData(info, context);

        Assert.True(info.MemberCount > 0);
    }

    [Fact]
    [Trait("Category", "Core")]
    public void GetObjectData_ShouldThrow_WhenInfoIsNull()
    {
        var set = new ReadOnlyHashSet<int>([1, 2, 3]);

        Assert.Throws<ArgumentNullException>(() => set.GetObjectData(null!, new StreamingContext(StreamingContextStates.All)));
    }
}
