using DbSqlLikeMem.Benchmarks;
using Xunit;

namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies benchmark command-line profiles are parsed into the expected execution mode.
/// PT-br: Verifica se os perfis de linha de comando do benchmark sao analisados no modo de execucao esperado.
/// </summary>
public sealed class BenchmarkRunOptionsTests
{
    /// <summary>
    /// EN: Verifies the parser recognizes the supported benchmark profiles from both separated and inline forms.
    /// PT-br: Verifica se o parser reconhece os perfis de benchmark suportados nas formas separada e inline.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProfileParseCases))]
    public void Parse_ShouldRecognizeBenchmarkProfiles(string[] args, BenchmarkRunProfile expectedProfile)
    {
        var options = BenchmarkRunOptions.Parse(args);

        Assert.Equal(expectedProfile, options.Profile);
    }

    /// <summary>
    /// EN: Provides command-line profile cases for the benchmark parser.
    /// PT-br: Fornece casos de perfil de linha de comando para o parser do benchmark.
    /// </summary>
    public static IEnumerable<object[]> ProfileParseCases()
        => [
            Case(["--profile", "smoke"], BenchmarkRunProfile.Smoke),
            Case(["smoke"], BenchmarkRunProfile.Smoke),
            Case(["--profile=core"], BenchmarkRunProfile.Core),
            Case(["core"], BenchmarkRunProfile.Core),
            Case(["full"], BenchmarkRunProfile.Full),
            Case(["diagnostic"], BenchmarkRunProfile.Diagnostic)
        ];

    private static object[] Case(string[] args, BenchmarkRunProfile expectedProfile)
        => [args, expectedProfile];
}
