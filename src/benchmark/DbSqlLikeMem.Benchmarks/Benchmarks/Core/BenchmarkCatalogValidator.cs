using System.Reflection;
using System.Reflection.Emit;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Validates the benchmark catalog against the public benchmark suite surface.
/// PT: Valida o catalogo de benchmarks contra a superficie publica da suite de benchmark.
/// </summary>
internal static class BenchmarkCatalogValidator
{
    private static readonly IReadOnlyDictionary<ushort, OpCode> OpCodesByValue = typeof(OpCodes)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(static field => field.FieldType == typeof(OpCode))
        .Select(static field => (OpCode)field.GetValue(null)!)
        .ToDictionary(static opCode => unchecked((ushort)opCode.Value));

    /// <summary>
    /// EN: Validates benchmark feature and provider catalogs and the benchmark suite method mapping.
    /// PT: Valida os catalogos de recursos e provedores de benchmark e o mapeamento de metodos da suite.
    /// </summary>
    /// <returns>EN: Validation report with any detected issues. PT: Relatorio de validacao com eventuais problemas detectados.</returns>
    public static BenchmarkCatalogValidationReport Validate()
    {
        var issues = new List<string>();

        ValidateDistinctIds(FeatureCatalog.All.Select(static feature => feature.Id), "feature", issues);
        ValidateDistinctIds(ProviderCatalog.All.Select(static provider => provider.Id), "provider", issues);

        var suiteMethods = typeof(BenchmarkSuiteBase)
            .Assembly
            .GetTypes()
            .Where(static type => type.IsClass && !type.IsGenericTypeDefinition && typeof(BenchmarkSuiteBase).IsAssignableFrom(type))
            .Append(typeof(BenchmarkSuiteBase))
            .SelectMany(static type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Where(static method => method.GetCustomAttribute<BenchmarkAttribute>() is not null)
            .ToArray();

        var benchmarkFeatureIds = new HashSet<BenchmarkFeatureId>();
        foreach (var method in suiteMethods)
        {
            if (!TryGetBenchmarkFeatureId(method, out var featureId))
            {
                issues.Add($"Benchmark method '{method.Name}' does not map to a BenchmarkFeatureId.");
                continue;
            }

            benchmarkFeatureIds.Add(featureId);
        }

        var catalogFeatureIds = FeatureCatalog.All.Select(static feature => feature.Id).ToHashSet();
        foreach (var feature in FeatureCatalog.All.Where(static feature => feature.Comparable))
        {
            if (!benchmarkFeatureIds.Contains(feature.Id))
                issues.Add($"Comparable catalog feature '{feature.Id}' has no benchmark method on any benchmark suite.");
        }

        foreach (var featureId in benchmarkFeatureIds)
        {
            if (!catalogFeatureIds.Contains(featureId))
                issues.Add($"Benchmark method '{featureId}' is missing from FeatureCatalog.All.");
        }

        return new BenchmarkCatalogValidationReport(issues);
    }

    private static bool TryGetBenchmarkFeatureId(MethodInfo method, out BenchmarkFeatureId featureId)
    {
        featureId = default;

        var body = method.GetMethodBody();
        if (body is null)
        {
            return false;
        }

        var il = body.GetILAsByteArray();
        if (il is null || il.Length == 0)
        {
            return false;
        }

        int? lastIntegerConstant = null;
        for (var index = 0; index < il.Length;)
        {
            var opCode = ReadOpCode(il, ref index);
            if (opCode == default)
            {
                return false;
            }

            if (TryReadIntegerConstant(opCode, il, ref index, out var integerConstant))
            {
                lastIntegerConstant = integerConstant;
                continue;
            }

            if (opCode.OperandType == OperandType.InlineMethod)
            {
                var metadataToken = BitConverter.ToInt32(il, index);
                index += 4;

                MethodBase? targetMethod;
                try
                {
                    targetMethod = method.Module.ResolveMethod(metadataToken);
                }
                catch
                {
                    return false;
                }

                if (targetMethod is not null
                    && targetMethod.Name == nameof(BenchmarkSuiteBase.Run)
                    && lastIntegerConstant is not null
                    && Enum.IsDefined(typeof(BenchmarkFeatureId), lastIntegerConstant.Value))
                {
                    featureId = (BenchmarkFeatureId)lastIntegerConstant.Value;
                    return true;
                }

                lastIntegerConstant = null;
                continue;
            }

            SkipOperand(opCode, il, ref index);
        }

        return false;
    }

    private static OpCode ReadOpCode(byte[] il, ref int index)
    {
        var code = il[index++];
        if (code != 0xFE)
        {
            return OpCodesByValue.TryGetValue(code, out var opCode2) ? opCode2 : default;
        }

        if (index >= il.Length)
        {
            return default;
        }

        var twoByteCode = unchecked((ushort)(0xFE00 | il[index++]));
        return OpCodesByValue.TryGetValue(twoByteCode, out var opCode) ? opCode : default;
    }

    private static bool TryReadIntegerConstant(OpCode opCode, byte[] il, ref int index, out int value)
    {
        value = default;

        if (opCode == OpCodes.Ldc_I4_M1)
        {
            value = -1;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_0)
        {
            value = 0;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_1)
        {
            value = 1;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_2)
        {
            value = 2;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_3)
        {
            value = 3;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_4)
        {
            value = 4;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_5)
        {
            value = 5;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_6)
        {
            value = 6;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_7)
        {
            value = 7;
            return true;
        }

        if (opCode == OpCodes.Ldc_I4_8)
        {
            value = 8;
            return true;
        }

        switch (opCode.OperandType)
        {
            case OperandType.ShortInlineI:
                value = unchecked((sbyte)il[index]);
                index += 1;
                return true;
            case OperandType.InlineI:
                value = BitConverter.ToInt32(il, index);
                index += 4;
                return true;
            default:
                return false;
        }
    }

    private static void SkipOperand(OpCode opCode, byte[] il, ref int index)
    {
        switch (opCode.OperandType)
        {
            case OperandType.InlineNone:
                break;
            case OperandType.ShortInlineBrTarget:
            case OperandType.ShortInlineI:
            case OperandType.ShortInlineVar:
                index += 1;
                break;
            case OperandType.InlineVar:
                index += 2;
                break;
            case OperandType.InlineI:
            case OperandType.InlineBrTarget:
            case OperandType.InlineField:
            case OperandType.InlineMethod:
            case OperandType.InlineString:
            case OperandType.InlineSig:
            case OperandType.InlineTok:
            case OperandType.InlineType:
            case OperandType.InlineR:
                index += 4;
                break;
            case OperandType.InlineI8:
                index += 8;
                break;
            case OperandType.InlineSwitch:
                index += 4 + BitConverter.ToInt32(il, index) * 4;
                break;
            default:
                break;
        }
    }

    private static void ValidateDistinctIds<T>(
        IEnumerable<T> ids,
        string label,
        ICollection<string> issues)
        where T : notnull
    {
        var seen = new HashSet<T>();
        foreach (var id in ids)
        {
            if (seen.Add(id))
                continue;

            issues.Add($"Duplicate {label} id '{id}'.");
        }
    }
}

/// <summary>
/// EN: Represents the outcome of a benchmark catalog validation pass.
/// PT: Representa o resultado de uma validacao do catalogo de benchmark.
/// </summary>
/// <param name="Issues">EN: The validation issues, if any. PT: Os problemas de validacao, se existirem.</param>
public sealed record BenchmarkCatalogValidationReport(IReadOnlyList<string> Issues)
{
    /// <summary>
    /// EN: Gets a value that indicates whether the validation succeeded without issues.
    /// PT: Obtem um valor que indica se a validacao ocorreu sem problemas.
    /// </summary>
    public bool IsValid => Issues.Count == 0;

    /// <summary>
    /// EN: Formats the validation result for console output.
    /// PT: Formata o resultado da validacao para saida no console.
    /// </summary>
    /// <returns>EN: Formatted validation summary. PT: Resumo formatado da validacao.</returns>
    public string Format()
    {
        if (IsValid)
            return "Benchmark catalog validation succeeded.";

        var lines = new List<string> { "Benchmark catalog validation failed." };
        lines.AddRange(Issues.Select(issue => $"- {issue}"));
        return string.Join(Environment.NewLine, lines);
    }
}
