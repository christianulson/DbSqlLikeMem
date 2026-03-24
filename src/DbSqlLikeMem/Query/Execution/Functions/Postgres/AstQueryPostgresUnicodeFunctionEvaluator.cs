namespace DbSqlLikeMem;

using System;
using System.Text;

internal static class AstQueryPostgresUnicodeFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("NORMALIZE" or "TO_ASCII"))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (name is "NORMALIZE")
        {
            var formName = fn.Args.Count > 1
                ? (evalArg(1)?.ToString() ?? string.Empty).Trim().ToUpperInvariant()
                : "NFC";
            var form = formName switch
            {
                "" or "NFC" => NormalizationForm.FormC,
                "NFD" => NormalizationForm.FormD,
                "NFKC" => NormalizationForm.FormKC,
                "NFKD" => NormalizationForm.FormKD,
                _ => NormalizationForm.FormC
            };

            result = text.Normalize(form);
            return true;
        }

        result = AstQueryGeneralScalarFunctionEvaluator.ConvertToAscii(text);
        return true;
    }
}
