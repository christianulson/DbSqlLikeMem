namespace DbSqlLikeMem;

using System;
using System.Linq;

internal static class AstQueryPostgresNetworkFunctionEvaluator
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
        if (name is not ("HOST" or "HOSTMASK" or "INET_SAME_FAMILY" or "MASKLEN" or "NETMASK" or "NETWORK"))
        {
            result = null;
            return false;
        }

        if (name is "INET_SAME_FAMILY")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("INET_SAME_FAMILY() espera dois enderecos.");

            var left = evalArg(0);
            var right = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            {
                result = null;
                return true;
            }

            if (!AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresInetValue(left, out var leftAddress, out _)
                || !AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresInetValue(right, out var rightAddress, out _))
            {
                result = null;
                return true;
            }

            result = leftAddress.AddressFamily == rightAddress.AddressFamily;
            return true;
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

        if (!AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresInetValue(value, out var address, out var prefixLength))
        {
            result = null;
            return true;
        }

        var byteLength = address.GetAddressBytes().Length;
        var maskBytes = AstQueryGeneralScalarFunctionEvaluator.BuildPrefixMaskBytes(byteLength, prefixLength);

        result = name switch
        {
            "HOST" => address.ToString(),
            "MASKLEN" => prefixLength,
            "NETMASK" => new System.Net.IPAddress(maskBytes).ToString(),
            "HOSTMASK" => new System.Net.IPAddress([.. maskBytes.Select(static b => (byte)~b)]).ToString(),
            "NETWORK" => $"{new System.Net.IPAddress(AstQueryGeneralScalarFunctionEvaluator.ApplyNetworkMask(address.GetAddressBytes(), maskBytes))}/{prefixLength}",
            _ => null
        };
        return true;
    }
}
