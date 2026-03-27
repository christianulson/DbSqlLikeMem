namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Linq;

internal delegate bool AstQueryTryEvalPostgresNetworkFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresNetworkFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresNetworkFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        return handler(context,fn,  evalArg, out result);
    }

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresNetworkFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresNetworkFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalHostFunction, "HOST");
        Register(handlers, TryEvalHostMaskFunction, "HOSTMASK");
        Register(handlers, TryEvalInetSameFamilyFunction, "INET_SAME_FAMILY");
        Register(handlers, TryEvalMaskLenFunction, "MASKLEN");
        Register(handlers, TryEvalNetmaskFunction, "NETMASK");
        Register(handlers, TryEvalNetworkFunction, "NETWORK");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresNetworkFunction> handlers,
        AstQueryTryEvalPostgresNetworkFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalInetSameFamilyFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("INET_SAME_FAMILY() espera dois enderecos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
        {
            result = null;
            return true;
        }

        if (!TryParsePostgresInetValue(left, out var leftAddress, out _)
            || !TryParsePostgresInetValue(right, out var rightAddress, out _))
        {
            result = null;
            return true;
        }

        result = leftAddress.AddressFamily == rightAddress.AddressFamily;
        return true;
    }

    private static bool TryEvalHostFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalInetAddressFunction(context,fn,  evalArg, value => value.Address.ToString(), out result);

    private static bool TryEvalHostMaskFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalInetAddressFunction(
            context,
            fn,
            evalArg,
            value => new IPAddress([.. value.MaskBytes.Select(static b => (byte)~b)]).ToString(),
            out result);

    private static bool TryEvalMaskLenFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalInetAddressFunction(context, fn, evalArg, value => value.PrefixLength, out result);

    private static bool TryEvalNetmaskFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalInetAddressFunction(
            context,
            fn,
            evalArg,
            value => new IPAddress(value.MaskBytes).ToString(),
            out result);

    private static bool TryEvalNetworkFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalInetAddressFunction(
            context,
            fn,
            evalArg,
            value => $"{new IPAddress(ApplyNetworkMask(value.Address.GetAddressBytes(), value.MaskBytes))}/{value.PrefixLength}",
            out result);

    private static bool TryEvalInetAddressFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        Func<(IPAddress Address, byte[] MaskBytes, int PrefixLength), object?> transform,
        out object? result)
    {
        _ = context;

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

        if (!TryParsePostgresInetValue(value, out var address, out var prefixLength))
        {
            result = null;
            return true;
        }

        var byteLength = address.GetAddressBytes().Length;
        var maskBytes = BuildPrefixMaskBytes(byteLength, prefixLength);
        result = transform((address, maskBytes, prefixLength));
        return true;
    }

    private static byte[] BuildPrefixMaskBytes(int byteLength, int prefixLength)
    {
        var mask = new byte[byteLength];
        for (var i = 0; i < byteLength; i++)
        {
            var remainingBits = prefixLength - (i * 8);
            mask[i] = remainingBits switch
            {
                >= 8 => 0xFF,
                <= 0 => 0x00,
                _ => (byte)(0xFF << (8 - remainingBits))
            };
        }

        return mask;
    }

    private static bool TryParsePostgresInetValue(object? value, out IPAddress address, out int prefixLength)
    {
        address = IPAddress.None;
        prefixLength = 0;

        var text = value?.ToString()?.Trim() ?? string.Empty;
        if (string.IsNullOrEmpty(text))
            return false;

        var slashIndex = text.IndexOf('/');
        var addressText = slashIndex >= 0 ? text[..slashIndex] : text;
        if (!IPAddress.TryParse(addressText, out var parsedAddress))
            return false;

        address = parsedAddress;

        var maxPrefix = address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 32 : 128;
        if (slashIndex < 0)
        {
            prefixLength = maxPrefix;
            return true;
        }

        var prefixText = text[(slashIndex + 1)..];
        if (!int.TryParse(prefixText, NumberStyles.Integer, CultureInfo.InvariantCulture, out prefixLength))
            return false;

        return prefixLength >= 0 && prefixLength <= maxPrefix;
    }

    private static byte[] ApplyNetworkMask(byte[] addressBytes, byte[] maskBytes)
    {
        var networkBytes = new byte[addressBytes.Length];
        for (var i = 0; i < addressBytes.Length; i++)
            networkBytes[i] = (byte)(addressBytes[i] & maskBytes[i]);

        return networkBytes;
    }
}
