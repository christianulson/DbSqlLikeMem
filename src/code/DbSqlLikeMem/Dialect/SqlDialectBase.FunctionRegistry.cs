using System;
using System.Linq;

namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    protected bool TryGetFunctionDefinition(string functionName, out DbFunctionDef? definition)
    {
        definition = null;

        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return Functions.TryGetValue(functionName, out definition)
            && definition is not null;
    }

    public virtual bool TryGetScalarFunctionDefinition(string functionName, out DbFunctionDef? definition)
        => TryGetFunctionDefinition(functionName, out definition)
            && definition!.HasCapability(DbFunctionCapability.Scalar);

    public virtual bool TryGetScalarFunctionDefinition(FunctionCallExpr functionCall, out DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(functionCall, nameof(functionCall));

        if (functionCall.ResolvedScalarFunction is not null)
        {
            definition = functionCall.ResolvedScalarFunction;
            return true;
        }

        return TryGetScalarFunctionDefinition(functionCall.Name, out definition);
    }

    public virtual bool TryGetScalarFunctionDefinition(CallExpr functionCall, out DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(functionCall, nameof(functionCall));

        if (functionCall.ResolvedScalarFunction is not null)
        {
            definition = functionCall.ResolvedScalarFunction;
            return true;
        }

        return TryGetScalarFunctionDefinition(functionCall.Name, out definition);
    }

    public virtual bool TryGetTableFunctionDefinition(string functionName, out DbFunctionDef? definition)
        => TryGetFunctionDefinition(functionName, out definition)
            && definition!.HasCapability(DbFunctionCapability.Table);

    public virtual bool TryGetTableFunctionDefinition(FunctionCallExpr functionCall, out DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(functionCall, nameof(functionCall));

        if (functionCall.ResolvedTableFunction is not null)
        {
            definition = functionCall.ResolvedTableFunction;
            return true;
        }

        return TryGetTableFunctionDefinition(functionCall.Name, out definition);
    }

    public virtual bool TryGetWindowFunctionDefinition(string functionName, out DbFunctionDef? definition)
        => TryGetFunctionDefinition(functionName, out definition)
            && definition!.HasCapability(DbFunctionCapability.Window);

    public virtual bool TryGetWindowFunctionDefinition(WindowFunctionExpr windowFunction, out DbFunctionDef? definition)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunction, nameof(windowFunction));

        if (windowFunction.ResolvedWindowFunction is not null)
        {
            definition = windowFunction.ResolvedWindowFunction;
            return true;
        }

        return TryGetWindowFunctionDefinition(windowFunction.Name, out definition);
    }

    public virtual bool TryGetTemporalFunctionKind(string functionName, out SqlTemporalFunctionKind kind)
    {
        if (TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.TemporalKind is SqlTemporalFunctionKind temporalKind)
        {
            kind = temporalKind;
            return true;
        }

        return TemporalFunctionNames.TryGetValue(functionName, out kind);
    }

    public virtual bool AllowsTemporalIdentifier(string functionName)
    {
        if (TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.TemporalKind is not null)
        {
            return definition.AllowsIdentifier;
        }

        return TemporalFunctionIdentifierNames.Any(token => token.Equals(functionName, StringComparison.OrdinalIgnoreCase));
    }

    public virtual bool AllowsTemporalCall(string functionName)
    {
        if (TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.TemporalKind is not null)
        {
            return definition.AllowsCall;
        }

        return TemporalFunctionCallNames.Any(token => token.Equals(functionName, StringComparison.OrdinalIgnoreCase));
    }

    public virtual bool TryGetAggregateFunctionDefinition(string functionName, out DbFunctionDef? definition)
        => TryGetFunctionDefinition(functionName, out definition)
            && definition!.HasCapability(DbFunctionCapability.Aggregate);
}
