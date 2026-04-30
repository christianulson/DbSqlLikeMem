using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryInMembershipHelper
{
    private readonly record struct InMembershipState(bool Matched, bool HasNullCandidate);

    internal static object? EvaluateIn(
        this QueryExecutionContext context,
        InExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
    {
        var leftVal = eval(expression.Left, row, group, ctes);
        if (IsInLeftOperandNullish(leftVal))
            return IsInExpressionEmpty(expression, leftVal, row, ctes, getScalarLookup, getRowLookup) ? false : null;

        var membership = context.EvaluateInMembership(
            expression,
            leftVal!,
            row,
            group,
            ctes,
            eval,
            getScalarLookup,
            getRowLookup);

        if (membership.Matched)
            return true;

        return membership.HasNullCandidate ? null : false;
    }

    internal static object? EvaluateNotIn(
        this QueryExecutionContext context,
        InExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
    {
        var leftVal = eval(expression.Left, row, group, ctes);
        if (IsInLeftOperandNullish(leftVal))
            return IsInExpressionEmpty(expression, leftVal, row, ctes, getScalarLookup, getRowLookup) ? true : null;

        var membership = context.EvaluateInMembership(
            expression,
            leftVal!,
            row,
            group,
            ctes,
            eval,
            getScalarLookup,
            getRowLookup);

        if (membership.Matched)
            return false;

        return membership.HasNullCandidate ? null : true;
    }

    private static bool IsInLeftOperandNullish(object? value)
        => value is null or DBNull || (value is object?[] row && AstQueryBinarySupportHelper.HasNullElement(row));

    private static bool IsInExpressionEmpty(
        InExpr expression,
        object? leftVal,
        EvalRow row,
        IDictionary<string, Source> ctes,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
    {
        var items = expression.Items;
        var itemCount = items.Count;
        if (itemCount == 0)
            return true;

        if (itemCount == 1 && items[0] is SubqueryExpr subquery)
        {
            if (leftVal is object?[] leftRow)
            {
                var rowLookup = getRowLookup(subquery, row, ctes);
                return rowLookup.RowValues is null || rowLookup.RowValues.Count == 0;
            }

            var scalarLookup = getScalarLookup(subquery, row, ctes);
            return scalarLookup.Values.Count == 0;
        }

        return false;
    }

    private static InMembershipState EvaluateInMembership(
        this QueryExecutionContext context,
        InExpr expression,
        object leftVal,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
    {
        var hasNullCandidate = false;

        if (context.TryEvaluateInSubqueryMembership(expression, leftVal, row, ctes, eval, getScalarLookup, getRowLookup, ref hasNullCandidate, out var subqueryState))
            return subqueryState;

        var items = expression.Items;
        var itemCount = items.Count;
        for (var i = 0; i < itemCount; i++)
        {
            var candidate = eval(items[i], row, group, ctes);
            if (context.TryEvaluateEnumerableMembership(leftVal, candidate, ref hasNullCandidate, out var enumerableState))
            {
                if (enumerableState.Matched)
                    return enumerableState;

                continue;
            }

            if (context.TryEvaluateCandidateMembership(leftVal, candidate, ref hasNullCandidate, out var candidateState))
                return candidateState;
        }

        return new InMembershipState(Matched: false, HasNullCandidate: hasNullCandidate);
    }

    private static bool TryEvaluateInSubqueryMembership(
        this QueryExecutionContext context,
        InExpr expression,
        object leftVal,
        EvalRow row,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        _ = eval;
        state = default;
        var items = expression.Items;
        if (items.Count != 1 || items[0] is not SubqueryExpr subquery)
            return false;

        if (leftVal is object?[] leftRow)
        {
            var rowLookup = getRowLookup(subquery, row, ctes);
            hasNullCandidate |= rowLookup.HasNullCandidate;

            if (rowLookup.RowCandidates is not null
                && AstQuerySubqueryLookupSupport.TryBuildInLookupCompositeKey(leftRow, out var rowKey))
            {
                state = CreateMembershipState(rowLookup.RowCandidates.Contains(rowKey), hasNullCandidate);
                return true;
            }

            state = context.EvaluateRowMembershipCandidates(leftRow, rowLookup.RowValues ?? [], ref hasNullCandidate);
            return true;
        }

        var scalarLookup = getScalarLookup(subquery, row, ctes);
        hasNullCandidate |= scalarLookup.HasNullCandidate;

        if (scalarLookup.ScalarCandidates is not null
            && AstQuerySubqueryLookupSupport.TryCreateInLookupScalarKey(leftVal, context.Dialect, out var scalarKey))
        {
            state = CreateMembershipState(scalarLookup.ScalarCandidates.Contains(scalarKey), hasNullCandidate);
            return true;
        }

        state = context.EvaluateMembershipCandidates(leftVal, scalarLookup.Values, ref hasNullCandidate);
        return true;
    }

    private static bool TryEvaluateEnumerableMembership(
        this QueryExecutionContext context,
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        state = default;
        if (candidateValue is string)
            return false;

        var rowCandidateArray = candidateValue as object?[][];
        if (rowCandidateArray is not null
            && leftVal is object?[] leftRow)
        {
            state = context.EvaluateRowMembershipCandidates(leftRow, rowCandidateArray, ref hasNullCandidate);
            return true;
        }

        if (candidateValue is object?[] candidateArray
            && rowCandidateArray is null)
        {
            state = context.EvaluateMembershipCandidates(leftVal, candidateArray, ref hasNullCandidate);
            return true;
        }

        if (candidateValue is IReadOnlyList<object?[]> rowCandidateList
            && leftVal is object?[] leftRowList)
        {
            state = context.EvaluateRowMembershipCandidates(leftRowList, rowCandidateList, ref hasNullCandidate);
            return true;
        }

        if (candidateValue is IReadOnlyList<object?> candidateList)
        {
            state = context.EvaluateMembershipCandidates(leftVal, candidateList, ref hasNullCandidate);
            return true;
        }

        if (candidateValue is not IEnumerable enumerable)
            return false;

        state = context.EvaluateMembershipCandidates(leftVal, enumerable, ref hasNullCandidate);
        return true;
    }

    private static InMembershipState EvaluateMembershipCandidates(
        this QueryExecutionContext context,
        object leftVal,
        IEnumerable candidates,
        ref bool hasNullCandidate)
    {
        if (candidates is IReadOnlyList<object?> candidateList)
        {
            var candidateCount = candidateList.Count;
            for (var i = 0; i < candidateCount; i++)
            {
                var candidate = candidateList[i];
                if (context.TryEvaluateCandidateMembership(leftVal, candidate, ref hasNullCandidate, out var state))
                    return state;
            }

            return CreateMembershipState(matched: false, hasNullCandidate);
        }

        foreach (var candidate in candidates)
        {
            if (context.TryEvaluateCandidateMembership(leftVal, candidate, ref hasNullCandidate, out var state))
                return state;
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static InMembershipState EvaluateMembershipCandidates(
        this QueryExecutionContext context,
        object leftVal,
        object?[] candidates,
        ref bool hasNullCandidate)
    {
        var candidateCount = candidates.Length;
        for (var i = 0; i < candidateCount; i++)
        {
            var candidate = candidates[i];
            if (context.TryEvaluateCandidateMembership(leftVal, candidate, ref hasNullCandidate, out var state))
                return state;
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static InMembershipState EvaluateRowMembershipCandidates(
        this QueryExecutionContext context,
        object?[] leftRow,
        IEnumerable<object?[]> candidates,
        ref bool hasNullCandidate)
    {
        if (candidates is IReadOnlyList<object?[]> candidateList)
        {
            var candidateCount = candidateList.Count;
            for (var i = 0; i < candidateCount; i++)
            {
                var candidate = candidateList[i];
                if (context.TryEvaluateRowCandidateMembership(leftRow, candidate, ref hasNullCandidate, out var state)
                    && state.Matched)
                {
                    return state;
                }
            }

            return CreateMembershipState(matched: false, hasNullCandidate);
        }

        foreach (var candidate in candidates)
        {
            if (context.TryEvaluateRowCandidateMembership(leftRow, candidate, ref hasNullCandidate, out var state)
                && state.Matched)
            {
                return state;
            }
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static InMembershipState EvaluateRowMembershipCandidates(
        this QueryExecutionContext context,
        object?[] leftRow,
        object?[][] candidates,
        ref bool hasNullCandidate)
    {
        var candidateCount = candidates.Length;
        for (var i = 0; i < candidateCount; i++)
        {
            var candidate = candidates[i];
            if (context.TryEvaluateRowCandidateMembership(leftRow, candidate, ref hasNullCandidate, out var state)
                && state.Matched)
            {
                return state;
            }
        }

        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static bool TryEvaluateCandidateMembership(
        this QueryExecutionContext context,
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        if (AstQueryBinarySupportHelper.IsSqlNullLike(candidateValue))
        {
            state = RegisterNullCandidate(ref hasNullCandidate);
            return false;
        }

        if (context.TryEvaluateRowCandidateMembership(leftVal, candidateValue, ref hasNullCandidate, out state))
            return state.Matched;

        state = CreateMembershipState(leftVal.EqualsSql(candidateValue, context), hasNullCandidate);
        return state.Matched;
    }

    private static bool TryEvaluateRowCandidateMembership(
        this QueryExecutionContext context,
        object leftVal,
        object? candidateValue,
        ref bool hasNullCandidate,
        out InMembershipState state)
    {
        state = default;
        if (leftVal is not object?[] leftRow || candidateValue is not object?[] rightRow)
            return false;

        if (AstQueryBinarySupportHelper.HasNullElement(leftRow) || AstQueryBinarySupportHelper.HasNullElement(rightRow))
        {
            state = RegisterNullCandidate(ref hasNullCandidate);
            return true;
        }

        state = CreateMembershipState(context.RowValuesMatch(leftRow, rightRow), hasNullCandidate);
        return true;
    }

    private static InMembershipState RegisterNullCandidate(ref bool hasNullCandidate)
    {
        hasNullCandidate = true;
        return CreateMembershipState(matched: false, hasNullCandidate);
    }

    private static InMembershipState CreateMembershipState(bool matched, bool hasNullCandidate)
        => new(matched, hasNullCandidate);

    private static bool RowValuesMatch(this QueryExecutionContext context, object?[] left, object?[] right)
    {
        var leftLength = left.Length;
        if (leftLength != right.Length)
            return false;

        for (var i = 0; i < leftLength; i++)
        {
            if (!left[i].EqualsSql(right[i], context))
                return false;
        }

        return true;
    }
}
