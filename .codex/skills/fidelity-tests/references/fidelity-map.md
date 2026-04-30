# Fidelity Map

## 1. Parameter Fidelity

- Type mapping
- Null handling
- Size and precision
- Input, output, and input-output direction
- Provider-specific binding names and placeholders

## 2. Result Fidelity

- Full rowset comparison
- Column order and aliases
- Row order
- `DBNull` versus null normalization
- Snapshot-based comparisons for relational queries
- Do not reshape input or output values inside the fidelity test just to mask provider differences.
- If the mock and container need to agree on value shape, move that normalization to the core or dialect.

## 3. Parser Fidelity

- Accepted syntax
- Rejected syntax
- Provider-specific keywords and operators
- Failure timing and error shape

## 4. Function Fidelity

- Date and time semantics
- JSON path and extraction behavior
- String functions and collation-sensitive behavior
- Window functions and aggregate behavior
- Join and lateral/apply behavior

## 5. Transaction Fidelity

- Begin, commit, and rollback
- Savepoint and release behavior
- Nested transaction flow

## 6. Exception Fidelity

- Same failure trigger as the real provider
- Same exception category when possible
- Extra debug context only when it does not change the contract

## 7. Dialect Placement

- Put provider rules in `ProviderSqlDialect`
- Override only what differs by provider
- Keep test bases free from provider switches when the dialect can express the rule

## 8. Suggested Priority

1. Parameters
2. Result shape
3. Parser
4. Functions
5. Exceptions
6. Transactions
