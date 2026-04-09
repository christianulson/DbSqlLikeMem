namespace DbSqlLikeMem;

/// <summary>
/// EN: Formats interception events as normalized text for logs and diagnostics.
/// PT: Formata eventos de interceptacao como texto normalizado para logs e diagnostico.
/// </summary>
public static class DbInterceptionEventFormatter
{
    /// <summary>
    /// EN: Formats one interception event as a structured text line.
    /// PT: Formata um evento de interceptacao como uma linha de texto estruturada.
    /// </summary>
    /// <param name="interceptionEvent">EN: Event to format. PT: Evento a formatar.</param>
    /// <returns>EN: Structured text line. PT: Linha de texto estruturada.</returns>
    public static string Format(DbInterceptionEvent interceptionEvent)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(interceptionEvent, nameof(interceptionEvent));

        var message = $"event={interceptionEvent.EventKind};state={interceptionEvent.ConnectionState}";

        if (!string.IsNullOrWhiteSpace(interceptionEvent.CommandText))
            message += $";sql={interceptionEvent.CommandText}";
        if (interceptionEvent.CommandExecutionKind is not null)
            message += $";commandKind={interceptionEvent.CommandExecutionKind}";
        if (interceptionEvent.TransactionOperationKind is not null)
            message += $";transactionKind={interceptionEvent.TransactionOperationKind}";
        if (interceptionEvent.IsolationLevel is not null)
            message += $";isolation={interceptionEvent.IsolationLevel}";
        if (interceptionEvent.Result is not null)
            message += $";result={interceptionEvent.Result}";
        if (!string.IsNullOrWhiteSpace(interceptionEvent.PerformanceMetrics))
            message += $";performance={interceptionEvent.PerformanceMetrics}";
        if (!string.IsNullOrWhiteSpace(interceptionEvent.PerformanceMetricsDelta))
            message += $";performanceDelta={interceptionEvent.PerformanceMetricsDelta}";
        if (interceptionEvent.Exception is not null)
            message += $";exception={interceptionEvent.Exception.Message}";

        return message;
    }
}
