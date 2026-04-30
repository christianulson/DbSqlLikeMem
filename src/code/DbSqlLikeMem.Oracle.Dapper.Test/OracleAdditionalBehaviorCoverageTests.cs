using System.Text.Json;

namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers additional Oracle behavior scenarios for the Dapper provider.
/// PT: Cobre cenarios adicionais de comportamento Oracle para o provedor Dapper.
/// </summary>
public sealed class OracleAdditionalBehaviorCoverageTests(
    ITestOutputHelper helper
) : AdditionalBehaviorCoverageTestsBase<OracleDbMock, OracleConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies IS NULL and IS NOT NULL predicates return the expected rows.
    /// PT: Verifica se os predicados IS NULL e IS NOT NULL retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Where_IsNull_And_IsNotNull_ShouldWork_Test() => Where_IsNull_And_IsNotNull_ShouldWork();

    /// <summary>
    /// EN: Verifies equality comparisons against NULL return no rows.
    /// PT: Verifica se comparacoes de igualdade com NULL nao retornam linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Where_EqualNull_ShouldReturnNoRows_Test() => Where_EqualNull_ShouldReturnNoRows();

    /// <summary>
    /// EN: Verifies left joins preserve left-side rows when there is no matching right-side row.
    /// PT: Verifica se left joins preservam as linhas da esquerda quando nao ha linha correspondente na direita.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void LeftJoin_ShouldPreserveLeftRows_WhenNoMatch_Test() => LeftJoin_ShouldPreserveLeftRows_WhenNoMatch();

    /// <summary>
    /// EN: Verifies mixed descending and ascending ordering is deterministic.
    /// PT: Verifica se a ordenacao mista decrescente e crescente e deterministica.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void OrderBy_Desc_ThenAsc_ShouldBeDeterministic_Test() => OrderBy_Desc_ThenAsc_ShouldBeDeterministic();

    /// <summary>
    /// EN: Verifies COUNT(*) and COUNT(column) handle null values differently as expected.
    /// PT: Verifica se COUNT(*) e COUNT(coluna) tratam valores nulos de forma diferente conforme esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls_Test() => Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls();

    /// <summary>
    /// EN: Verifies HAVING filters grouped results correctly.
    /// PT: Verifica se HAVING filtra corretamente os resultados agrupados.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Having_ShouldFilterGroups_Test() => Having_ShouldFilterGroups();

    /// <summary>
    /// EN: Verifies parameter lists work correctly in IN predicates.
    /// PT: Verifica se listas de parametros funcionam corretamente em predicados IN.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Where_In_WithParameterList_ShouldWork_Test() => Where_In_WithParameterList_ShouldWork();

    /// <summary>
    /// EN: Verifies inserts map values correctly when columns are specified out of order.
    /// PT: Verifica se insercoes mapeiam valores corretamente quando as colunas sao informadas fora de ordem.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Insert_WithColumnsOutOfOrder_ShouldMapCorrectly_Test() => Insert_WithColumnsOutOfOrder_ShouldMapCorrectly();

    /// <summary>
    /// EN: Verifies binary keys round-trip through Dapper when the payload starts as Guid bytes.
    /// PT: Verifica se chaves binarias retornam pelo Dapper quando o payload comeca como bytes de Guid.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void BinaryGuidPrimaryKey_ShouldRoundTripThroughDapper_Test() => BinaryGuidPrimaryKey_ShouldRoundTripThroughDapper();

    /// <summary>
    /// EN: Verifies deletes using an IN parameter list remove the expected rows.
    /// PT: Verifica se deletes usando uma lista de parametros em IN removem as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Delete_WithInParameterList_ShouldDeleteMatchingRows_Test() => Delete_WithInParameterList_ShouldDeleteMatchingRows();

    /// <summary>
    /// EN: Verifies update set expressions can reference the current column value correctly.
    /// PT: Verifica se expressoes SET em updates podem referenciar corretamente o valor atual da coluna.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Update_SetExpression_ShouldUpdateRows_Test() => Update_SetExpression_ShouldUpdateRows();

    /// <summary>
    /// EN: Verifies UPDATE statements can match rows through an IN subquery that uses a table alias.
    /// PT: Verifica se UPDATEs podem casar linhas por meio de uma subquery IN que usa alias de tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void Update_WithInSubqueryAndAlias_ShouldUpdateMatchingRows_Test()
    {
        var db = CreateDb();

        var wallet = db.AddTable("wallet");
        wallet.AddColumn("wlt_id", DbType.Int64, false);
        wallet.AddColumn("ven_id", DbType.Int16, false);
        wallet.AddColumn("usr_id", DbType.Int64, false);
        wallet.AddColumn("wlt_deviceid", DbType.String, false);
        wallet.AddColumn("wlt_status", DbType.String, false);

        var hotlist = db.AddTable("wallethotlist");
        hotlist.AddColumn("wlthot_id", DbType.Int64, false);
        hotlist.AddColumn("wlt_id", DbType.Int64, false);
        hotlist.AddColumn("wlthot_status", DbType.String, false);
        hotlist.AddColumn("wlthot_dtcreated", DbType.DateTime, false);
        hotlist.AddColumn("wlthot_dtdeleted", DbType.DateTime, true);

        wallet.Add(new Dictionary<int, object?>
        {
            { 0, 101L },
            { 1, (short)7 },
            { 2, 1L },
            { 3, "DEVICE-1" },
            { 4, "A" }
        });

        wallet.Add(new Dictionary<int, object?>
        {
            { 0, 202L },
            { 1, (short)9 },
            { 2, 1L },
            { 3, "DEVICE-2" },
            { 4, "A" }
        });

        var created = new DateTime(2026, 4, 30, 10, 0, 0, DateTimeKind.Utc);
        var untouchedCreated = new DateTime(2026, 4, 30, 11, 0, 0, DateTimeKind.Utc);

        hotlist.Add(new Dictionary<int, object?>
        {
            { 0, 1L },
            { 1, 101L },
            { 2, "A" },
            { 3, created },
            { 4, null }
        });

        hotlist.Add(new Dictionary<int, object?>
        {
            { 0, 2L },
            { 1, 202L },
            { 2, "A" },
            { 3, untouchedCreated },
            { 4, null }
        });

        using var connection = CreateConnection(db);
        connection.Open();

        var affected = connection.Execute(
            @"
UPDATE WALLETHOTLIST
   SET WLTHOT_DTDELETED = :dateTime
     , WLTHOT_Status = 'I'
 WHERE WLT_ID IN (
    SELECT W.WLT_ID
      FROM WALLET W
     WHERE W.WLT_DEVICEID = :deviceId
       AND W.USR_ID = :userId
       AND W.WLT_STATUS = 'A'
)
   AND WLTHOT_DTDELETED IS NULL",
            new
            {
                userId = 1L,
                deviceId = "DEVICE-1",
                dateTime = created.AddHours(2)
            });

        affected.Should().Be(1);
        hotlist[0][2].Should().Be("I");
        hotlist[0][4].Should().Be(created.AddHours(2));
        hotlist[1][2].Should().Be("A");
        hotlist[1][4].Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies a JSON body stored as text round-trips through Dapper together with a binary notification identifier.
    /// PT: Verifica se um corpo JSON armazenado como texto faz round-trip pelo Dapper junto com um identificador binario de notificacao.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void BinaryGuidJsonBody_ShouldRoundTripThroughDapper_Test()
    {
        var db = CreateDb();
        var table = db.AddTable("notification_messages");
        table.AddColumn("notification_id", DbType.Binary, false);
        table.AddColumn("body", DbType.String, false);

        using var connection = CreateConnection(db);
        connection.Open();

        var notificationId = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");
        var payload = new NotificationBody("ok", 2);
        var bodyJson = JsonSerializer.Serialize(payload);

        connection.Execute(
            "INSERT INTO notification_messages (notification_id, body) VALUES (@notificationId, @body)",
            new { notificationId = notificationId.ToByteArray(), body = bodyJson });

        var row = connection.QuerySingle<(byte[] NotificationId, string Body)>(
            "SELECT notification_id NotificationId, body Body FROM notification_messages WHERE notification_id = @notificationId",
            new { notificationId = notificationId.ToByteArray() });

        row.NotificationId.Should().Equal(notificationId.ToByteArray());
        row.Body.Should().Be(bodyJson);

        var loaded = JsonSerializer.Deserialize<NotificationBody>(row.Body);
        loaded.Should().NotBeNull();
        loaded!.Message.Should().Be("ok");
        loaded.Attempts.Should().Be(2);

        var typedRow = connection.QuerySingle<NotificationMessageRow>(
            "SELECT notification_id NotificationId, body Body FROM notification_messages WHERE notification_id = @notificationId",
            new { notificationId = notificationId.ToByteArray() });

        typedRow.NotificationId.Should().Equal(notificationId.ToByteArray());
        typedRow.Body.Should().Be(bodyJson);
    }

    /// <summary>
    /// EN: Verifies an updated JSON body and status round-trip through Dapper after replacing the stored row values.
    /// PT: Verifica se um corpo JSON atualizado e o status fazem round-trip pelo Dapper depois de substituir os valores da linha armazenada.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdditionalBehaviorCoverage")]
    public void BinaryGuidJsonBodyUpdate_ShouldPersistUpdatedValuesThroughDapper_Test()
    {
        var db = CreateDb();
        var table = db.AddTable("notification_messages");
        table.AddColumn("notification_id", DbType.Binary, false);
        table.AddColumn("status", DbType.String, false);
        table.AddColumn("error_message", DbType.String, true);
        table.AddColumn("body", DbType.String, false);

        using var connection = CreateConnection(db);
        connection.Open();

        var notificationId = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");
        var initialBody = new NotificationBody("init", 1);
        var updatedBody = new NotificationBody("changed", 3);

        connection.Execute(
            "INSERT INTO notification_messages (notification_id, status, error_message, body) VALUES (@notificationId, @status, @errorMessage, @body)",
            new
            {
                notificationId = notificationId.ToByteArray(),
                status = "S",
                errorMessage = (string?)null,
                body = JsonSerializer.Serialize(initialBody)
            });

        connection.Execute(
            "UPDATE notification_messages SET status = @status, error_message = @errorMessage, body = @body WHERE notification_id = @notificationId",
            new
            {
                notificationId = notificationId.ToByteArray(),
                status = "E",
                errorMessage = "falha",
                body = JsonSerializer.Serialize(updatedBody)
            });

        var row = connection.QuerySingle<(byte[] NotificationId, string Status, string? ErrorMessage, string Body)>(
            "SELECT notification_id NotificationId, status Status, error_message ErrorMessage, body Body FROM notification_messages WHERE notification_id = @notificationId",
            new { notificationId = notificationId.ToByteArray() });

        row.NotificationId.Should().Equal(notificationId.ToByteArray());
        row.Status.Should().Be("E");
        row.ErrorMessage.Should().Be("falha");
        row.Body.Should().Be(JsonSerializer.Serialize(updatedBody));

        var loaded = JsonSerializer.Deserialize<NotificationBody>(row.Body);
        loaded.Should().NotBeNull();
        loaded!.Message.Should().Be("changed");
        loaded.Attempts.Should().Be(3);
    }

    private sealed record NotificationBody(string Message, int Attempts);

    private sealed record NotificationMessageRow(byte[] NotificationId, string Body);
}
