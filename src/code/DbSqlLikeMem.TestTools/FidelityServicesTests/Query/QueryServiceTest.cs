namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes shared query benchmark workflows and validates the observed provider results.
/// PT-br: Executa fluxos compartilhados de benchmark de consulta e valida os resultados observados do provedor.
/// </summary>
public partial class QueryServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context)
{

}
