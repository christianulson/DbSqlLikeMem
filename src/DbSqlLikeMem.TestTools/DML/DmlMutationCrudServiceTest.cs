namespace DbSqlLikeMem.TestTools.DML;

public partial class DmlMutationServiceTest<T>
{
    /// <summary>
    /// EN: Reads a user name by primary key and validates the updated value.
    /// PT: Lê um nome de usuario pela chave primaria e valida o valor atualizado.
    /// </summary>
    public string RunUpdateByPk(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected update result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value!;
    }

    /// <summary>
    /// EN: Deletes one user row and validates the remaining row count.
    /// PT: Exclui uma linha de usuario e valida a contagem de linhas restante.
    /// </summary>
    public int RunDeleteByPk(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.DeleteUserById(users, 1));
        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected delete count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Executes the join query between users and orders and validates the count.
    /// PT: Executa a consulta de junção entre usuarios e pedidos e valida a contagem.
    /// </summary>
    public int RunSelectJoin(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CountJoinForUser(users, orders, 1)), CultureInfo.InvariantCulture);
        if (value != 2)
        {
            throw new InvalidOperationException($"Unexpected join count for {Dialect.DisplayName}: {value}.");
        }

        return value;
    }

    /// <summary>
    /// EN: Updates a row and validates the affected-row count reported by the provider.
    /// PT: Atualiza uma linha e valida a contagem de linhas afetadas informada pelo provedor.
    /// </summary>
    public int RunRowCountAfterUpdate(params object[] pars)
    {
        var users = (string)pars[0];
        var affected = ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected update rowcount for {Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }
}
