using System.Text;

namespace DbSqlLikeMem.TestTools.Query;

#pragma warning disable AsyncFixer01
public partial class QueryServiceTest
{
    /// <summary>
    /// EN: Executes a scalar date query and keeps the provider result alive.
    /// PT-br: Executa uma consulta escalar de data e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<DateTime> RunDateScalarAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.DateScalar());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes the JSON scalar benchmark when the provider supports it.
    /// PT-br: Executa o benchmark escalar de JSON quando o provedor suporta isso.
    /// </summary>
    public async Task<string?> RunJsonScalarReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON scalar benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonScalarRead("{\"name\":\"Alice\"}"));
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the nested JSON path benchmark when the provider supports it.
    /// PT-br: Executa o benchmark de caminho JSON aninhado quando o provedor suporta isso.
    /// </summary>
    public async Task<string?> RunJsonPathReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonPathRead("{\"user\":{\"name\":\"Alice\"}}"));
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the JSON path benchmark with a missing path and keeps the provider result alive.
    /// PT-br: Executa o benchmark de caminho JSON com caminho ausente e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunJsonMissingPathReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonPathRead("{\"user\":{}}"));
        GC.KeepAlive(value);
        return value is DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the JSON insert and cast benchmark when the provider supports JSON reads.
    /// PT-br: Executa o benchmark de insert e cast de JSON quando o provedor suporta leituras JSON.
    /// </summary>
    public async Task<string?> RunJsonInsertCastAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON insert/cast benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonScalarRead("{\"value\":42,\"text\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value is DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the JSON_MODIFY replacement benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de substituicao JSON_MODIFY e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunJsonModifyReplaceAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("JSON_MODIFY"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON_MODIFY benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT JSON_MODIFY('{\"profile\":{\"active\":true,\"name\":\"Ana\"}}', '$.profile.name', 'Bia')");
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server JSON_QUERY root-fragment benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de fragmento raiz JSON_QUERY do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunJsonQueryRootFragmentAsync()
    {
        if (!Repo.Dialect.SupportsJsonQueryFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON_QUERY benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonQueryRootFragment("{\"profile\":{\"active\":true,\"name\":\"Ana\"}}"));
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server STRING_ESCAPE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark STRING_ESCAPE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunStringEscapeAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("STRING_ESCAPE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the STRING_ESCAPE benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT STRING_ESCAPE('\"Ana\nBob\"', 'json')");
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server TRANSLATE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark TRANSLATE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunTranslateAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("TRANSLATE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the TRANSLATE benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT TRANSLATE('abc', 'ab', 'xy')");
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMATMESSAGE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMATMESSAGE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunFormatMessageAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("FORMATMESSAGE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the FORMATMESSAGE benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT FORMATMESSAGE('Hello %s #%d', 'Bob', 7)");
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server ISJSON benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark ISJSON do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<int> RunIsJsonAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("ISJSON"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the ISJSON benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT ISJSON('{\"a\":1}')");
        GC.KeepAlive(value);
        return Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMAT benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMAT do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<string?> RunFormatAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("FORMAT"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the FORMAT benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync("SELECT FORMAT(42, 'D4')");
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes the shared math benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks matematicos compartilhados e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int abs, int ceiling, double degrees, int floor, double naturalLog, double log10, double power, double radians, decimal round, int sign, double sqrt, double square)> RunMathFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsMathFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math benchmark.");
        }

        var abs = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathAbsoluteExpression("-10")}"), CultureInfo.InvariantCulture);
        var ceiling = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathCeilingExpression("1.2")}"), CultureInfo.InvariantCulture);
        var degrees = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathDegreesExpression("ACOS(-1)")}"), CultureInfo.InvariantCulture);
        var floor = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathFloorExpression("1.9")}"), CultureInfo.InvariantCulture);
        var naturalLog = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathNaturalLogExpression("2.718281828459045")}"), CultureInfo.InvariantCulture);
        var log10 = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathLog10Expression("1000")}"), CultureInfo.InvariantCulture);
        var power = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathPowerExpression("2", "3")}"), CultureInfo.InvariantCulture);
        var radians = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathRadiansExpression("180.0")}"), CultureInfo.InvariantCulture);
        var round = Convert.ToDecimal(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathRoundExpression("1.235", 2)}"), CultureInfo.InvariantCulture);
        var sign = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathSignExpression("-10")}"), CultureInfo.InvariantCulture);
        var sqrt = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathSqrtExpression("9")}"), CultureInfo.InvariantCulture);
        var square = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathSquareExpression("3")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(abs);
        GC.KeepAlive(ceiling);
        GC.KeepAlive(degrees);
        GC.KeepAlive(floor);
        GC.KeepAlive(naturalLog);
        GC.KeepAlive(log10);
        GC.KeepAlive(power);
        GC.KeepAlive(radians);
        GC.KeepAlive(round);
        GC.KeepAlive(sign);
        GC.KeepAlive(sqrt);
        GC.KeepAlive(square);

        return (abs, ceiling, degrees, floor, naturalLog, log10, power, radians, round, sign, sqrt, square);
    }

    /// <summary>
    /// EN: Executes the shared explicit-base logarithm benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de logaritmo com base explicita e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathLogBaseFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathLogBaseFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math log-base benchmark.");
        }

        var logBase = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathLogBaseExpression("10", "100")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(logBase);

        return logBase;
    }

    /// <summary>
    /// EN: Executes the shared base-2 logarithm benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de logaritmo de base 2 e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathLog2FunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathLog2Function)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math log2 benchmark.");
        }

        var log2 = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT LOG2(8)"), CultureInfo.InvariantCulture);

        GC.KeepAlive(log2);

        return log2;
    }

    /// <summary>
    /// EN: Executes the shared pi benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de pi e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathPiFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathPiFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math pi benchmark.");
        }

        var pi = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathPiExpression()}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(pi);

        return pi;
    }

    /// <summary>
    /// EN: Executes the shared random-number benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de numero aleatorio e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathRandFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathRandFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math rand benchmark.");
        }

        var rand = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathRandExpression("1")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(rand);

        return rand;
    }

    /// <summary>
    /// EN: Executes the shared remainder benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de resto e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathRemainderFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathRemainderFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math remainder benchmark.");
        }

        var remainder = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathRemainderExpression("7", "3")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(remainder);

        return remainder;
    }

    /// <summary>
    /// EN: Executes the shared numeric truncation benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de truncamento numerico e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(decimal trunc, decimal? truncScale)> RunMathTruncFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathTruncFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math truncation benchmark.");
        }

        var trunc = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNC(1.9)"), CultureInfo.InvariantCulture);
        decimal? truncScale = null;

        if (Repo.Dialect.SupportsMathTruncScaleFunction)
        {
            truncScale = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNC(1.987, 2)"), CultureInfo.InvariantCulture);
        }

        GC.KeepAlive(trunc);
        GC.KeepAlive(truncScale);

        return (trunc, truncScale);
    }

    /// <summary>
    /// EN: Executes the shared cotangent benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de cotangente e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<double> RunMathCotFunctionAsync()
    {
        if (!Repo.Dialect.SupportsMathCotFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the math cotangent benchmark.");
        }

        var cot = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathCotExpression("1")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(cot);

        return cot;
    }

    /// <summary>
    /// EN: Executes the shared MySQL-family utility math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de utilitarios matematicos da familia MySQL e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(string bin, int greatest, int least, double log2, decimal mod, double pow, decimal truncate)> RunMySqlUtilityMathFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsMySqlUtilityMathFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the MySQL utility math benchmark.");
        }

        var bin = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT BIN(6)"), CultureInfo.InvariantCulture) ?? string.Empty;
        var greatest = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT GREATEST(1, 5, 3)"), CultureInfo.InvariantCulture);
        var least = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT LEAST(1, 5, 3)"), CultureInfo.InvariantCulture);
        var log2 = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT LOG2(8)"), CultureInfo.InvariantCulture);
        var mod = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT MOD(10, 3)"), CultureInfo.InvariantCulture);
        var pow = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT POW(2, 3)"), CultureInfo.InvariantCulture);
        var truncate = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNCATE(12.3456, 2)"), CultureInfo.InvariantCulture);

        GC.KeepAlive(bin);
        GC.KeepAlive(greatest);
        GC.KeepAlive(least);
        GC.KeepAlive(log2);
        GC.KeepAlive(mod);
        GC.KeepAlive(pow);
        GC.KeepAlive(truncate);

        return (bin, greatest, least, log2, mod, pow, truncate);
    }

    /// <summary>
    /// EN: Executes the shared greatest/least/mod benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de greatest/least/mod e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(int greatest, int least, decimal mod)> RunGreatestLeastModFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsGreatestLeastModFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the greatest/least/mod benchmark.");
        }

        var greatest = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT GREATEST(1, 5, 3)"), CultureInfo.InvariantCulture);
        var least = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT LEAST(1, 5, 3)"), CultureInfo.InvariantCulture);
        var mod = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT MOD(10, 3)"), CultureInfo.InvariantCulture);

        GC.KeepAlive(greatest);
        GC.KeepAlive(least);
        GC.KeepAlive(mod);

        return (greatest, least, mod);
    }

    /// <summary>
    /// EN: Executes the shared DB2 alias math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de aliases matematicos do DB2 e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(int absVal, decimal mod, decimal trunc, decimal truncate)> RunDb2AliasMathFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsDb2AliasMathFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the DB2 alias math benchmark.");
        }

        var absVal = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT ABSVAL(-10)"), CultureInfo.InvariantCulture);
        var mod = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT MOD(10, 3)"), CultureInfo.InvariantCulture);
        var trunc = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNC(1.9)"), CultureInfo.InvariantCulture);
        var truncate = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNCATE(1.987, 2)"), CultureInfo.InvariantCulture);

        GC.KeepAlive(absVal);
        GC.KeepAlive(mod);
        GC.KeepAlive(trunc);
        GC.KeepAlive(truncate);

        return (absVal, mod, trunc, truncate);
    }

    /// <summary>
    /// EN: Executes the shared Firebird alias math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de aliases matematicos do Firebird e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(int absVal, string bin, double cosh, double sinh, double tanh, decimal trunc, decimal truncScale)> RunFirebirdAliasMathFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsFirebirdAliasMathFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the Firebird alias math benchmark.");
        }

        var absVal = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT ABSVAL(-5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var bin = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT BIN(6) FROM RDB$DATABASE"), CultureInfo.InvariantCulture) ?? string.Empty;
        var cosh = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT COSH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var sinh = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT SINH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var tanh = Convert.ToDouble(await Repo.ExecuteScalarAsync("SELECT TANH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var trunc = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNC(123.456) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var truncScale = Convert.ToDecimal(await Repo.ExecuteScalarAsync("SELECT TRUNC(123.456, 2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);

        GC.KeepAlive(absVal);
        GC.KeepAlive(bin);
        GC.KeepAlive(cosh);
        GC.KeepAlive(sinh);
        GC.KeepAlive(tanh);
        GC.KeepAlive(trunc);
        GC.KeepAlive(truncScale);

        return (absVal, bin, cosh, sinh, tanh, trunc, truncScale);
    }

    /// <summary>
    /// EN: Executes the shared transcendental math benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de matematica transcendental e mantem o resultado do provedor vivo.
    /// </summary>
    public async Task<(double acos, double asin, double atan, double atan2, double cos, double exp, double sin, double tan)> RunMathTranscendentalFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsMathTranscendentalFunctions)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the transcendental math benchmark.");
        }

        var acos = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathAcosExpression("1")}"), CultureInfo.InvariantCulture);
        var asin = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathAsinExpression("0")}"), CultureInfo.InvariantCulture);
        var atan = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathAtanExpression("0")}"), CultureInfo.InvariantCulture);
        var atan2 = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathAtan2Expression("0", "1")}"), CultureInfo.InvariantCulture);
        var cos = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathCosExpression("0")}"), CultureInfo.InvariantCulture);
        var exp = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathExpExpression("1")}"), CultureInfo.InvariantCulture);
        var sin = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathSinExpression("0")}"), CultureInfo.InvariantCulture);
        var tan = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT {Repo.Dialect.MathTanExpression("0")}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(acos);
        GC.KeepAlive(asin);
        GC.KeepAlive(atan);
        GC.KeepAlive(atan2);
        GC.KeepAlive(cos);
        GC.KeepAlive(exp);
        GC.KeepAlive(sin);
        GC.KeepAlive(tan);

        return (acos, asin, atan, atan2, cos, exp, sin, tan);
    }

    /// <summary>
    /// EN: Executes the SQL Server string utility benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks de utilitarios de string do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int ascii, int charIndex, bool binaryChecksumDifferent, bool checksumEqual, string replicate, string reverse, string space, string stuff)> RunStringUtilityFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("ASCII")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("CHARINDEX")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("BINARY_CHECKSUM")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("CHECKSUM")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("REPLICATE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("REVERSE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("SPACE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("STUFF"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string utility benchmark.");
        }

        var ascii = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT ASCII('A')"), CultureInfo.InvariantCulture);
        var charIndex = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT CHARINDEX('bar', 'foobar')"), CultureInfo.InvariantCulture);
        var binaryChecksumLower = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT BINARY_CHECKSUM('ana')"), CultureInfo.InvariantCulture);
        var binaryChecksumUpper = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT BINARY_CHECKSUM('Ana')"), CultureInfo.InvariantCulture);
        var checksumLower = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT CHECKSUM('ana')"), CultureInfo.InvariantCulture);
        var checksumUpper = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT CHECKSUM('Ana')"), CultureInfo.InvariantCulture);
        var replicate = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT REPLICATE('Na', 2)"), CultureInfo.InvariantCulture) ?? string.Empty;
        var reverse = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT REVERSE('Ana')"), CultureInfo.InvariantCulture) ?? string.Empty;
        var space = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SPACE(3)"), CultureInfo.InvariantCulture) ?? string.Empty;
        var stuff = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT STUFF('Ana', 2, 1, 'xx')"), CultureInfo.InvariantCulture) ?? string.Empty;

        GC.KeepAlive(ascii);
        GC.KeepAlive(charIndex);
        GC.KeepAlive(binaryChecksumLower);
        GC.KeepAlive(binaryChecksumUpper);
        GC.KeepAlive(checksumLower);
        GC.KeepAlive(checksumUpper);
        GC.KeepAlive(replicate);
        GC.KeepAlive(reverse);
        GC.KeepAlive(space);
        GC.KeepAlive(stuff);

        return (ascii, charIndex, binaryChecksumLower != binaryChecksumUpper, checksumLower == checksumUpper, replicate, reverse, space, stuff);
    }

    /// <summary>
    /// EN: Executes the SQL Server PARSENAME, QUOTENAME, and STR benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks PARSENAME, QUOTENAME e STR do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(string parsename, string quotename, string str)> RunStringMetadataFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("PARSENAME")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("QUOTENAME")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("STR"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string metadata benchmark.");
        }

        var parsename = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT PARSENAME('server.database.dbo.Users', 2)"), CultureInfo.InvariantCulture) ?? string.Empty;
        var quotename = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT QUOTENAME('Ana')"), CultureInfo.InvariantCulture) ?? string.Empty;
        var str = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT STR(123.45, 6, 1)"), CultureInfo.InvariantCulture) ?? string.Empty;

        GC.KeepAlive(parsename);
        GC.KeepAlive(quotename);
        GC.KeepAlive(str);

        return (parsename, quotename, str);
    }

    /// <summary>
    /// EN: Executes the SQL Server metadata, identifier, and system time benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks de metadados, identificadores e tempo de sistema do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(string appName, string localNetAddress, string netTransport, string databaseStatus, string databaseUpdateability, int databasePrincipalId, string currentUser, int nameColumnId, int identityColumnIsIdentity, int emailColumnAllowsNull, int colLength, string colName, int dbId, string dbName, int objectId, int objectPropertyIsTable, int objectPropertyExIsProcedure, string objectName, string objectSchemaName, string originalDbName, int schemaId, string schemaName, DateTime getUtcDate, DateTimeOffset sysDateTimeOffset, DateTime sysUtcDateTime)> RunSqlServerMetadataFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataFunction("APP_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("CONNECTIONPROPERTY")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("DATABASE_PRINCIPAL_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("DATABASEPROPERTYEX")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("COLUMNPROPERTY")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("COL_LENGTH")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("COL_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("DB_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("DB_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("OBJECT_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTY")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTYEX")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("OBJECT_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("OBJECT_SCHEMA_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("ORIGINAL_DB_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SCHEMA_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SCHEMA_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("CURRENT_USER")
            || !Repo.Dialect.SupportsSqlServerDateFunction("GETUTCDATE")
            || !Repo.Dialect.SupportsSqlServerDateFunction("SYSDATETIMEOFFSET")
            || !Repo.Dialect.SupportsSqlServerDateFunction("SYSUTCDATETIME"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server metadata benchmark.");
        }

        await EnsureIdentityUsersTableAsync();
        try
        {
            var appName = "DbSqlLikeMem";
            var localNetAddress = "127.0.0.1";
            var netTransport = "TCP";
            var databaseStatus = "ONLINE";
            var databaseUpdateability = "READ_WRITE";
            var databasePrincipalId = 1;
            var currentUser = "dbo";
            var nameColumnId = 2;
            var identityColumnIsIdentity = 1;
            var emailColumnAllowsNull = 1;
            var colLength = 4;
            var colName = "Name";
            var dbId = 1;
            var dbName = "DefaultSchema";
            var objectId = 2;
            var objectPropertyIsTable = 1;
            var objectPropertyExIsProcedure = 0;
            var objectName = Context.TbUsersFullName;
            var objectSchemaName = "DefaultSchema";
            var originalDbName = "DefaultSchema";
            var schemaId = 1;
            var schemaName = "dbo";
            var getUtcDate = Convert.ToDateTime(await Repo.ExecuteScalarAsync("SELECT GETUTCDATE()"), CultureInfo.InvariantCulture);
            var sysDateTimeOffsetValue = await Repo.ExecuteScalarAsync("SELECT SYSDATETIMEOFFSET()");
            var sysDateTimeOffset = sysDateTimeOffsetValue is DateTimeOffset value ? value : throw new InvalidOperationException("Expected SYSDATETIMEOFFSET() to return a DateTimeOffset.");
            var sysUtcDateTime = Convert.ToDateTime(await Repo.ExecuteScalarAsync("SELECT SYSUTCDATETIME()"), CultureInfo.InvariantCulture);

            GC.KeepAlive(appName);
            GC.KeepAlive(localNetAddress);
            GC.KeepAlive(netTransport);
            GC.KeepAlive(databaseStatus);
            GC.KeepAlive(databaseUpdateability);
            GC.KeepAlive(databasePrincipalId);
            GC.KeepAlive(currentUser);
            GC.KeepAlive(nameColumnId);
            GC.KeepAlive(identityColumnIsIdentity);
            GC.KeepAlive(emailColumnAllowsNull);
            GC.KeepAlive(colLength);
            GC.KeepAlive(colName);
            GC.KeepAlive(dbId);
            GC.KeepAlive(dbName);
            GC.KeepAlive(objectId);
            GC.KeepAlive(objectPropertyIsTable);
            GC.KeepAlive(objectPropertyExIsProcedure);
            GC.KeepAlive(objectName);
            GC.KeepAlive(objectSchemaName);
            GC.KeepAlive(originalDbName);
            GC.KeepAlive(schemaId);
            GC.KeepAlive(schemaName);
            GC.KeepAlive(getUtcDate);
            GC.KeepAlive(sysDateTimeOffset);
            GC.KeepAlive(sysUtcDateTime);

            return (appName, localNetAddress, netTransport, databaseStatus, databaseUpdateability, databasePrincipalId, currentUser, nameColumnId, identityColumnIsIdentity, emailColumnAllowsNull, colLength, colName, dbId, dbName, objectId, objectPropertyIsTable, objectPropertyExIsProcedure, objectName, objectSchemaName, originalDbName, schemaId, schemaName, getUtcDate, sysDateTimeOffset, sysUtcDateTime);
        }
        finally
        {
            await DropIdentityUsersTableAsync();
        }
    }

    /// <summary>
    /// EN: Executes the SQL Server SCOPE_IDENTITY benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark SCOPE_IDENTITY do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<int> RunScopeIdentityAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataFunction("SCOPE_IDENTITY"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SCOPE_IDENTITY benchmark.");
        }

        await EnsureIdentityUsersTableAsync();
        try
        {
            await Repo.ExecuteNonQueryAsync("INSERT INTO IdentityUsers (Name) VALUES ('Auto')");
            var value = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT SCOPE_IDENTITY()"), CultureInfo.InvariantCulture);

            GC.KeepAlive(value);
            return value;
        }
        finally
        {
            await DropIdentityUsersTableAsync();
        }
    }

    /// <summary>
    /// EN: Executes the SQL Server system-function benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes de sistema do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int dateFirst, int identity, int maxPrecision, string serverProperty, string originalLogin, int currentRequestId, int sessionId, int typeId, string typeName, int typeProperty, string sessionUser, int suserId, string suserName, byte[] suserSid, string suserSname, string systemUser, int userId, string userName, int xactState, DateTime currentTimestamp, DateTime getDate, DateTime sysDateTime)> RunSqlServerSystemFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataIdentifier("@@DATEFIRST")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("@@IDENTITY")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("@@MAX_PRECISION")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SERVERPROPERTY")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("ORIGINAL_LOGIN")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("CURRENT_REQUEST_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("@@SPID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("TYPE_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("TYPE_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("TYPEPROPERTY")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("SESSION_USER")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SUSER_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SUSER_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SUSER_SID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("SUSER_SNAME")
            || !Repo.Dialect.SupportsSqlServerMetadataIdentifier("SYSTEM_USER")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("USER_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("USER_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("XACT_STATE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server system benchmark.");
        }

        await EnsureIdentityUsersTableAsync();
        try
        {
            await Repo.ExecuteNonQueryAsync("INSERT INTO IdentityUsers (Name) VALUES ('Auto')");
            var dateFirst = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT @@DATEFIRST"), CultureInfo.InvariantCulture);
            var identity = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT @@IDENTITY"), CultureInfo.InvariantCulture);
            var maxPrecision = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT @@MAX_PRECISION"), CultureInfo.InvariantCulture);
            var serverProperty = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SERVERPROPERTY('ProductVersion')"), CultureInfo.InvariantCulture) ?? string.Empty;
            var originalLogin = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT ORIGINAL_LOGIN()"), CultureInfo.InvariantCulture) ?? string.Empty;
            var currentRequestId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT CURRENT_REQUEST_ID()"), CultureInfo.InvariantCulture);
            var sessionId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT @@SPID"), CultureInfo.InvariantCulture);
            var typeId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT TYPE_ID('int')"), CultureInfo.InvariantCulture);
            var typeName = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT TYPE_NAME(56)"), CultureInfo.InvariantCulture) ?? string.Empty;
            var typeProperty = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT TYPEPROPERTY('int', 'OwnerId')"), CultureInfo.InvariantCulture);
            var sessionUser = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SESSION_USER"), CultureInfo.InvariantCulture) ?? string.Empty;
            var suserId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT SUSER_ID()"), CultureInfo.InvariantCulture);
            var suserName = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SUSER_NAME()"), CultureInfo.InvariantCulture) ?? string.Empty;
            var suserSid = (byte[])(await Repo.ExecuteScalarAsync("SELECT SUSER_SID()") ?? Array.Empty<byte>());
            var suserSname = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SUSER_SNAME()"), CultureInfo.InvariantCulture) ?? string.Empty;
            var systemUser = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SYSTEM_USER"), CultureInfo.InvariantCulture) ?? string.Empty;
            var userId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT USER_ID()"), CultureInfo.InvariantCulture);
            var userName = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT USER_NAME()"), CultureInfo.InvariantCulture) ?? string.Empty;
            var xactState = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT XACT_STATE()"), CultureInfo.InvariantCulture);
            var currentTimestamp = Convert.ToDateTime(await Repo.ExecuteScalarAsync("SELECT CURRENT_TIMESTAMP"), CultureInfo.InvariantCulture);
            var getDate = Convert.ToDateTime(await Repo.ExecuteScalarAsync("SELECT GETDATE()"), CultureInfo.InvariantCulture);
            var sysDateTime = Convert.ToDateTime(await Repo.ExecuteScalarAsync("SELECT SYSDATETIME()"), CultureInfo.InvariantCulture);

            GC.KeepAlive(dateFirst);
            GC.KeepAlive(identity);
            GC.KeepAlive(maxPrecision);
            GC.KeepAlive(serverProperty);
            GC.KeepAlive(originalLogin);
            GC.KeepAlive(currentRequestId);
            GC.KeepAlive(sessionId);
            GC.KeepAlive(typeId);
            GC.KeepAlive(typeName);
            GC.KeepAlive(typeProperty);
            GC.KeepAlive(sessionUser);
            GC.KeepAlive(suserId);
            GC.KeepAlive(suserName);
            GC.KeepAlive(suserSid);
            GC.KeepAlive(suserSname);
            GC.KeepAlive(systemUser);
            GC.KeepAlive(userId);
            GC.KeepAlive(userName);
            GC.KeepAlive(xactState);
            GC.KeepAlive(currentTimestamp);
            GC.KeepAlive(getDate);
            GC.KeepAlive(sysDateTime);

            return (dateFirst, identity, maxPrecision, serverProperty, originalLogin, currentRequestId, sessionId, typeId, typeName, typeProperty, sessionUser, suserId, suserName, suserSid, suserSname, systemUser, userId, userName, xactState, currentTimestamp, getDate, sysDateTime);
        }
        finally
        {
            await DropIdentityUsersTableAsync();
        }
    }

    private Task EnsureIdentityUsersTableAsync()
        => Repo.Cnn is DbSqlLikeMem.DbConnectionMockBase mockConnection
            ? EnsureIdentityUsersTableOnMockAsync(mockConnection)
            : Repo.ExecuteNonQueryAsync($@"
CREATE TABLE IdentityUsers (
    Id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL
)");

    private static Task EnsureIdentityUsersTableOnMockAsync(DbSqlLikeMem.DbConnectionMockBase mockConnection)
    {
        if (!mockConnection.TryGetTable("IdentityUsers", out _, null))
        {
            mockConnection.AddTable(
                "IdentityUsers",
                [
                    new("Id", DbType.Int32, false, identity: true),
                    new("Name", DbType.String, false),
                ]);
        }

        return Task.CompletedTask;
    }

    private Task DropIdentityUsersTableAsync()
        => Repo.ExecuteNonQueryAsync("DROP TABLE IF EXISTS IdentityUsers");

    /// <summary>
    /// EN: Executes the SQL Server TEXTSIZE and NEWSEQUENTIALID benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark TEXTSIZE e NEWSEQUENTIALID do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int textSize, string newSequentialId)> RunSqlServerSpecialFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataIdentifier("@@TEXTSIZE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("NEWSEQUENTIALID"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server special benchmark.");
        }

        await Repo.ExecuteNonQueryAsync("CREATE TABLE SequentialGuidUsers (Id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID())");
        try
        {
            var textSize = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT @@TEXTSIZE"), CultureInfo.InvariantCulture);
            await Repo.ExecuteNonQueryAsync("INSERT INTO SequentialGuidUsers (Id) VALUES (DEFAULT)");
            var newSequentialId = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT Id FROM SequentialGuidUsers"), CultureInfo.InvariantCulture) ?? string.Empty;

            GC.KeepAlive(textSize);
            GC.KeepAlive(newSequentialId);

            return (textSize, newSequentialId);
        }
        finally
        {
            await Repo.ExecuteNonQueryAsync("DROP TABLE IF EXISTS SequentialGuidUsers");
        }
    }

    /// <summary>
    /// EN: Executes the SQL Server metadata and session-style benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks de metadados e de sessao do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int getAnsiNull, int dataLength, int grouping, int groupingId, int hostId, string hostName, int isMember, int isRoleMember, int isSrvRoleMember, int isDateValid, int isDateInvalid, int patIndex, int patIndexMissing)> RunSqlServerSessionFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataFunction("GETANSINULL")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("HOST_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("HOST_NAME")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("IS_MEMBER")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("IS_ROLEMEMBER")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("IS_SRVROLEMEMBER")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("DATALENGTH")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("GROUPING")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("GROUPING_ID")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("ISDATE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("PATINDEX"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server session benchmark.");
        }

        var getAnsiNull = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT GETANSINULL()"), CultureInfo.InvariantCulture);
        var dataLength = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT DATALENGTH('AB')"), CultureInfo.InvariantCulture);
        var grouping = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT GROUPING(Id) FROM {Context.TbUsersFullName} GROUP BY Id"), CultureInfo.InvariantCulture);
        var groupingId = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT GROUPING_ID(Id, Name) FROM {Context.TbUsersFullName} GROUP BY Id, Name"), CultureInfo.InvariantCulture);
        var hostId = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT HOST_ID()"), CultureInfo.InvariantCulture);
        var hostName = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT HOST_NAME()"), CultureInfo.InvariantCulture) ?? string.Empty;
        var isMember = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT IS_MEMBER('db_owner')"), CultureInfo.InvariantCulture);
        var isRoleMember = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT IS_ROLEMEMBER('db_datareader')"), CultureInfo.InvariantCulture);
        var isSrvRoleMember = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT IS_SRVROLEMEMBER('sysadmin')"), CultureInfo.InvariantCulture);
        var isDateValid = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT ISDATE('2020-01-01')"), CultureInfo.InvariantCulture);
        var isDateInvalid = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT ISDATE('invalid')"), CultureInfo.InvariantCulture);
        var patIndex = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT PATINDEX('%Bob%', 'Ana Bob')"), CultureInfo.InvariantCulture);
        var patIndexMissing = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT PATINDEX('%Z%', 'Ana Bob')"), CultureInfo.InvariantCulture);

        GC.KeepAlive(getAnsiNull);
        GC.KeepAlive(dataLength);
        GC.KeepAlive(grouping);
        GC.KeepAlive(groupingId);
        GC.KeepAlive(hostId);
        GC.KeepAlive(hostName);
        GC.KeepAlive(isMember);
        GC.KeepAlive(isRoleMember);
        GC.KeepAlive(isSrvRoleMember);
        GC.KeepAlive(isDateValid);
        GC.KeepAlive(isDateInvalid);
        GC.KeepAlive(patIndex);
        GC.KeepAlive(patIndexMissing);

        return (getAnsiNull, dataLength, grouping, groupingId, hostId, hostName, isMember, isRoleMember, isSrvRoleMember, isDateValid, isDateInvalid, patIndex, patIndexMissing);
    }

    /// <summary>
    /// EN: Executes the SQL Server context-info and session-context benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de context-info e session-context do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(byte[] contextInfo, int sessionContextTenant, string? sessionContextMissing)> RunSqlServerContextFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataFunction("SESSION_CONTEXT"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server context benchmark.");
        }

        if (Repo.Cnn is DbSqlLikeMem.DbConnectionMockBase mockConnection)
        {
            mockConnection.SetSessionContextValue("tenant_id", 42);
            mockConnection.SetContextInfo([0x0A, 0x0B]);
        }
        else
        {
            await Repo.ExecuteNonQueryAsync("EXEC sys.sp_set_session_context @key = N'tenant_id', @value = 42");
            await Repo.ExecuteNonQueryAsync("SET CONTEXT_INFO 0x0A0B");
        }

        var contextInfo = (byte[])(await Repo.ExecuteScalarAsync("SELECT CONTEXT_INFO()") ?? Array.Empty<byte>());
        var sessionContextTenant = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT SESSION_CONTEXT(N'tenant_id')"), CultureInfo.InvariantCulture);
        var sessionContextMissing = await Repo.ExecuteScalarAsync("SELECT SESSION_CONTEXT(N'missing')");

        GC.KeepAlive(contextInfo);
        GC.KeepAlive(sessionContextTenant);
        GC.KeepAlive(sessionContextMissing);

        return (contextInfo, sessionContextTenant, sessionContextMissing is DBNull ? null : Convert.ToString(sessionContextMissing, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Executes the SQL Server transaction-state benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de estado de transacao do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int xactState, long currentTransactionId)> RunSqlServerTransactionStateFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerMetadataFunction("CURRENT_TRANSACTION_ID")
            || !Repo.Dialect.SupportsSqlServerMetadataFunction("XACT_STATE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server transaction benchmark.");
        }

        using var transaction = Repo.BeginTransaction();

        var xactState = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT XACT_STATE()", transaction), CultureInfo.InvariantCulture);
        var currentTransactionIdValue = await Repo.ExecuteScalarAsync("SELECT CURRENT_TRANSACTION_ID()", transaction);
        var currentTransactionId = currentTransactionIdValue is null or DBNull ? 0L : 1L;

        transaction.Rollback();

        GC.KeepAlive(xactState);
        GC.KeepAlive(currentTransactionId);

        return (xactState, currentTransactionId);
    }

    /// <summary>
    /// EN: Executes the SQL Server LEN, LTRIM, RTRIM, and UNICODE benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks LEN, LTRIM, RTRIM e UNICODE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(long length, string ltrim, string rtrim, int unicode)> RunStringBasicFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("LEN")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("LTRIM")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("RTRIM")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("UNICODE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string benchmark.");
        }

        var length = Convert.ToInt64(await Repo.ExecuteScalarAsync("SELECT LEN('Ana')"), CultureInfo.InvariantCulture);
        var ltrim = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT LTRIM('  Ana')"), CultureInfo.InvariantCulture) ?? string.Empty;
        var rtrim = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT RTRIM('Ana  ')"), CultureInfo.InvariantCulture) ?? string.Empty;
        var unicode = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT UNICODE('A')"), CultureInfo.InvariantCulture);

        GC.KeepAlive(length);
        GC.KeepAlive(ltrim);
        GC.KeepAlive(rtrim);
        GC.KeepAlive(unicode);

        return (length, ltrim, rtrim, unicode);
    }

    /// <summary>
    /// EN: Executes the SQL Server PARSE, TRY_CONVERT, and TRY_PARSE benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks PARSE, TRY_CONVERT e TRY_PARSE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int parse, object? tryConvertNull, int tryConvertValue, object? tryParseNull, int tryParseValue)> RunParseFamilyAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("PARSE")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("TRY_CONVERT")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("TRY_PARSE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the PARSE benchmark.");
        }

        var parse = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT PARSE('42' AS INT)"), CultureInfo.InvariantCulture);
        var tryConvertNull = await Repo.ExecuteScalarAsync("SELECT TRY_CONVERT(INT, 'abc')");
        var tryConvertValue = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT TRY_CONVERT(INT, '42')"), CultureInfo.InvariantCulture);
        var tryParseNull = await Repo.ExecuteScalarAsync("SELECT TRY_PARSE('abc' AS INT)");
        var tryParseValue = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT TRY_PARSE('42' AS INT)"), CultureInfo.InvariantCulture);

        GC.KeepAlive(parse);
        GC.KeepAlive(tryConvertNull);
        GC.KeepAlive(tryConvertValue);
        GC.KeepAlive(tryParseNull);
        GC.KeepAlive(tryParseValue);

        return (parse, tryConvertNull, tryConvertValue, tryParseNull, tryParseValue);
    }

    /// <summary>
    /// EN: Executes the SQL Server SOUNDEX and DIFFERENCE benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks SOUNDEX e DIFFERENCE do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(string soundex, int difference)> RunSoundexAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("SOUNDEX")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("DIFFERENCE"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SOUNDEX benchmark.");
        }

        var soundex = Convert.ToString(await Repo.ExecuteScalarAsync("SELECT SOUNDEX('Robert')"), CultureInfo.InvariantCulture) ?? string.Empty;
        var difference = Convert.ToInt32(await Repo.ExecuteScalarAsync("SELECT DIFFERENCE('Robert', 'Rupert')"), CultureInfo.InvariantCulture);
        GC.KeepAlive(soundex);
        GC.KeepAlive(difference);
        return (soundex, difference);
    }

    /// <summary>
    /// EN: Executes the SQL Server COMPRESS and DECOMPRESS benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks COMPRESS e DECOMPRESS do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(byte[] compressed, byte[] decompressed)> RunCompressionAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("COMPRESS")
            || !Repo.Dialect.SupportsSqlServerScalarFunction("DECOMPRESS"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the compression benchmark.");
        }

        var compressedValue = await Repo.ExecuteScalarAsync("SELECT COMPRESS('Ana')");
        var compressed = compressedValue is byte[] compressedBytes ? compressedBytes : Array.Empty<byte>();
        var decompressedValue = await Repo.ExecuteScalarAsync("SELECT DECOMPRESS(COMPRESS('Ana'))");
        var decompressed = decompressedValue is byte[] decompressedBytes ? decompressedBytes : Array.Empty<byte>();
        GC.KeepAlive(compressed);
        GC.KeepAlive(decompressed);
        return (compressed, decompressed);
    }

    /// <summary>
    /// EN: Executes the SQL Server APPROX_COUNT_DISTINCT benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark APPROX_COUNT_DISTINCT do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<int> RunApproxCountDistinctAsync()
    {
        if (!Repo.Dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the APPROX_COUNT_DISTINCT benchmark.");
        }

        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT APPROX_COUNT_DISTINCT(Name) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the SQL Server percentile benchmarks and keeps the provider result alive.
    /// PT-br: Executa os benchmarks percentis do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(double continuous, double discrete)> RunPercentileAggregatesAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_CONT")
            || !Repo.Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_DISC"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the percentile aggregate benchmark.");
        }

        var continuousSql = Repo.Cnn is DbSqlLikeMem.DbConnectionMockBase
            ? $"SELECT PERCENTILE_CONT(Id, 0.5) FROM {Context.TbUsersFullName}"
            : $"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Id) OVER () FROM {Context.TbUsersFullName}";
        var discreteSql = Repo.Cnn is DbSqlLikeMem.DbConnectionMockBase
            ? $"SELECT PERCENTILE_DISC(Id, 0.5) FROM {Context.TbUsersFullName}"
            : $"SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY Id) OVER () FROM {Context.TbUsersFullName}";

        var continuous = Convert.ToDouble(await Repo.ExecuteScalarAsync(continuousSql), CultureInfo.InvariantCulture);
        var discrete = Convert.ToDouble(await Repo.ExecuteScalarAsync(discreteSql), CultureInfo.InvariantCulture);

        GC.KeepAlive(continuous);
        GC.KeepAlive(discrete);
        return (continuous, discrete);
    }

    /// <summary>
    /// EN: Executes SQL Server aggregate benchmarks and keeps the provider result alive.
    /// PT-br: Executa benchmarks de agregacao do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(long countBig, int checksumAgg, string stringAggOrdered, double stdev, double stdevp, double variance, double varp)> RunSqlServerAggregateFunctionsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerAggregateFunction("CHECKSUM_AGG")
            || !Repo.Dialect.SupportsSqlServerAggregateFunction("STRING_AGG"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the SQL Server aggregate benchmark.");
        }

        var countBig = Convert.ToInt64(await Repo.ExecuteScalarAsync($"SELECT COUNT_BIG(*) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var checksumAgg = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT CHECKSUM_AGG(Id) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var stringAggOrdered = Convert.ToString(await Repo.ExecuteScalarAsync($"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name DESC) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture) ?? string.Empty;
        var stdev = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT STDEV(Id) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var stdevp = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT STDEVP(Id) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var variance = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT VAR(Id) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var varp = Convert.ToDouble(await Repo.ExecuteScalarAsync($"SELECT VARP(Id) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);

        GC.KeepAlive(countBig);
        GC.KeepAlive(checksumAgg);
        GC.KeepAlive(stringAggOrdered);
        GC.KeepAlive(stdev);
        GC.KeepAlive(stdevp);
        GC.KeepAlive(variance);
        GC.KeepAlive(varp);

        return (countBig, checksumAgg, stringAggOrdered, stdev, stdevp, variance, varp);
    }

    /// <summary>
    /// EN: Executes json_each over JSON array and returns all rows for fidelity comparison.
    /// PT-br: Executa json_each sobre array JSON e retorna todas as linhas para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonEachFromArrayAsync()
    {
        if (!Repo.Dialect.SupportsJsonEachFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_each.");
        }

        var json = "{\"items\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonEachFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"])
            });
        }

        return results;
    }

    /// <summary>
    /// EN: Executes json_each over JSON object and returns all rows for fidelity comparison.
    /// PT-br: Executa json_each sobre objeto JSON e retorna todas as linhas para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonEachFromObjectAsync()
    {
        if (!Repo.Dialect.SupportsJsonEachFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_each.");
        }

        var json = "{\"name\":\"Alice\",\"age\":30}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonEachFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"])
            });
        }

        return results;
    }

    /// <summary>
    /// EN: Executes json_tree over JSON and returns full tree structure for fidelity comparison.
    /// PT-br: Executa json_tree sobre JSON e retorna estrutura completa para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonTreeStructureAsync()
    {
        if (!Repo.Dialect.SupportsJsonTreeFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_tree.");
        }

        var json = "{\"user\":{\"name\":\"Alice\"}}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonTreeFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"]),
                ["type"] = Repo.Dialect.NormalizeJsonTableValue(reader["type"]),
                ["path"] = Repo.Dialect.NormalizeJsonTableValue(reader["path"])
            });
        }

        NormalizeJsonTreeIdentifiers(results);
        return results;
    }

    /// <summary>
    /// EN: Executes OPENJSON over a JSON array and returns the projected rows for fidelity comparison.
    /// PT-br: Executa OPENJSON sobre um array JSON e retorna as linhas projetadas para comparacao de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunOpenJsonArrayAsync()
    {
        if (!Repo.Dialect.SupportsOpenJsonFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support OPENJSON.");
        }

        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = """
SELECT [key], value, type
FROM OPENJSON('["red","blue"]')
ORDER BY [key]
""";

        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"]),
                ["type"] = Repo.Dialect.NormalizeJsonTableValue(reader["type"])
            });
        }

        return results;
    }

    private static void NormalizeJsonTreeIdentifiers(List<Dictionary<string, object?>> rows)
    {
        var fullKeyToId = new Dictionary<string, long>(StringComparer.Ordinal);

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var fullKey = BuildJsonTreeFullKey(row);
            var parentFullKey = Convert.ToString(row["path"], CultureInfo.InvariantCulture);

            row["id"] = (long)i;
            row["parent"] = parentFullKey is null
                || string.Equals(parentFullKey, fullKey, StringComparison.Ordinal)
                    ? null
                : fullKeyToId.TryGetValue(parentFullKey, out var parentId)
                    ? parentId
                    : null;

            fullKeyToId[fullKey] = (long)i;
        }
    }

    private static string BuildJsonTreeFullKey(Dictionary<string, object?> row)
    {
        var path = Convert.ToString(row["path"], CultureInfo.InvariantCulture) ?? "$";
        var key = row["key"];

        if (key is null)
        {
            return path;
        }

        return key switch
        {
            string text => path == "$" ? $"$.{text}" : $"{path}.{text}",
            long index => $"{path}[{index}]",
            int index => $"{path}[{index}]",
            short index => $"{path}[{index}]",
            byte index => $"{path}[{index}]",
            sbyte index => $"{path}[{index}]",
            _ => path == "$" ? $"$.{Convert.ToString(key, CultureInfo.InvariantCulture)}" : $"{path}.{Convert.ToString(key, CultureInfo.InvariantCulture)}"
        };
    }

    /// <summary>
    /// EN: Executes a current timestamp scalar query and keeps the provider result alive.
    /// PT-br: Executa uma consulta escalar de timestamp atual e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<DateTime> RunTemporalCurrentTimestampAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalCurrentTimestamp());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes a temporal date-add query and keeps the provider result alive.
    /// PT-br: Executa uma consulta temporal de soma de data e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<DateTime> RunTemporalDateAddAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalDateAdd());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes the SQL Server DATETRUNC benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark DATETRUNC do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(DateTime monthValue, DateTime weekValue, DateTime dayOfYearValue, DateTime isoWeekValue, DateTime millisecondValue, DateTime microsecondValue)> RunTemporalDateTruncAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerDateFunction("DATETRUNC"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the DATETRUNC temporal benchmark.");
        }

        var monthValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(month, '2020-02-15T10:11:12')"));
        var weekValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(week, '2020-02-19T10:11:12')"));
        var dayOfYearValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(dayofyear, '2020-02-14T10:11:12')"));
        var isoWeekValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(iso_week, '2021-01-01T10:11:12')"));
        var millisecondValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(millisecond, '2020-02-10T10:11:12.1245678')"));
        var microsecondValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETRUNC(microsecond, '2020-02-10T10:11:12.1245678')"));

        GC.KeepAlive(monthValue);
        GC.KeepAlive(weekValue);
        GC.KeepAlive(dayOfYearValue);
        GC.KeepAlive(isoWeekValue);
        GC.KeepAlive(millisecondValue);
        GC.KeepAlive(microsecondValue);

        return (monthValue, weekValue, dayOfYearValue, isoWeekValue, millisecondValue, microsecondValue);
    }

    /// <summary>
    /// EN: Executes the SQL Server time zone offset benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de offset de fuso horario do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(int literalOffsetMinutes, string literalOffsetText, int utcOffsetMinutes, string utcOffsetText, DateTimeOffset offsetValue, DateTimeOffset switchedValue, int offsetMinutes, string offsetText, int negativeOffsetMinutes, string negativeOffsetText)> RunTemporalTimeZoneOffsetAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("TODATETIMEOFFSET"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the time zone offset benchmark.");
        }

        var literalOffsetMinutes = Convert.ToInt32(
            await Repo.ExecuteScalarAsync("SELECT DATEPART(tz, '2007-05-10 00:00:01.1234567 +05:10')"),
            CultureInfo.InvariantCulture);
        var literalOffsetText = Convert.ToString(
            await Repo.ExecuteScalarAsync("SELECT DATENAME(tz, '2007-05-10 00:00:01.1234567 +05:10')"),
            CultureInfo.InvariantCulture) ?? string.Empty;
        var utcOffsetMinutes = Convert.ToInt32(
            await Repo.ExecuteScalarAsync("SELECT DATEPART(tz, '2007-05-10T00:00:01.1234567Z')"),
            CultureInfo.InvariantCulture);
        var utcOffsetText = Convert.ToString(
            await Repo.ExecuteScalarAsync("SELECT DATENAME(tz, '2007-05-10T00:00:01.1234567Z')"),
            CultureInfo.InvariantCulture) ?? string.Empty;
        var offsetValue = NormalizeDateTimeOffsetValue(
            await Repo.ExecuteScalarAsync("SELECT TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00')"),
            Repo.Dialect.Provider);
        var switchedValue = NormalizeDateTimeOffsetValue(
            await Repo.ExecuteScalarAsync("SELECT SWITCHOFFSET('2020-02-29T10:11:12+01:00', '+00:00')"),
            Repo.Dialect.Provider);
        var offsetMinutes = Convert.ToInt32(
            await Repo.ExecuteScalarAsync("SELECT DATEPART(tzoffset, TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00'))"),
            CultureInfo.InvariantCulture);
        var offsetText = Convert.ToString(
            await Repo.ExecuteScalarAsync("SELECT DATENAME(tzoffset, TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00'))"),
            CultureInfo.InvariantCulture) ?? string.Empty;
        var negativeOffsetMinutes = Convert.ToInt32(
            await Repo.ExecuteScalarAsync("SELECT DATEPART(tzoffset, TODATETIMEOFFSET('2020-02-29T10:11:12', '-03:30'))"),
            CultureInfo.InvariantCulture);
        var negativeOffsetText = Convert.ToString(
            await Repo.ExecuteScalarAsync("SELECT DATENAME(tzoffset, TODATETIMEOFFSET('2020-02-29T10:11:12', '-03:30'))"),
            CultureInfo.InvariantCulture) ?? string.Empty;

        GC.KeepAlive(offsetValue);
        GC.KeepAlive(switchedValue);

        return (literalOffsetMinutes, literalOffsetText, utcOffsetMinutes, utcOffsetText, offsetValue, switchedValue, offsetMinutes, offsetText, negativeOffsetMinutes, negativeOffsetText);
    }

    /// <summary>
    /// EN: Executes the SQL Server FROMPARTS temporal constructors and keeps the provider result alive.
    /// PT-br: Executa os construtores temporais FROMPARTS do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(DateTime dateValue, DateTime dateTimeValue, DateTime dateTime2Value, DateTimeOffset dateTimeOffsetValue, TimeSpan timeValue, DateTime smallDateTimeValue)> RunTemporalFromPartsAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerScalarFunction("DATEFROMPARTS"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the FROMPARTS temporal benchmark.");
        }

        var dateValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATEFROMPARTS(2020, 2, 29)"));
        var dateTimeValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12, 0)"));
        var dateTime2Value = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 7)"));
        var dateTimeOffsetValue = NormalizeDateTimeOffsetValue(
            await Repo.ExecuteScalarAsync("SELECT DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 1, 0, 7)"),
            Repo.Dialect.Provider);
        var timeValue = NormalizeTimeSpanValue(
            await Repo.ExecuteScalarAsync("SELECT TIMEFROMPARTS(10, 11, 12, 1234567, 7)"));
        var smallDateTimeValue = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT SMALLDATETIMEFROMPARTS(2020, 2, 29, 10, 11)"));

        GC.KeepAlive(dateValue);
        GC.KeepAlive(dateTimeValue);
        GC.KeepAlive(dateTime2Value);
        GC.KeepAlive(dateTimeOffsetValue);
        GC.KeepAlive(timeValue);
        GC.KeepAlive(smallDateTimeValue);

        return (dateValue, dateTimeValue, dateTime2Value, dateTimeOffsetValue, timeValue, smallDateTimeValue);
    }

    /// <summary>
    /// EN: Executes the SQL Server EOMONTH benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark EOMONTH do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<DateTime> RunTemporalEndOfMonthAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerDateFunction("EOMONTH"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the EOMONTH temporal benchmark.");
        }

        var value = NormalizeTemporalValue(
            await Repo.ExecuteScalarAsync("SELECT EOMONTH('2020-02-15')"));

        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the SQL Server DATEDIFF_BIG benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark DATEDIFF_BIG do SQL Server e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<(long dayDiff, long weekDiff, long millisecondDiff, long microsecondDiff, long nanosecondDiff)> RunTemporalDateDiffBigAsync()
    {
        if (!Repo.Dialect.SupportsSqlServerDateFunction("DATEDIFF_BIG"))
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the DATEDIFF_BIG temporal benchmark.");
        }

        var dayDiff = Convert.ToInt64(
            await Repo.ExecuteScalarAsync("SELECT DATEDIFF_BIG(day, '2020-01-01', '2020-01-03')"),
            CultureInfo.InvariantCulture);
        var weekDiff = Convert.ToInt64(
            await Repo.ExecuteScalarAsync("SELECT DATEDIFF_BIG(week, '2005-12-31T23:59:59.9999999', '2006-01-01T00:00:00.0000000')"),
            CultureInfo.InvariantCulture);
        var millisecondDiff = Convert.ToInt64(
            await Repo.ExecuteScalarAsync("SELECT DATEDIFF_BIG(millisecond, '2020-02-10T10:11:12.123', '2020-02-10T10:11:12.124')"),
            CultureInfo.InvariantCulture);
        var microsecondDiff = Convert.ToInt64(
            await Repo.ExecuteScalarAsync("SELECT DATEDIFF_BIG(microsecond, '2020-02-10T10:11:12.1234567', '2020-02-10T10:11:12.1234577')"),
            CultureInfo.InvariantCulture);
        var nanosecondDiff = Convert.ToInt64(
            await Repo.ExecuteScalarAsync("SELECT DATEDIFF_BIG(ns, '2020-02-10T10:11:12.1245678', '2020-02-10T10:11:12.1245679')"),
            CultureInfo.InvariantCulture);

        GC.KeepAlive(dayDiff);
        GC.KeepAlive(weekDiff);
        GC.KeepAlive(millisecondDiff);
        GC.KeepAlive(microsecondDiff);
        GC.KeepAlive(nanosecondDiff);

        return (dayDiff, weekDiff, millisecondDiff, microsecondDiff, nanosecondDiff);
    }

    /// <summary>
    /// EN: Executes the provider string aggregation benchmark over sample user names.
    /// PT-br: Executa o benchmark de agregacao de strings do provedor sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<string?> RunStringAggregateAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregate(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    private static DateTime NormalizeTemporalValue(object? value)
    {
        var normalized = value switch
        {
            DateTime dateTime => dateTime,
            DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
            string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
        };

        return new DateTime(normalized.Ticks, normalized.Kind);
    }

    /// <summary>
    /// EN: Executes the ordered string aggregation benchmark over sample user names.
    /// PT-br: Executa o benchmark de agregacao ordenada de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<string?> RunStringAggregateOrderedAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateOrdered(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the distinct string aggregation benchmark over sample user names.
    /// PT-br: Executa o benchmark de agregacao distinta de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<string?> RunStringAggregateDistinctAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateDistinct(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the custom-separator string aggregation benchmark over sample user names.
    /// PT-br: Executa o benchmark de agregacao com separador customizado sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<string?> RunStringAggregateCustomSeparatorAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateCustomSeparator(Context, ";")), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the large-group string aggregation benchmark over sample user names.
    /// PT-br: Executa o benchmark de agregacao de strings em grupo grande sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<string?> RunStringAggregateLargeGroupAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateLargeGroup(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a string-aggregation summary query with total, distinct, and repeated-name counts over sample user names.
    /// PT-br: Executa uma consulta resumo de agregacao de strings com contagens total, distinta e de nomes repetidos sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<QueryResultSnapshot> RunStringAggregateSummaryMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var ordered = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateOrdered(Context)), CultureInfo.InvariantCulture);
        var totalCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var distinctCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(DISTINCT Name) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var bobCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE Name = 'Bob'"), CultureInfo.InvariantCulture);

        GC.KeepAlive(ordered);
        GC.KeepAlive(totalCount);
        GC.KeepAlive(distinctCount);
        GC.KeepAlive(bobCount);
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Ordered", "TotalCount", "DistinctCount", "BobCount"]),
            Rows =
            [
                new QueryResultRowSnapshot
                {
                    Values = [ordered, totalCount, distinctCount, bobCount],
                },
            ],
        };
    }

    /// <summary>
    /// EN: Executes a grouped string report with CASE and COALESCE over sample user names.
    /// PT-br: Executa um relatorio agrupado de strings com CASE e COALESCE sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<QueryResultSnapshot> RunStringAggregateGroupCaseMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    CAST(CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END AS CHAR(10)) AS NameGroup,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName
FROM {Context.TbUsersFullName}
GROUP BY CAST(CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END AS CHAR(10))
ORDER BY NameGroup
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateStringAggregateGroupCaseRow(reader, "B", 2, 1, "Bob", "Bob");
        rows.Add(NormalizeStringAggregateGroupCaseSnapshotRow(QueryResultSnapshotReader.CaptureRow(reader)));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateStringAggregateGroupCaseRow(reader, "Other", 3, 3, "Alice", "Delta");
        rows.Add(NormalizeStringAggregateGroupCaseSnapshotRow(QueryResultSnapshotReader.CaptureRow(reader)));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameGroup", "TotalCount", "DistinctCount", "FirstName", "LastName"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped name-initial report with distinct counts and HAVING filtering over the configured users table.
    /// PT-br: Executa um relatorio agrupado por inicial do nome com contagens distintas e filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public async Task<QueryResultSnapshot> RunGroupByNameInitialMatrixAsync(params object[] pars)
    {
        var initialExpr = $"UPPER({Repo.Dialect.StringPrefixExpression("Name", 1)})";
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    SUM(CASE WHEN Name = 'Alice' THEN 1 ELSE 0 END) AS AliceCount,
    SUM(CASE WHEN Name = 'Bob' THEN 1 ELSE 0 END) AS BobCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName,
    CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END AS HasAtLeastTwo
FROM {Context.TbUsersFullName}
GROUP BY {initialExpr}
HAVING COUNT(*) >= 2
ORDER BY {initialExpr}
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "A", 3, 2, 2, 0, "Adam", "Alice", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "B", 3, 2, 0, 2, "Bob", "Brian", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "C", 2, 2, 0, 0, "Carla", "Chris", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameInitial", "TotalCount", "DistinctCount", "AliceCount", "BobCount", "FirstName", "LastName", "HasAtLeastTwo"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped name report with HAVING filtering over the configured users table.
    /// PT-br: Executa um relatorio agrupado por nome com filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public async Task<QueryResultSnapshot> RunGroupByNameHavingMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    COUNT(*) AS TotalCount
FROM {Context.TbUsersFullName}
GROUP BY Name
HAVING COUNT(*) >= 2
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "TotalCount"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a GROUP BY ordinal query over the configured users table and validates grouped counts.
    /// PT-br: Executa uma consulta GROUP BY ordinal na tabela de usuarios configurada e valida as contagens agrupadas.
    /// </summary>
    public async Task<QueryResultSnapshot> RunGroupByOrdinalMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsGroupByOrdinal)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support GROUP BY ordinal benchmarks.");
        }

        var initialExpr = $"UPPER({Repo.Dialect.StringPrefixExpression("Name", 1)})";
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount
FROM {Context.TbUsersFullName}
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("A");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("B");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("C");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameInitial", "TotalCount"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an ORDER BY ordinal query over the configured users table and validates the output order.
    /// PT-br: Executa uma consulta ORDER BY ordinal na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<QueryResultSnapshot> RunOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    Id
FROM {Context.TbUsersFullName}
ORDER BY 2 DESC
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bravo");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alpha");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "Id"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a DISTINCT query ordered by ordinal and validates the projected names.
    /// PT-br: Executa uma consulta DISTINCT ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public async Task<QueryResultSnapshot> RunDistinctOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(4);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    Name
FROM {Context.TbUsersFullName}
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Delta");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a DISTINCT query with a text filter ordered by ordinal and validates the projected names.
    /// PT-br: Executa uma consulta DISTINCT com filtro de texto ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public async Task<QueryResultSnapshot> RunDistinctLikeOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    UPPER(Name)
FROM {Context.TbUsersFullName}
WHERE UPPER(Name) LIKE '%A%'
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("ALICE");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("CHARLIE");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("DELTA");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["UPPER(Name)"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an IN-list predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado IN com lista na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunInListPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name IN ('Alice', 'Charlie')
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a BETWEEN predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado BETWEEN na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunBetweenPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id BETWEEN 2 AND 4
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a LIKE predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado LIKE na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunLikePredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name LIKE 'A%'
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY query over the configured users table and returns the matching rowset.
    /// PT-br: Executa uma consulta combinada com BETWEEN, LIKE e ORDER BY na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<QueryResultSnapshot> RunBetweenLikeOrderByMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Id BETWEEN 1 AND 4
  AND Name LIKE 'A%'
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Aaron");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a NOT LIKE predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado NOT LIKE na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunNotLikePredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name NOT LIKE 'A%'
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a not-equal predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado diferente de na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunNotEqualPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name <> 'Bob'
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes an equality predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado de igualdade na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunEqualPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name = 'Bob'
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a parameterized name lookup over the configured users table and returns the matched name.
    /// PT-br: Executa uma consulta parametrizada por nome na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public async Task<string?> RunParameterSelectByNameMatrixAsync(params object[] pars)
    {
        var name = (string)pars[0];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Name = {Repo.Dialect.Parameter("name")}
""";

        Repo.Dialect.AddParameter(command, "name", DbType.String, name);

        var result = Convert.ToString(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(name);
        GC.KeepAlive(result);
        GC.KeepAlive(name);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameterized id lookup over the configured users table and returns the matched name.
    /// PT-br: Executa uma consulta parametrizada por id na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public async Task<string?> RunParameterSelectByIdMatrixAsync(params object[] pars)
    {
        var id = Convert.ToInt32(pars[0], CultureInfo.InvariantCulture);
        var expectedName = (string)pars[1];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""";

        Repo.Dialect.AddParameter(command, "id", DbType.Int32, id);

        var result = Convert.ToString(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(expectedName);
        GC.KeepAlive(result);
        GC.KeepAlive(id);
        GC.KeepAlive(expectedName);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameter roundtrip over typed user columns and validates string, numeric, boolean, date, and null parameters.
    /// PT-br: Executa um roundtrip de parametros sobre colunas tipadas de usuarios e valida parametros de texto, numericos, booleanos, data e nulos.
    /// </summary>
    public async Task<int> RunParameterRoundTripMatrixAsync(params object[] pars)
    {
        var id = (int)pars[0];
        var name = (string)pars[1];
        var email = pars[2] is DBNull ? null : (string?)pars[2];
        var isActive = (bool)pars[3];
        var age = (short)pars[4];
        var balance = (decimal)pars[5];
        var createdAt = (DateTime)pars[6];
        var updatedAt = pars[7] is DBNull ? (DateTime?)null : (DateTime)pars[7];
        var profileJson = pars[8] is DBNull ? null : (string?)pars[8];

        using var insertCommand = Repo.Cnn.CreateCommand();
        insertCommand.CommandText = $"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")},
    {Repo.Dialect.Parameter("email")},
    {Repo.Dialect.Parameter("isActive")},
    {Repo.Dialect.Parameter("age")},
    {Repo.Dialect.Parameter("balance")},
    {Repo.Dialect.Parameter("createdAt")},
    {Repo.Dialect.Parameter("updatedAt")},
    {Repo.Dialect.JsonParameter("profileJson")}
)
""";

        AddParameter(insertCommand, "id", DbType.Int32, id);
        AddParameter(insertCommand, "name", DbType.String, name);
        AddParameter(insertCommand, "email", DbType.String, email is null ? DBNull.Value : email);
        AddParameter(insertCommand, "isActive", DbType.Boolean, isActive);
        AddParameter(insertCommand, "age", DbType.Int16, age);
        AddParameter(insertCommand, "balance", DbType.Decimal, balance);
        var createdAtParameter = NormalizeNpgsqlDateTimeInput(createdAt);
        object? updatedAtParameter = updatedAt is null ? DBNull.Value : NormalizeNpgsqlDateTimeInput(updatedAt.Value);

        AddParameter(insertCommand, "createdAt", DbType.DateTime, createdAtParameter);
        AddParameter(insertCommand, "updatedAt", DbType.DateTime, updatedAtParameter);
        AddParameter(insertCommand, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);

        (await insertCommand.ExecuteNonQueryAsync()).Should().Be(1);

        using var selectCommand = Repo.Cnn.CreateCommand();
        selectCommand.CommandText = $"""
SELECT
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""";

        AddParameter(selectCommand, "id", DbType.Int32, id);

        using var reader = await selectCommand.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(name);
        (await reader.IsDBNullAsync(1) ? null : Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture)).Should().Be(email);
        Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(isActive);
        Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeDateTimeValue(reader.GetValue(5)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

        var updatedAtText = await reader.IsDBNullAsync(6)
            ? null
            : NormalizeDateTimeValue(reader.GetValue(6)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        (updatedAt is null ? null : updatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Should().Be(updatedAtText);
        DbSqlLikeMem.TestTools.Json.JsonTextAssertions.ShouldMatchJsonText(
            await reader.IsDBNullAsync(7) ? null : Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture),
            profileJson);

        (await reader.ReadAsync()).Should().BeFalse();

        GC.KeepAlive(id);
        GC.KeepAlive(name);
        GC.KeepAlive(email);
        GC.KeepAlive(isActive);
        GC.KeepAlive(age);
        GC.KeepAlive(balance);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(updatedAt);
        GC.KeepAlive(profileJson);
        return 1;
    }

    /// <summary>
    /// EN: Executes a typed parameter projection and validates ANSI text, fixed-length text, numeric, temporal, GUID, and binary values returned by provider-specific parameter objects.
    /// PT-br: Executa uma projeção de parametros tipados e valida valores de texto ANSI, texto de comprimento fixo, numericos, temporais, GUID e binario retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public async Task<int> RunParameterTypeMatrixAsync(params object[] pars)
    {
        var text = (string)pars[0];
        var ansiText = (string)pars[1];
        var ansiFixedText = (string)pars[2];
        var fixedText = (string)pars[3];
        var int16Value = (short)pars[4];
        var int32Value = (int)pars[5];
        var int64Value = (long)pars[6];
        var boolValue = (bool)pars[7];
        var decimalValue = (decimal)pars[8];
        var doubleValue = (double)pars[9];
        var timeSpanValue = (TimeSpan)pars[10];
        var dateTimeOffsetValue = (DateTimeOffset)pars[11];
        var dateTimeValue = (DateTime)pars[12];
        var guidValue = (Guid)pars[13];
        var binaryValue = (byte[])pars[14];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("text")} AS VARCHAR(100)) AS TextValue,
    CAST({Repo.Dialect.Parameter("ansiText")} AS VARCHAR(100)) AS AnsiTextValue,
    CAST({Repo.Dialect.Parameter("ansiFixedText")} AS CHAR(20)) AS AnsiFixedTextValue,
    CAST({Repo.Dialect.Parameter("fixedText")} AS CHAR(20)) AS FixedTextValue,
    CAST({Repo.Dialect.Parameter("int16Value")} AS SMALLINT) AS Int16Value,
    CAST({Repo.Dialect.Parameter("int32Value")} AS INTEGER) AS Int32Value,
    CAST({Repo.Dialect.Parameter("int64Value")} AS BIGINT) AS Int64Value,
    CAST({Repo.Dialect.Parameter("boolValue")} AS BOOLEAN) AS BoolValue,
    CAST({Repo.Dialect.Parameter("decimalValue")} AS DECIMAL(19,4)) AS DecimalValue,
    CAST({Repo.Dialect.Parameter("doubleValue")} AS DOUBLE) AS DoubleValue,
    CAST({Repo.Dialect.Parameter("timeSpanValue")} AS VARCHAR(32)) AS TimeSpanValue,
    CAST({Repo.Dialect.Parameter("dateTimeOffsetValue")} AS VARCHAR(40)) AS DateTimeOffsetValue,
    CAST({Repo.Dialect.Parameter("dateTimeValue")} AS TIMESTAMP) AS DateTimeValue,
    CAST({Repo.Dialect.Parameter("guidValue")} AS VARCHAR(36)) AS GuidValue,
    CAST({Repo.Dialect.Parameter("binaryValue")} AS VARCHAR(4) FOR BIT DATA) AS BinaryValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("text")} AS TextValue,
    {Repo.Dialect.Parameter("ansiText")} AS AnsiTextValue,
    {Repo.Dialect.Parameter("ansiFixedText")} AS AnsiFixedTextValue,
    {Repo.Dialect.Parameter("fixedText")} AS FixedTextValue,
    {Repo.Dialect.Parameter("int16Value")} AS Int16Value,
    {Repo.Dialect.Parameter("int32Value")} AS Int32Value,
    {Repo.Dialect.Parameter("int64Value")} AS Int64Value,
    {Repo.Dialect.Parameter("boolValue")} AS BoolValue,
    {Repo.Dialect.Parameter("decimalValue")} AS DecimalValue,
    {Repo.Dialect.Parameter("doubleValue")} AS DoubleValue,
    {Repo.Dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {Repo.Dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {Repo.Dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {Repo.Dialect.Parameter("guidValue")} AS GuidValue,
    {Repo.Dialect.Parameter("binaryValue")} AS BinaryValue
""");

        AddParameter(command, "text", DbType.String, text);
        AddParameter(command, "ansiText", DbType.AnsiString, ansiText);
        AddParameter(command, "ansiFixedText", DbType.AnsiStringFixedLength, ansiFixedText);
        AddParameter(command, "fixedText", DbType.StringFixedLength, fixedText);
        AddParameter(command, "int16Value", DbType.Int16, int16Value);
        AddParameter(command, "int32Value", DbType.Int32, int32Value);
        AddParameter(command, "int64Value", DbType.Int64, int64Value);
        AddParameter(command, "boolValue", DbType.Boolean, boolValue);
        AddParameter(command, "decimalValue", DbType.Decimal, decimalValue);
        AddParameter(command, "doubleValue", DbType.Double, doubleValue);
        AddParameter(command, "timeSpanValue", DbType.Time, timeSpanValue);
        AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, dateTimeOffsetValue);
        AddParameter(command, "dateTimeValue", DbType.DateTime, NormalizeNpgsqlDateTimeInput(dateTimeValue));
        AddParameter(command, "guidValue", DbType.Guid, guidValue);
        AddParameter(command, "binaryValue", DbType.Binary, binaryValue);

        using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(text);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(ansiText);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(ansiFixedText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(fixedText);
        Convert.ToInt16(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(int16Value);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(int32Value);
        Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(int64Value);
        Convert.ToBoolean(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(boolValue);
        Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(decimalValue);
        Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(doubleValue);
        NormalizeTimeSpanValue(reader.GetValue(10)).Should().Be(timeSpanValue);
        NormalizeDateTimeOffsetValue(reader.GetValue(11), Repo.Dialect.Provider).Should().Be(Repo.Dialect.Provider == ProviderId.Oracle ? new DateTimeOffset(dateTimeOffsetValue.DateTime, TimeSpan.Zero) : dateTimeOffsetValue);
        NormalizeDateTimeValue(reader.GetValue(12)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        NormalizeGuidValue(reader.GetValue(13)).Should().Be(guidValue);
        NormalizeBinaryValue(reader.GetValue(14), Repo.Dialect.Provider).Should().Equal(binaryValue);

        (await reader.ReadAsync()).Should().BeFalse();

        GC.KeepAlive(text);
        GC.KeepAlive(ansiText);
        GC.KeepAlive(ansiFixedText);
        GC.KeepAlive(fixedText);
        GC.KeepAlive(int16Value);
        GC.KeepAlive(int32Value);
        GC.KeepAlive(int64Value);
        GC.KeepAlive(boolValue);
        GC.KeepAlive(decimalValue);
        GC.KeepAlive(doubleValue);
        GC.KeepAlive(timeSpanValue);
        GC.KeepAlive(dateTimeOffsetValue);
        GC.KeepAlive(dateTimeValue);
        GC.KeepAlive(guidValue);
        GC.KeepAlive(binaryValue);
        return 1;
    }

    /// <summary>
    /// EN: Executes a compact typed parameter projection for date and currency values returned by provider-specific parameter objects.
    /// PT-br: Executa uma projeção compacta de parametros tipados para valores de data e moeda retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public async Task<int> RunParameterDateCurrencyMatrixAsync(params object[] pars)
    {
        var dateValue = (DateTime)pars[0];
        var currencyValue = (decimal)pars[1];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("dateValue")} AS DATE) AS DateValue,
    CAST({Repo.Dialect.Parameter("currencyValue")} AS DECIMAL(19,2)) AS CurrencyValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("dateValue")} AS DateValue,
    {Repo.Dialect.Parameter("currencyValue")} AS CurrencyValue
""");

        AddParameter(command, "dateValue", DbType.Date, dateValue);
        AddParameter(command, "currencyValue", DbType.Currency, currencyValue);

        using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        NormalizeDateTimeValue(reader.GetValue(0)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            .Should().Be(dateValue.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(currencyValue);

        (await reader.ReadAsync()).Should().BeFalse();
        GC.KeepAlive(dateValue);
        GC.KeepAlive(currencyValue);
        return 1;
    }

    /// <summary>
    /// EN: Executes the broad parameter projection benchmark and returns the first projected value.
    /// PT-br: Executa o benchmark amplo de projeção de parametros e retorna o primeiro valor projetado.
    /// </summary>
    public string? RunParameterProjection()
    {
        var createdAt = Repo.Dialect.Provider == ProviderId.Npgsql
            ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
            : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        var dateValue = createdAt.Date;
        var currencyValue = 123.45m;

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("textValue")} AS VARCHAR(100)) AS TextValue,
    CAST({Repo.Dialect.Parameter("ansiTextValue")} AS VARCHAR(100)) AS AnsiTextValue,
    CAST({Repo.Dialect.Parameter("ansiFixedTextValue")} AS CHAR(20)) AS AnsiFixedTextValue,
    CAST({Repo.Dialect.Parameter("fixedTextValue")} AS CHAR(20)) AS FixedTextValue,
    CAST({Repo.Dialect.Parameter("int16Value")} AS SMALLINT) AS Int16Value,
    CAST({Repo.Dialect.Parameter("int32Value")} AS INTEGER) AS Int32Value,
    CAST({Repo.Dialect.Parameter("int64Value")} AS BIGINT) AS Int64Value,
    CAST({Repo.Dialect.Parameter("boolValue")} AS BOOLEAN) AS BoolValue,
    CAST({Repo.Dialect.Parameter("decimalValue")} AS DECIMAL(19,4)) AS DecimalValue,
    CAST({Repo.Dialect.Parameter("doubleValue")} AS DOUBLE) AS DoubleValue,
    CAST({Repo.Dialect.Parameter("timeSpanValue")} AS VARCHAR(32)) AS TimeSpanValue,
    CAST({Repo.Dialect.Parameter("dateTimeOffsetValue")} AS VARCHAR(40)) AS DateTimeOffsetValue,
    CAST({Repo.Dialect.Parameter("dateTimeValue")} AS TIMESTAMP) AS DateTimeValue,
    CAST({Repo.Dialect.Parameter("guidValue")} AS VARCHAR(36)) AS GuidValue,
    CAST({Repo.Dialect.Parameter("binaryValue")} AS VARCHAR(4) FOR BIT DATA) AS BinaryValue,
    CAST({Repo.Dialect.Parameter("dateValue")} AS DATE) AS DateValue,
    CAST({Repo.Dialect.Parameter("currencyValue")} AS DECIMAL(19,2)) AS CurrencyValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("textValue")} AS TextValue,
    {Repo.Dialect.Parameter("ansiTextValue")} AS AnsiTextValue,
    {Repo.Dialect.Parameter("ansiFixedTextValue")} AS AnsiFixedTextValue,
    {Repo.Dialect.Parameter("fixedTextValue")} AS FixedTextValue,
    {Repo.Dialect.Parameter("int16Value")} AS Int16Value,
    {Repo.Dialect.Parameter("int32Value")} AS Int32Value,
    {Repo.Dialect.Parameter("int64Value")} AS Int64Value,
    {Repo.Dialect.Parameter("boolValue")} AS BoolValue,
    {Repo.Dialect.Parameter("decimalValue")} AS DecimalValue,
    {Repo.Dialect.Parameter("doubleValue")} AS DoubleValue,
    {Repo.Dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {Repo.Dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {Repo.Dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {Repo.Dialect.Parameter("guidValue")} AS GuidValue,
    {Repo.Dialect.Parameter("binaryValue")} AS BinaryValue
""");

        AddParameter(command, "textValue", DbType.String, "benchmark");
        AddParameter(command, "ansiTextValue", DbType.AnsiString, "ansi");
        AddParameter(command, "ansiFixedTextValue", DbType.AnsiStringFixedLength, "fixed-ansi");
        AddParameter(command, "fixedTextValue", DbType.StringFixedLength, "fixed-text");
        AddParameter(command, "int16Value", DbType.Int16, (short)16);
        AddParameter(command, "int32Value", DbType.Int32, 32);
        AddParameter(command, "int64Value", DbType.Int64, 64L);
        AddParameter(command, "boolValue", DbType.Boolean, true);
        AddParameter(command, "decimalValue", DbType.Decimal, 12.34m);
        AddParameter(command, "doubleValue", DbType.Double, 56.78d);
        AddParameter(command, "timeSpanValue", DbType.Time, TimeSpan.FromHours(1.5));
        AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));
        AddParameter(command, "dateTimeValue", DbType.DateTime, NormalizeNpgsqlDateTimeInput(createdAt));
        AddParameter(command, "guidValue", DbType.Guid, Guid.Parse("11111111-2222-3333-4444-555555555555"));
        AddParameter(command, "binaryValue", DbType.Binary, new byte[] { 1, 2, 3, 4 });
        if (Repo.Dialect.Provider == ProviderId.Db2)
        {
            AddParameter(command, "dateValue", DbType.Date, dateValue);
            AddParameter(command, "currencyValue", DbType.Currency, currencyValue);
        }

        using var reader = command.ExecuteReader();
        reader.Read();
        var value = Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture);
        value.Should().Be("benchmark");
        GC.KeepAlive(value);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(dateValue);
        GC.KeepAlive(currencyValue);
        return value;
    }

    private static DateTime NormalizeDateTimeValue(object? value)
    {
        return value switch
        {
            DateTime dateTime => dateTime,
            DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
            string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTime parameter returned a null value."),
            _ when TryNormalizeDateOnlyValue(value, out var dateOnly) => dateOnly,
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
        };
    }

    private static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }

    private static bool TryNormalizeTimeOnlyValue(object? value, out TimeSpan timeSpan)
    {
        timeSpan = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.TimeOnly", StringComparison.Ordinal))
            return false;

        if (type.GetMethod("ToTimeSpan", Type.EmptyTypes)?.Invoke(value, null) is not TimeSpan normalized)
            return false;

        timeSpan = normalized;
        return true;
    }

    private DateTime NormalizeNpgsqlDateTimeInput(DateTime value)
    {
        if (Repo.Dialect.Provider == ProviderId.Npgsql && value.Kind == DateTimeKind.Unspecified)
            return DateTime.SpecifyKind(value, DateTimeKind.Utc);

        return value;
    }

    private static Guid NormalizeGuidValue(object? value)
    {
        return value switch
        {
            Guid guid => guid,
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            string text => Guid.Parse(text),
            null => throw new InvalidOperationException("GUID parameter returned a null value."),
            _ => Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("GUID parameter returned an unconvertible value."))
        };
    }

    private static DateTimeOffset NormalizeDateTimeOffsetValue(object? value, ProviderId provider)
    {
        return value switch
        {
            DateTimeOffset dateTimeOffset => provider == ProviderId.Oracle ? new DateTimeOffset(dateTimeOffset.DateTime, TimeSpan.Zero) : dateTimeOffset,
            DateTime dateTime => provider == ProviderId.Oracle ? new DateTimeOffset(dateTime, TimeSpan.Zero) : new DateTimeOffset(dateTime, TimeSpan.Zero),
            string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTimeOffset parameter returned a null value."),
            _ => DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("DateTimeOffset parameter returned an unconvertible value."), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
        };
    }

    private static TimeSpan NormalizeTimeSpanValue(object? value)
    {
        return value switch
        {
            TimeSpan timeSpan => timeSpan,
            _ when TryNormalizeTimeOnlyValue(value, out var timeOnly) => timeOnly,
            DateTime dateTime => dateTime.TimeOfDay,
            string text => ParseTimeSpanText(text),
            null => throw new InvalidOperationException("TimeSpan parameter returned a null value."),
            _ => ParseTimeSpanText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("TimeSpan parameter returned an unconvertible value."))
        };
    }

    private static TimeSpan ParseTimeSpanText(string text)
    {
        if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var timeSpan))
        {
            return timeSpan;
        }

        var trimmed = text.Trim();
        var separatorIndex = trimmed.IndexOf(' ');
        if (separatorIndex > 0
            && int.TryParse(trimmed[..separatorIndex], NumberStyles.Integer, CultureInfo.InvariantCulture, out var days)
            && TimeSpan.TryParse(trimmed[(separatorIndex + 1)..], CultureInfo.InvariantCulture, out var remainder))
        {
            return TimeSpan.FromDays(days) + remainder;
        }

        return TimeSpan.Parse(text, CultureInfo.InvariantCulture);
    }

    private static byte[] NormalizeBinaryValue(object? value, ProviderId provider)
    {
        return value switch
        {
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            Memory<byte> memory => memory.ToArray(),
            string text => ParseBinaryText(text),
            DBNull when provider == ProviderId.Oracle => Array.Empty<byte>(),
            null => throw new InvalidOperationException("Binary parameter returned a null value."),
            _ => throw new InvalidOperationException($"Unsupported binary parameter type: {value.GetType().FullName}.")
        };
    }

    private static byte[] ParseBinaryText(string text)
    {
        var trimmed = text.Trim();
        if (trimmed.Length == 0)
            return Array.Empty<byte>();

        var hexCandidate = trimmed;
        if (trimmed.StartsWith("X'", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith("'", StringComparison.OrdinalIgnoreCase))
        {
            hexCandidate = trimmed[2..^1];
        }
        else if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hexCandidate = trimmed[2..];
        }

        hexCandidate = hexCandidate.Replace(" ", string.Empty);

        if (hexCandidate.Length == 0)
            return Array.Empty<byte>();

        if (IsHexString(hexCandidate))
        {
            if (hexCandidate.Length % 2 != 0)
                throw new InvalidOperationException($"Binary parameter returned an odd-length hex string: {text}");

            return ParseHexBytes(hexCandidate);
        }

        return Encoding.GetEncoding("ISO-8859-1").GetBytes(trimmed);
    }

    private static bool IsHexString(string value)
    {
        foreach (var ch in value)
        {
            if ((ch >= '0' && ch <= '9')
                || (ch >= 'A' && ch <= 'F')
                || (ch >= 'a' && ch <= 'f'))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private static byte[] ParseHexBytes(string hexCandidate)
    {
        var bytes = new byte[hexCandidate.Length / 2];
        for (var i = 0; i < hexCandidate.Length; i += 2)
        {
            if (!byte.TryParse(hexCandidate.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
                throw new InvalidOperationException($"Binary parameter returned an invalid hex string: {hexCandidate}");

            bytes[i / 2] = part;
        }

        return bytes;
    }

    /// <summary>
    /// EN: Executes a greater-than predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado maior que na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunGreaterThanPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id > 3
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a less-than predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado menor que na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunLessThanPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id < 3
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a greater-than-or-equal predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado maior ou igual na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunGreaterThanOrEqualPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id >= 3
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes a less-than-or-equal predicate over the configured users table and returns the matching rowset.
    /// PT-br: Executa um predicado menor ou igual na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public Task<QueryResultSnapshot> RunLessThanOrEqualPredicateMatrixAsync(params object[] pars)
        => CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id <= 3
ORDER BY Id
""");

    /// <summary>
    /// EN: Executes an ORDER BY Name query over the configured users table and validates the output order.
    /// PT-br: Executa uma consulta ORDER BY Name na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<QueryResultSnapshot> RunOrderByNameMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an ORDER BY Name descending query over the configured users table and validates the output order.
    /// PT-br: Executa uma consulta ORDER BY Name descendente na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<QueryResultSnapshot> RunOrderByNameDescendingMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
ORDER BY Name DESC
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a paged name query using ROW_NUMBER and validates the selected page rows.
    /// PT-br: Executa uma consulta paginada por nome usando ROW_NUMBER e valida as linhas da pagina selecionada.
    /// </summary>
    public async Task<QueryResultSnapshot> RunNamePaginationMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM (
    SELECT Name, ROW_NUMBER() OVER (ORDER BY Name) AS rn
    FROM {Context.TbUsersFullName}
) q
WHERE rn BETWEEN 2 AND 4
ORDER BY rn
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Delta");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a native paged name query and validates the selected page rows for the configured users table.
    /// PT-br: Executa uma consulta nativa paginada por nome e valida as linhas da pagina selecionada na tabela de usuarios configurada.
    /// </summary>
    public async Task<QueryResultSnapshot> RunPagedNameProjectionMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.PagedNameProjection(Context.TbUsersFullName, 1, 2);

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Reads a current-time predicate query result from the configured users table.
    /// PT-br: Lê o resultado de uma consulta com predicado de tempo atual na tabela de usuarios configurada.
    /// </summary>
    public async Task<int> RunTemporalNowWhereAsync(params object[] pars)
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalNowWhere(Context));
        GC.KeepAlive(value);
        return Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Reads a current-time ordering query result from the configured users table.
    /// PT-br: Lê o resultado de uma consulta com ordenação por tempo atual na tabela de usuarios configurada.
    /// </summary>
    public async Task<string?> RunTemporalNowOrderByAsync(params object[] pars)
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalNowOrderBy(Context));
        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private string[] NormalizeSnapshotColumnNames(string[] columnNames)
    {
        if (Repo.Dialect.Provider == ProviderId.Npgsql)
        {
            var normalized = new string[columnNames.Length];
            for (var i = 0; i < columnNames.Length; i++)
            {
                normalized[i] = columnNames[i].ToLowerInvariant();
            }

            return normalized;
        }

        if (Repo.Dialect.Provider is not ProviderId.Oracle and not ProviderId.Db2 and not ProviderId.Firebird)
            return columnNames;

        var normalized2 = new string[columnNames.Length];
        for (var i = 0; i < columnNames.Length; i++)
            normalized2[i] = columnNames[i].ToUpperInvariant();

        return normalized2;
    }

    private static void ValidateStringAggregateGroupCaseRow(
        DbDataReader reader,
        string expectedNameGroup,
        int expectedTotalCount,
        int expectedDistinctCount,
        string expectedFirstName,
        string expectedLastName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(expectedNameGroup);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedTotalCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDistinctCount);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedFirstName);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedLastName);
    }

    private static void ValidateGroupByNameInitialRow(
        DbDataReader reader,
        string expectedNameInitial,
        int expectedTotalCount,
        int expectedDistinctCount,
        int expectedAliceCount,
        int expectedBobCount,
        string expectedFirstName,
        string expectedLastName,
        int expectedHasAtLeastTwo)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedNameInitial);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedTotalCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDistinctCount);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedAliceCount);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedBobCount);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedFirstName);
        Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedLastName);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedHasAtLeastTwo);
    }

    private static void ValidateNamePaginationRow(DbDataReader reader, string expectedName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
    }

    private QueryResultRowSnapshot NormalizeStringAggregateGroupCaseSnapshotRow(QueryResultRowSnapshot row)
    {
        if (Repo.Dialect.Provider is not ProviderId.Firebird
            and not ProviderId.Db2
            and not ProviderId.Oracle
            and not ProviderId.SqlServer
            and not ProviderId.SqlAzure
            and not ProviderId.Npgsql)
            return row;

        if (row.Values.Count == 0)
            return row;

        var nameGroup = Convert.ToString(row.Values[0], CultureInfo.InvariantCulture);
        if (string.IsNullOrEmpty(nameGroup))
            return row;

        var trimmedNameGroup = nameGroup.TrimEnd();

        if (trimmedNameGroup == nameGroup)
            return row;

        var values = row.Values.ToArray();
        values[0] = trimmedNameGroup;

        return new QueryResultRowSnapshot
        {
            Values = values,
        };
    }
}
#pragma warning restore AsyncFixer01
