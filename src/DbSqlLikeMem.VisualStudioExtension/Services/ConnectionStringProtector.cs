using System.Security.Cryptography;
using System.Text;

namespace DbSqlLikeMem.VisualStudioExtension.Services;

internal static class ConnectionStringProtector
{
    private const string Prefix = "enc:";

    public static string Protect(string plainText)
    {
        if (string.IsNullOrWhiteSpace(plainText))
        {
            return plainText;
        }

        var bytes = Encoding.UTF8.GetBytes(plainText);
        var protectedBytes = ProtectedData.Protect(bytes, null, DataProtectionScope.CurrentUser);
        return Prefix + Convert.ToBase64String(protectedBytes);
    }

    public static string Unprotect(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || !value.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return value;
        }

        var payload = value.Substring(Prefix.Length);
        var protectedBytes = Convert.FromBase64String(payload);
        var plainBytes = ProtectedData.Unprotect(protectedBytes, null, DataProtectionScope.CurrentUser);
        return Encoding.UTF8.GetString(plainBytes);
    }
}
