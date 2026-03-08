# NuGet readiness validation report

Date: 2026-03-07

## Executed validations

1. Checked runtime availability for .NET CLI:
   - `dotnet --info`
   - Result: `dotnet: command not found` in this container.

2. Tried installing .NET SDK via official install script:
   - `curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh`
   - Result: HTTP 403 from network gateway in this environment.

3. Tried installing .NET SDK via apt repositories:
   - `apt-get update -y`
   - Result: repository access blocked with HTTP 403, package install unavailable.

4. Static packaging checks (without build):
   - Verified global multi-target frameworks in `src/Directory.Build.props`.
   - Verified non-package internal projects are marked `IsPackable=false`:
     - `DbSqlLikeMem.MySqlConsoleGenerator`
     - `DbSqlLikeMem.VisualStudioExtension.Core`
   - Verified package metadata configuration is centralized in `src/Directory.Build.props`.

5. Metadata checks (NuGet quality gates):
  - Present: `Version`, `Authors`, `PackageTags`, `PackageReadmeFile`, `RepositoryType`, `RepositoryUrl`, `PackageProjectUrl`, `PackageLicenseExpression`, `PackageRequireLicenseAcceptance`, `PackageReleaseNotes`, `PublishRepositoryUrl`, `SymbolPackageFormat`.
  - Post-pack artifact audit is now available through `scripts/check_nuget_package_metadata.py`.
  - The audit derives expected values from `src/Directory.Build.props` and checks `.nuspec` fields for `version`, `authors`, `repository`, `projectUrl`, `readme`, `tags`, `releaseNotes`, `license` and `requireLicenseAcceptance`, plus the presence of the readme file inside the package and coherence with the `.nupkg` filename suffix.

## Conclusion

- The repository is structurally close to NuGet publication readiness for multiple frameworks and now has a reusable metadata audit script for `.nupkg` artifacts.
- Full runtime validation (`restore`, `build`, `test`, `pack`) could not be executed in this environment due blocked .NET installation/network access.
- Before final release tag, run in CI or a machine with .NET SDK:

```bash
dotnet restore src/DbSqlLikeMem.slnx
dotnet build src/DbSqlLikeMem.slnx -c Release
dotnet test src/DbSqlLikeMem.slnx -c Release --no-build
dotnet pack src/DbSqlLikeMem.slnx -c Release -o ./artifacts
python3 scripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts
```
