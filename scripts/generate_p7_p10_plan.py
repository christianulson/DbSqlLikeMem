#!/usr/bin/env python3
"""Generate an actionable P7-P10 implementation plan mapped to current test files."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "p7-p10-implementation-plan.md"

PROVIDERS = [
    ("MySQL", "MySql"),
    ("SQL Server", "SqlServer"),
    ("Oracle", "Oracle"),
    ("PostgreSQL (Npgsql)", "Npgsql"),
    ("SQLite", "Sqlite"),
    ("DB2", "Db2"),
]

PILLARS = {
    "P7 (DML avançado)": [
        "InsertOnDuplicateTests",
        "UpdateStrategyTests",
        "DeleteStrategyTests",
    ],
    "P8 (Paginação/ordenação)": [
        "UnionLimitAndJsonCompatibilityTests",
    ],
    "P9 (JSON)": [
        "UnionLimitAndJsonCompatibilityTests",
    ],
    "P10 (Procedures/OUT params)": [
        "StoredProcedureExecutionTests",
        "StoredProcedureSignatureTests",
    ],
}


def find_test_paths(provider_folder: str, tokens: list[str]) -> list[str]:
    base = ROOT / "src" / f"DbSqlLikeMem.{provider_folder}.Test"
    if not base.exists():
        return []

    found: list[str] = []
    for token in tokens:
        matches = sorted(base.rglob(f"*{token}*.cs"))
        found.extend(str(m.relative_to(ROOT)) for m in matches)
    # de-duplicate preserving order
    unique = list(dict.fromkeys(found))
    return unique


def build() -> str:
    lines: list[str] = []
    lines.append("# Plano executável — P7 a P10")
    lines.append("")
    lines.append(
        "Documento gerado por `scripts/generate_p7_p10_plan.py` para orientar implementação técnica "
        "dos itens P7, P8, P9 e P10 com base nos testes existentes por provider."
    )
    lines.append("")
    lines.append("## Como usar")
    lines.append("")
    lines.append("1. Escolha um pilar (P7–P10).")
    lines.append("2. Implemente provider por provider, priorizando os arquivos mapeados abaixo.")
    lines.append("3. Rode os testes por provider e atualize o status.")
    lines.append("")

    for pillar, tokens in PILLARS.items():
        lines.append(f"## {pillar}")
        lines.append("")
        lines.append("| Provider | Arquivos de teste-alvo | Status |")
        lines.append("| --- | --- | --- |")
        for provider_name, provider_folder in PROVIDERS:
            paths = find_test_paths(provider_folder, tokens)
            if paths:
                joined = "<br>".join(f"`{p}`" for p in paths)
            else:
                joined = "_Sem arquivo específico; criar suíte dedicada._"
            lines.append(f"| {provider_name} | {joined} | ⬜ Pending |")
        lines.append("")

    lines.append("## Checklist de saída por PR")
    lines.append("")
    lines.append("- [ ] Parser e Dialect atualizados para o pilar.")
    lines.append("- [ ] Executor atualizado para os casos do pilar.")
    lines.append("- [ ] Testes do provider alterado verdes.")
    lines.append("- [ ] Smoke tests dos demais providers sem regressão.")
    lines.append("- [ ] Documentação de compatibilidade atualizada.")
    lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    DOC.write_text(build(), encoding="utf-8")
    print(f"Updated {DOC.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
