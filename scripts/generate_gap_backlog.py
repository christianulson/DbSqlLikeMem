#!/usr/bin/env python3
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_PATTERN = "src/*/*GapTests.cs"
OUTPUT_PATH = REPO_ROOT / "docs" / "gap-tests-technical-backlog.md"


@dataclass(frozen=True)
class ItemMeta:
    title: str
    epic: str
    effort: str
    risk: str
    dependencies: str


PROVIDER_LABELS = {
    "Db2": "DB2",
    "MySql": "MySQL",
    "Oracle": "Oracle",
    "PostgreSql": "PostgreSQL",
    "Sqlite": "SQLite",
    "SqlServer": "SQL Server",
}

EPIC_PRIORITY = [
    "Parser",
    "Executor",
    "Funções SQL",
    "Tipagem/Collation",
]


def to_title(test_name: str) -> str:
    base = test_name.removesuffix("_ShouldWork").replace("_", " ")
    return base


def infer_meta(test_name: str) -> ItemMeta:
    lowered = test_name.lower()

    if any(k in lowered for k in ["where_", "cte_", "union_", "regexp", "cast_"]):
        epic = "Parser"
    elif any(k in lowered for k in ["join_", "groupby", "orderby", "window_", "correlatedsubquery", "distinct"]):
        epic = "Executor"
    elif any(k in lowered for k in ["functions_", "dateadd", "if_", "iif", "concat", "coalesce", "ifnull", "field_function", "case_when"]):
        epic = "Funções SQL"
    elif any(k in lowered for k in ["typing_", "collation", "implicitcasts"]):
        epic = "Tipagem/Collation"
    else:
        epic = "Executor"

    if any(k in lowered for k in ["window_", "correlatedsubquery", "cte_", "union_inside", "join_complexon", "typing_"]):
        effort = "G"
    elif any(k in lowered for k in ["groupby", "orderby", "dateadd", "regexp", "cast_", "parentheses", "precedence", "case_when", "union_"]):
        effort = "M"
    else:
        effort = "P"

    if any(k in lowered for k in ["typing_", "collation", "cast_", "window_", "correlatedsubquery", "join_complexon"]):
        risk = "Alto"
    elif any(k in lowered for k in ["groupby", "orderby", "union", "cte", "case_when", "regexp", "dateadd"]):
        risk = "Médio"
    else:
        risk = "Baixo"

    deps: List[str] = []
    if epic == "Parser":
        deps.append("AST + precedência de operadores")
    if epic == "Executor":
        deps.append("Planejador de execução em memória")
    if epic == "Funções SQL":
        deps.append("Registro/catálogo de funções por provider")
    if epic == "Tipagem/Collation":
        deps.append("Matriz de coerção de tipos")

    if "window_" in lowered:
        deps.append("Engine de funções de janela")
    if "correlatedsubquery" in lowered:
        deps.append("Escopo de aliases em subquery correlata")
    if "groupby" in lowered or "having" in lowered:
        deps.append("Pipeline de agregação + HAVING")
    if "cte_" in lowered:
        deps.append("Suporte a CTE no parser + binding")
    if "union" in lowered:
        deps.append("Normalização de schemas em set operators")
    if "typing_" in lowered or "collation" in lowered:
        deps.append("Comparador com collation configurável")

    return ItemMeta(
        title=to_title(test_name),
        epic=epic,
        effort=effort,
        risk=risk,
        dependencies="; ".join(dict.fromkeys(deps)),
    )


def score(meta: ItemMeta, providers_count: int) -> int:
    risk_score = {"Alto": 3, "Médio": 2, "Baixo": 1}[meta.risk]
    effort_penalty = {"P": 0, "M": 1, "G": 2}[meta.effort]
    return (providers_count * 2) + risk_score - effort_penalty


def collect_tests() -> Dict[str, set[str]]:
    grouped: Dict[str, set[str]] = defaultdict(set)
    for path in sorted(REPO_ROOT.glob(TEST_PATTERN)):
        provider_key = path.stem.replace("AdvancedSqlGapTests", "").replace("SqlCompatibilityGapTests", "")
        provider = PROVIDER_LABELS.get(provider_key, provider_key)
        content = path.read_text(encoding="utf-8")
        tests = re.findall(r"public void (\w+)\(", content)
        for test in tests:
            grouped[test].add(provider)
    return grouped


def render_markdown(grouped: Dict[str, set[str]]) -> str:
    items = []
    for test_name, providers in grouped.items():
        meta = infer_meta(test_name)
        items.append((test_name, meta, sorted(providers), score(meta, len(providers))))

    by_epic: Dict[str, List[tuple]] = defaultdict(list)
    for row in items:
        by_epic[row[1].epic].append(row)

    lines: List[str] = []
    lines.append("# Backlog técnico automatizado a partir de *GapTests")
    lines.append("")
    lines.append("Fonte automática: varredura dos arquivos `*SqlCompatibilityGapTests.cs` e `*AdvancedSqlGapTests.cs` por provider.")
    lines.append("")
    lines.append("## Priorização")
    lines.append("")
    lines.append("Prioridade calculada por cobertura de providers + risco de regressão - esforço estimado.")
    lines.append("")

    for epic in EPIC_PRIORITY:
        rows = sorted(by_epic.get(epic, []), key=lambda x: (-x[3], x[1].title))
        if not rows:
            continue
        lines.append(f"## Épico: {epic}")
        lines.append("")
        lines.append("| Prioridade | Título | Provider(s) | Esforço | Risco de regressão | Dependências técnicas |")
        lines.append("|---|---|---|---|---|---|")
        for idx, (_raw, meta, providers, _s) in enumerate(rows, start=1):
            p = "P0" if idx <= 3 else ("P1" if idx <= 6 else "P2")
            provider_list = ", ".join(providers)
            lines.append(f"| {p} | {meta.title} | {provider_list} | {meta.effort} | {meta.risk} | {meta.dependencies} |")
        lines.append("")

    lines.append("## Formato alternativo (checklist para GitHub Projects/Jira)")
    lines.append("")
    for epic in EPIC_PRIORITY:
        rows = sorted(by_epic.get(epic, []), key=lambda x: (-x[3], x[1].title))
        if not rows:
            continue
        lines.append(f"### {epic}")
        for _raw, meta, providers, _s in rows:
            lines.append(
                f"- [ ] **{meta.title}**  "+
                f"`providers: {', '.join(providers)}` · `esforço: {meta.effort}` · "
                f"`risco: {meta.risk}`  "+
                f"Dependências: {meta.dependencies}"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    grouped = collect_tests()
    markdown = render_markdown(grouped)
    OUTPUT_PATH.write_text(markdown, encoding="utf-8")
    print(f"Backlog gerado em: {OUTPUT_PATH.relative_to(REPO_ROOT)}")
    print(f"Total de itens únicos: {len(grouped)}")


if __name__ == "__main__":
    main()
