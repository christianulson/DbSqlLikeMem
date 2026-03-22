---
name: execute-plan
description: Carry out a plan or roadmap in this repository when the user asks for step-by-step execution, backlog progress, phased implementation, or controlled refactoring with explicit stopping points.
---

# Execute Plan

## Overview
Carry out a plan step by step, keeping track of where the work stopped and what comes next.

## Workflow
1. Read the plan and identify the next blocking slice.
2. Make one small useful change at a time.
3. Record the current stopping point when the slice is complete.
4. Keep the implementation aligned with the plan, the repo rules, and the requested order.
5. Resume from the last recorded point on the next step.

## Guardrails
- Prefer the smallest change that advances the current slice.
- Do not jump ahead to later phases unless the current slice is blocked.
- Do not lose track of the stopping point.
- Keep behavior, coverage, and documentation aligned.
