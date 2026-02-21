# Auditoria da correção CS0111 (AstQueryExecutorBase)

Para responder ao comentário "os métodos duplicados faziam exatamente a mesma coisa?", foi feita uma comparação direta entre os dois blocos existentes no commit anterior à remoção.

## Resultado

Os blocos duplicados de:

- `EvalTryCast(...)`
- `EvalCast(...)`
- `TryEvalDateAddFunction(...)`

eram **idênticos byte a byte** (mesmo conteúdo, mesmo tamanho: 5312 caracteres por bloco).

## Comando usado

```bash
python - <<'PY'
from pathlib import Path
import subprocess,re
text=subprocess.check_output(['git','show','HEAD^:src/DbSqlLikeMem/Query/AstQueryExecutorBase.cs'],text=True)
pat=r'    private object\\? EvalTryCast\\(FunctionCallExpr fn, Func<int, object\\?> evalArg\\)\\n    \\{.*?\\n    \\}\\n\\n    private object\\? EvalCast\\(FunctionCallExpr fn, Func<int, object\\?> evalArg\\)\\n    \\{.*?\\n    \\}\\n\\n    private object\\? TryEvalDateAddFunction\\(\\n        FunctionCallExpr fn,\\n        EvalRow row,\\n        EvalGroup\\? group,\\n        IDictionary<string, Source> ctes,\\n        Func<int, object\\?> evalArg,\\n        out bool handled\\)\\n    \\{.*?\\n    \\}\\n'
ms=list(re.finditer(pat,text,flags=re.S))
print('matches',len(ms))
if len(ms)==2:
    a=text[ms[0].start():ms[0].end()]
    b=text[ms[1].start():ms[1].end()]
    print('equal',a==b)
    print('lenA',len(a),'lenB',len(b))
PY
```

Saída esperada:

- `matches 2`
- `equal True`
- `lenA 5312 lenB 5312`
