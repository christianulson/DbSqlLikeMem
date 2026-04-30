# Fase 5 - Funções e semântica

## Status

IN PROGRESS

## Percentual de entrega

52%

## O que foi feito

- Adicionado o primeiro wrapper de `JsonTableFunctionTestsBase` na suite de fidelidade e iniciado o coverage de `json_each` e `json_tree`.
- Adicionado o wrapper de fidelidade para `JSON insert/cast`, cobrindo o benchmark escalar de leitura JSON com coerção.
- Ligados os handlers de `json_each` e `json_tree` ao executor de table functions do mock.
- Mantida a validacao negativa quando o provider nao suporta funcoes JSON tabulares.
- Expandido o parser e o avaliador para `CAST`, `CONVERT`, `TRY_CAST`, `TRY_CONVERT`, `PARSE` e `TRY_PARSE`, com restricao por capability no dialect.
- Adicionados caminhos de sintaxe e avaliacao para funcoes Firebird como `DATEADD`, `SUBSTRING`, `HASH` e `CRYPT_HASH` quando o dialect suporta a chamada.
- Adicionado suporte a `DATETRUNC` no SQL Server 2022+, com registro de capability, parser e avaliacao temporal compartilhada.
- Adicionado o contrato de capability para `DATETRUNC` e `FROM PARTS` no SQL Server, com cobertura de parser para o limite de versao e o grupo de construtores nativos.
- Adicionado suporte a `iso_week` em `DATEPART`, `DATENAME` e `DATETRUNC` no SQL Server, com rejeicao explicita em `DATEADD` e `DATEDIFF` quando a unidade nao faz parte do contrato real.
- Expandido o suporte de `iso_week` para os aliases `isowk` e `isoww` em `DATEPART`, `DATENAME` e `DATETRUNC`, com a mesma rejeicao em `DATEADD` e `DATEDIFF`.
- Adicionado o contrato de `JSON_QUERY` sem path no SQL Server, preservando o fragmento JSON bruto da raiz e a insercao dentro de `FOR JSON PATH`.
- Validado `STRING_AGG` com `WITHIN GROUP (ORDER BY ...)` no SQL Server para manter a ordem explicita de concatenacao.
- Adicionado tratamento de `strict` nos caminhos JSON do SQL Server, com erro quando o caminho nao existe ou nao resolve para o tipo esperado.
- Adicionado tratamento de `strict` em `JSON_MODIFY` no SQL Server, com erro quando o caminho nao existe no documento JSON.
- Adicionado `append` em `JSON_MODIFY` no SQL Server, com append de valor em array JSON existente.
- Adicionado o limite de 4000 caracteres em `JSON_VALUE` do SQL Server, com retorno nulo no modo lax e erro no modo strict.
- Validado o limite exato de 4000 caracteres em `JSON_VALUE` do SQL Server, com retorno do valor no limite e rejeicao acima dele.
- Adicionado tratamento runtime para `NTILE` no SQL Server quando a expressao de buckets avalia para valor nao positivo.
- Adicionado tratamento runtime para `LAG` no SQL Server quando o offset avaliado em runtime fica abaixo de zero.
- Adicionado tratamento runtime para `LEAD` no SQL Server quando o offset avaliado em runtime fica abaixo de zero.
- Adicionado tratamento runtime para `NTH_VALUE` no SQL Server quando o ordinal avaliado em runtime fica abaixo de um.
- Corrigido o frame padrao de janela ordenada no SQL Server para tratar `LAST_VALUE` e `NTH_VALUE` como acumulados por linha.
- Corrigido o frame padrao de janela ordenada no SQL Server para tratar agregados em janela como acumulados por linha.
- Reforcada a cobertura de agregados em janela ordenada no SQL Server com peers duplicados em `ORDER BY`, preservando o frame padrao acumulado por linha.
- Reforcada a diferenca entre `ROWS` e `RANGE` em janela ordenada no SQL Server com valores repetidos em `ORDER BY`.
- Reforcada a cobertura de agregados em janela ordenada no SQL Server com `AVG` e `MAX` acumulados por linha.
- Expandido o suporte temporal compartilhado para `millisecond` em `DATEADD`, `DATEDIFF`, `DATEPART`, `DATENAME` e `DATETRUNC`.
- Expandido o suporte temporal compartilhado para `microsecond` em `DATEADD`, `DATEDIFF`, `DATEPART`, `DATENAME` e `DATETRUNC`.
- Expandido o suporte SQL Server para `dayofyear`, `week` e `weekday` em `DATEADD`, `DATEPART` e `DATENAME`, com `dayofyear` e `week` tambem em `DATEDIFF` e `DATETRUNC`, mantendo `@@DATEFIRST` no valor padrao do provider.
- Expandido o suporte temporal compartilhado para `nanosecond` nas funcoes temporais do SQL Server, com `DATETRUNC` rejeitando a unidade conforme o banco real.
- Reforcados os aliases temporais do SQL Server para `dy`, `y`, `wk`, `ww`, `dw` e `w` em `DATEPART`, `DATENAME`, `DATEADD` e `DATEDIFF`.
- Reforcados os aliases temporais do SQL Server para `dy`, `y`, `wk` e `ww` em `DATETRUNC`.
- Reforcados os aliases temporais do SQL Server para `dw`, `w`, `dy`, `y` e `ww` com cobertura adicional em `DATEPART`, `DATENAME`, `DATEADD` e `DATEDIFF`.
- Adicionado suporte a `tzoffset` e `tz` em `DATEPART` e `DATENAME` no SQL Server, com leitura do offset em minutos a partir de `DateTime`, `DateTimeOffset`, literais com offset e `TODATETIMEOFFSET`.
- Adicionado suporte de parser para `DATEPART(tz, ...)` e `DATENAME(tz, ...)` no SQL Server, com cobertura de sintaxe para literal UTC e offset nativo.
- Expandido o suporte temporal do SQL Server para o alias `tzoffset`, mantendo o mesmo contrato de leitura de offset em minutos em `DATEPART` e `DATENAME`.
- Expandido o suporte equivalente de `tz` e `tzoffset` no SQL Azure, com cobertura de execucao e parser alinhada ao SQL Server.
- Expandido o suporte do SQL Azure para `tzoffset` com literal UTC e `TODATETIMEOFFSET`, mantendo o mesmo contrato de offset em minutos.
- Adicionado cobertura de offset negativo em `TODATETIMEOFFSET` para `tzoffset` no SQL Server e no SQL Azure.

## Próximos passos

- Separar funções por categoria.
- Cobrir tipos nativos de retorno e de parâmetro.
- Alinhar temporais, JSON e window functions com o banco real.
