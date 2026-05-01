namespace DbSqlLikeMem;

/// <summary>
/// EN: Holds the SQL keywords and helper tokens shared by the parser and execution pipeline.
/// PT-br: Contém as palavras-chave SQL e os tokens auxiliares compartilhados pelo parser e pelo pipeline de execução.
/// </summary>
public static class SqlConst
{
    #region DML and DDL keywords
    /// <summary>
    /// EN: SQL keyword used to start a SELECT statement.
    /// PT-br: Palavra-chave SQL usada para iniciar uma instrução SELECT.
    /// </summary>
    public const string SELECT = "SELECT";
    /// <summary>
    /// EN: SQL keyword used to introduce a common table expression.
    /// PT-br: Palavra-chave SQL usada para introduzir uma expressao de tabela comum.
    /// </summary>
    public const string WITH = "WITH";
    /// <summary>
    /// EN: SQL keyword used to insert rows into a table.
    /// PT-br: Palavra-chave SQL usada para inserir linhas em uma tabela.
    /// </summary>
    public const string INSERT = "INSERT";
    /// <summary>
    /// EN: SQL keyword used to replace rows in dialects that support it.
    /// PT-br: Palavra-chave SQL usada para substituir linhas nos dialetos que a suportam.
    /// </summary>
    public const string REPLACE = "REPLACE";
    /// <summary>
    /// EN: SQL keyword used to update existing rows.
    /// PT-br: Palavra-chave SQL usada para atualizar linhas existentes.
    /// </summary>
    public const string UPDATE = "UPDATE";
    /// <summary>
    /// EN: SQL keyword used to delete rows.
    /// PT-br: Palavra-chave SQL usada para remover linhas.
    /// </summary>
    public const string DELETE = "DELETE";
    /// <summary>
    /// EN: SQL keyword used to merge source and target rows.
    /// PT-br: Palavra-chave SQL usada para mesclar linhas de origem e destino.
    /// </summary>
    public const string MERGE = "MERGE";
    /// <summary>
    /// EN: SQL keyword used to create database objects.
    /// PT-br: Palavra-chave SQL usada para criar objetos de banco de dados.
    /// </summary>
    public const string CREATE = "CREATE";
    /// <summary>
    /// EN: SQL keyword used to refer to a schema object.
    /// PT-br: Palavra-chave SQL usada para se referir a um objeto de schema.
    /// </summary>
    public const string SCHEMA = "SCHEMA";
    /// <summary>
    /// EN: SQL keyword used to recreate a database object.
    /// PT-br: Palavra-chave SQL usada para recriar um objeto de banco de dados.
    /// </summary>
    public const string RECREATE = "RECREATE";
    /// <summary>
    /// EN: SQL keyword used to alter an existing database object.
    /// PT-br: Palavra-chave SQL usada para alterar um objeto de banco de dados existente.
    /// </summary>
    public const string ALTER = "ALTER";
    /// <summary>
    /// EN: SQL keyword used to drop a database object.
    /// PT-br: Palavra-chave SQL usada para remover um objeto de banco de dados.
    /// </summary>
    public const string DROP = "DROP";
    /// <summary>
    /// EN: SQL keyword used to restart a sequence or generator.
    /// PT-br: Palavra-chave SQL usada para reiniciar uma sequence ou generator.
    /// </summary>
    public const string RESTART = "RESTART";
    /// <summary>
    /// EN: SQL keyword used to refer to an owned sequence or generator.
    /// PT-br: Palavra-chave SQL usada para se referir a uma sequence ou generator de propriedade.
    /// </summary>
    public const string OWNED = "OWNED";
    /// <summary>
    /// EN: SQL keyword used to declare a generator in Firebird-style syntax.
    /// PT-br: Palavra-chave SQL usada para declarar um generator em sintaxe estilo Firebird.
    /// </summary>
    public const string GENERATOR = "GENERATOR";
    /// <summary>
    /// EN: SQL keyword used to execute a block or statement.
    /// PT-br: Palavra-chave SQL usada para executar um bloco ou instrução.
    /// </summary>
    public const string EXECUTE = "EXECUTE";
    /// <summary>
    /// EN: SQL keyword used to mark an executable block.
    /// PT-br: Palavra-chave SQL usada para marcar um bloco executavel.
    /// </summary>
    public const string BLOCK = "BLOCK";
    /// <summary>
    /// EN: SQL keyword used to refer to a statement body.
    /// PT-br: Palavra-chave SQL usada para se referir ao corpo de uma instrução.
    /// </summary>
    public const string STATEMENT = "STATEMENT";
    /// <summary>
    /// EN: SQL keyword used to suspend execution in procedural blocks.
    /// PT-br: Palavra-chave SQL usada para suspender a execucao em blocos procedurais.
    /// </summary>
    public const string SUSPEND = "SUSPEND";
    /// <summary>
    /// EN: SQL keyword used to exit a block or loop.
    /// PT-br: Palavra-chave SQL usada para sair de um bloco ou loop.
    /// </summary>
    public const string EXIT = "EXIT";
    /// <summary>
    /// EN: SQL keyword used to break a loop.
    /// PT-br: Palavra-chave SQL usada para interromper um loop.
    /// </summary>
    public const string BREAK = "BREAK";
    /// <summary>
    /// EN: SQL keyword used to leave a loop or block.
    /// PT-br: Palavra-chave SQL usada para sair de um loop ou bloco.
    /// </summary>
    public const string LEAVE = "LEAVE";
    /// <summary>
    /// EN: SQL keyword used to target the destination of an INSERT or SELECT INTO statement.
    /// PT-br: Palavra-chave SQL usada para indicar o destino de uma instrução INSERT ou SELECT INTO.
    /// </summary>
    public const string INTO = "INTO";
    /// <summary>
    /// EN: SQL keyword used to refer to a single value in Firebird-style syntax.
    /// PT-br: Palavra-chave SQL usada para se referir a um unico valor em sintaxe estilo Firebird.
    /// </summary>
    public const string VALUE = "VALUE";
    /// <summary>
    /// EN: SQL keyword used to refer to multiple values in an INSERT statement.
    /// PT-br: Palavra-chave SQL usada para se referir a varios valores em uma instrução INSERT.
    /// </summary>
    public const string VALUES = "VALUES";
    /// <summary>
    /// EN: SQL keyword used to address a partition in partition-aware statements.
    /// PT-br: Palavra-chave SQL usada para identificar uma particao em instrucoes com particionamento.
    /// </summary>
    public const string PARTITION = "PARTITION";
    /// <summary>
    /// EN: SQL keyword used for duplicate-key handling in insert conflicts.
    /// PT-br: Palavra-chave SQL usada para tratamento de chave duplicada em conflitos de insert.
    /// </summary>
    public const string DUPLICATE = "DUPLICATE";
    /// <summary>
    /// EN: SQL keyword used to introduce a filter predicate.
    /// PT-br: Palavra-chave SQL usada para introduzir um predicado de filtro.
    /// </summary>
    public const string WHERE = "WHERE";
    /// <summary>
    /// EN: SQL keyword used to reference a source table or query.
    /// PT-br: Palavra-chave SQL usada para referenciar uma tabela ou consulta de origem.
    /// </summary>
    public const string FROM = "FROM";
    /// <summary>
    /// EN: SQL keyword used to refer to the source of an update or delete.
    /// PT-br: Palavra-chave SQL usada para referenciar a origem de um update ou delete.
    /// </summary>
    public const string USING = "USING";
    /// <summary>
    /// EN: SQL keyword used to join result sets.
    /// PT-br: Palavra-chave SQL usada para unir conjuntos de resultados.
    /// </summary>
    public const string JOIN = "JOIN";
    /// <summary>
    /// EN: SQL keyword used to request an inner join.
    /// PT-br: Palavra-chave SQL usada para solicitar um inner join.
    /// </summary>
    public const string INNER = "INNER";
    /// <summary>
    /// EN: SQL keyword used to request a left join.
    /// PT-br: Palavra-chave SQL usada para solicitar um left join.
    /// </summary>
    public const string LEFT = "LEFT";
    /// <summary>
    /// EN: SQL keyword used to request a right join.
    /// PT-br: Palavra-chave SQL usada para solicitar um right join.
    /// </summary>
    public const string RIGHT = "RIGHT";
    /// <summary>
    /// EN: SQL keyword used to request a full join.
    /// PT-br: Palavra-chave SQL usada para solicitar um full join.
    /// </summary>
    public const string FULL = "FULL";
    /// <summary>
    /// EN: SQL keyword used to request a cross join.
    /// PT-br: Palavra-chave SQL usada para solicitar um cross join.
    /// </summary>
    public const string CROSS = "CROSS";
    /// <summary>
    /// EN: SQL keyword used to request an outer join.
    /// PT-br: Palavra-chave SQL usada para solicitar um outer join.
    /// </summary>
    public const string OUTER = "OUTER";
    /// <summary>
    /// EN: SQL keyword used to request an APPLY join.
    /// PT-br: Palavra-chave SQL usada para solicitar um join APPLY.
    /// </summary>
    public const string APPLY = "APPLY";
    /// <summary>
    /// EN: SQL keyword used to introduce a join predicate.
    /// PT-br: Palavra-chave SQL usada para introduzir um predicado de join.
    /// </summary>
    public const string ON = "ON";
    /// <summary>
    /// EN: SQL keyword used to start a grouping clause.
    /// PT-br: Palavra-chave SQL usada para iniciar uma clausula de agrupamento.
    /// </summary>
    public const string GROUP = "GROUP";
    /// <summary>
    /// EN: SQL keyword used to combine grouped items.
    /// PT-br: Palavra-chave SQL usada para combinar itens agrupados.
    /// </summary>
    public const string BY = "BY";
    /// <summary>
    /// EN: SQL keyword used to start an ordering clause.
    /// PT-br: Palavra-chave SQL usada para iniciar uma clausula de ordenacao.
    /// </summary>
    public const string ORDER = "ORDER";
    /// <summary>
    /// EN: SQL keyword used to introduce a HAVING predicate.
    /// PT-br: Palavra-chave SQL usada para introduzir um predicado HAVING.
    /// </summary>
    public const string HAVING = "HAVING";
    /// <summary>
    /// EN: SQL keyword used to reference a view object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto de view.
    /// </summary>
    public const string VIEW = "VIEW";
    /// <summary>
    /// EN: SQL keyword used to mark a uniqueness constraint.
    /// PT-br: Palavra-chave SQL usada para marcar uma restricao de unicidade.
    /// </summary>
    public const string UNIQUE = "UNIQUE";
    /// <summary>
    /// EN: SQL keyword used to reference an index object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto de indice.
    /// </summary>
    public const string INDEX = "INDEX";
    /// <summary>
    /// EN: SQL keyword used to reference a stored function.
    /// PT-br: Palavra-chave SQL usada para referenciar uma funcao armazenada.
    /// </summary>
    public const string FUNCTION = "FUNCTION";
    /// <summary>
    /// EN: SQL keyword used to reference a stored procedure.
    /// PT-br: Palavra-chave SQL usada para referenciar uma procedure armazenada.
    /// </summary>
    public const string PROCEDURE = "PROCEDURE";
    /// <summary>
    /// EN: SQL keyword used to reference a trigger object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto trigger.
    /// </summary>
    public const string TRIGGER = "TRIGGER";
    /// <summary>
    /// EN: SQL keyword used to mark a BEFORE trigger.
    /// PT-br: Palavra-chave SQL usada para marcar um trigger BEFORE.
    /// </summary>
    public const string BEFORE = "BEFORE";
    /// <summary>
    /// EN: SQL keyword used to mark an AFTER trigger.
    /// PT-br: Palavra-chave SQL usada para marcar um trigger AFTER.
    /// </summary>
    public const string AFTER = "AFTER";
    /// <summary>
    /// EN: SQL keyword used to mark a temporary object.
    /// PT-br: Palavra-chave SQL usada para marcar um objeto temporario.
    /// </summary>
    public const string TEMPORARY = "TEMPORARY";
    /// <summary>
    /// EN: SQL keyword used to mark a global temporary object.
    /// PT-br: Palavra-chave SQL usada para marcar um objeto temporario global.
    /// </summary>
    public const string GLOBAL = "GLOBAL";
    /// <summary>
    /// EN: SQL keyword used as a shorthand for temporary.
    /// PT-br: Palavra-chave SQL usada como abreviacao de temporary.
    /// </summary>
    public const string TEMP = "TEMP";
    /// <summary>
    /// EN: SQL keyword used to reference a table object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto de tabela.
    /// </summary>
    public const string TABLE = "TABLE";
    /// <summary>
    /// EN: SQL keyword used for conditional creation or alteration.
    /// PT-br: Palavra-chave SQL usada para criacao ou alteracao condicional.
    /// </summary>
    public const string IF = "IF";
    /// <summary>
    /// EN: SQL keyword used to start a sequence or generator definition.
    /// PT-br: Palavra-chave SQL usada para iniciar a definicao de uma sequence ou generator.
    /// </summary>
    public const string START = "START";
    /// <summary>
    /// EN: SQL keyword used to mark an autonomous transaction.
    /// PT-br: Palavra-chave SQL usada para marcar uma transacao autonoma.
    /// </summary>
    public const string AUTONOMOUS = "AUTONOMOUS";
    /// <summary>
    /// EN: SQL keyword used to mark a common object.
    /// PT-br: Palavra-chave SQL usada para marcar um objeto comum.
    /// </summary>
    public const string COMMON = "COMMON";
    /// <summary>
    /// EN: SQL keyword used to mark caller privileges or context.
    /// PT-br: Palavra-chave SQL usada para marcar privilegios ou contexto do chamador.
    /// </summary>
    public const string CALLER = "CALLER";
    /// <summary>
    /// EN: SQL keyword used to request elevated privileges in a routine body.
    /// PT-br: Palavra-chave SQL usada para solicitar privilegios elevados em um corpo de rotina.
    /// </summary>
    public const string PRIVILEGES = "PRIVILEGES";
    /// <summary>
    /// EN: SQL keyword used to reference a transaction object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto de transacao.
    /// </summary>
    public const string TRANSACTION = "TRANSACTION";
    /// <summary>
    /// EN: SQL keyword used to define the increment of a sequence.
    /// PT-br: Palavra-chave SQL usada para definir o incremento de uma sequence.
    /// </summary>
    public const string INCREMENT = "INCREMENT";
    /// <summary>
    /// EN: SQL keyword used to declare an output parameter.
    /// PT-br: Palavra-chave SQL usada para declarar um parametro de saida.
    /// </summary>
    public const string OUT = "OUT";
    /// <summary>
    /// EN: SQL keyword used to declare an input/output parameter.
    /// PT-br: Palavra-chave SQL usada para declarar um parametro de entrada e saida.
    /// </summary>
    public const string INOUT = "INOUT";
    /// <summary>
    /// EN: SQL keyword used to declare the return type of a routine.
    /// PT-br: Palavra-chave SQL usada para declarar o tipo de retorno de uma rotina.
    /// </summary>
    public const string RETURNS = "RETURNS";
    /// <summary>
    /// EN: SQL keyword used to return a scalar value.
    /// PT-br: Palavra-chave SQL usada para retornar um valor escalar.
    /// </summary>
    public const string RETURN = "RETURN";
    /// <summary>
    /// EN: SQL keyword used to start a routine body.
    /// PT-br: Palavra-chave SQL usada para iniciar o corpo de uma rotina.
    /// </summary>
    public const string BEGIN = "BEGIN";
    /// <summary>
    /// EN: SQL keyword used to mark each row in a trigger body.
    /// PT-br: Palavra-chave SQL usada para marcar cada linha no corpo de um trigger.
    /// </summary>
    public const string EACH = "EACH";
    /// <summary>
    /// EN: SQL keyword used for IS predicates.
    /// PT-br: Palavra-chave SQL usada para predicados IS.
    /// </summary>
    public const string IS = "IS";
    /// <summary>
    /// EN: SQL keyword used to introduce referenced rows in trigger syntax.
    /// PT-br: Palavra-chave SQL usada para introduzir linhas referenciadas em sintaxe de trigger.
    /// </summary>
    public const string REFERENCING = "REFERENCING";
    /// <summary>
    /// EN: SQL keyword used to identify a trigger mode.
    /// PT-br: Palavra-chave SQL usada para identificar um modo de trigger.
    /// </summary>
    public const string MODE = "MODE";
    /// <summary>
    /// EN: SQL keyword used to mark an atomic block.
    /// PT-br: Palavra-chave SQL usada para marcar um bloco atomico.
    /// </summary>
    public const string ATOMIC = "ATOMIC";
    /// <summary>
    /// EN: SQL keyword used to declare a language for a routine.
    /// PT-br: Palavra-chave SQL usada para declarar uma linguagem de rotina.
    /// </summary>
    public const string LANGUAGE = "LANGUAGE";
    /// <summary>
    /// EN: SQL keyword used to reference a column object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto de coluna.
    /// </summary>
    public const string COLUMN = "COLUMN";
    /// <summary>
    /// EN: SQL keyword used to add a table element.
    /// PT-br: Palavra-chave SQL usada para adicionar um elemento de tabela.
    /// </summary>
    public const string ADD = "ADD";
    /// <summary>
    /// EN: SQL keyword used to request a count aggregate.
    /// PT-br: Palavra-chave SQL usada para solicitar um agregado de contagem.
    /// </summary>
    public const string COUNT = "COUNT";
    /// <summary>
    /// EN: SQL keyword used to request a bigint count aggregate.
    /// PT-br: Palavra-chave SQL usada para solicitar um agregado de contagem bigint.
    /// </summary>
    public const string COUNT_BIG = "COUNT_BIG";
    /// <summary>
    /// EN: SQL keyword used for JSON object aggregation in some dialects.
    /// PT-br: Palavra-chave SQL usada para agregacao de objetos JSON em alguns dialetos.
    /// </summary>
    public const string JSON_GROUP_OBJECT = "JSON_GROUP_OBJECT";
    /// <summary>
    /// EN: SQL keyword used for JSON object aggregation in MySQL-compatible dialects.
    /// PT-br: Palavra-chave SQL usada para agregacao de objetos JSON em dialetos compativeis com MySQL.
    /// </summary>
    public const string JSON_OBJECTAGG = "JSON_OBJECTAGG";
    /// <summary>
    /// EN: SQL keyword used for JSON object aggregation with underscore naming.
    /// PT-br: Palavra-chave SQL usada para agregacao de objetos JSON com nomenclatura com underscore.
    /// </summary>
    public const string JSON_OBJECT_AGG = "JSON_OBJECT_AGG";
    /// <summary>
    /// EN: SQL keyword used for strict JSON object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao estrita de objetos JSON.
    /// </summary>
    public const string JSON_OBJECT_AGG_STRICT = "JSON_OBJECT_AGG_STRICT";
    /// <summary>
    /// EN: SQL keyword used for unique JSON object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao unica de objetos JSON.
    /// </summary>
    public const string JSON_OBJECT_AGG_UNIQUE = "JSON_OBJECT_AGG_UNIQUE";
    /// <summary>
    /// EN: SQL keyword used for strict unique JSON object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao unica e estrita de objetos JSON.
    /// </summary>
    public const string JSON_OBJECT_AGG_UNIQUE_STRICT = "JSON_OBJECT_AGG_UNIQUE_STRICT";
    /// <summary>
    /// EN: SQL keyword used for JSONB object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de objetos JSONB.
    /// </summary>
    public const string JSONB_OBJECT_AGG = "JSONB_OBJECT_AGG";
    /// <summary>
    /// EN: SQL keyword used for strict JSONB object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao estrita de objetos JSONB.
    /// </summary>
    public const string JSONB_OBJECT_AGG_STRICT = "JSONB_OBJECT_AGG_STRICT";
    /// <summary>
    /// EN: SQL keyword used for unique JSONB object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao unica de objetos JSONB.
    /// </summary>
    public const string JSONB_OBJECT_AGG_UNIQUE = "JSONB_OBJECT_AGG_UNIQUE";
    /// <summary>
    /// EN: SQL keyword used for strict unique JSONB object aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao unica e estrita de objetos JSONB.
    /// </summary>
    public const string JSONB_OBJECT_AGG_UNIQUE_STRICT = "JSONB_OBJECT_AGG_UNIQUE_STRICT";

    /// <summary>
    /// EN: SQL keyword used for string concatenation aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de concatenacao de strings.
    /// </summary>
    public const string GROUP_CONCAT = "GROUP_CONCAT";
    /// <summary>
    /// EN: SQL keyword used for string aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de strings.
    /// </summary>
    public const string STRING_AGG = "STRING_AGG";
    /// <summary>
    /// EN: SQL keyword used for ordered list aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de listas ordenadas.
    /// </summary>
    public const string LISTAGG = "LISTAGG";
    /// <summary>
    /// EN: SQL keyword used for list aggregation in some dialects.
    /// PT-br: Palavra-chave SQL usada para agregacao de lista em alguns dialetos.
    /// </summary>
    public const string LIST = "LIST";
    /// <summary>
    /// EN: SQL keyword used for sum aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de soma.
    /// </summary>
    public const string SUM = "SUM";
    /// <summary>
    /// EN: SQL keyword used for average aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de media.
    /// </summary>
    public const string AVG = "AVG";
    /// <summary>
    /// EN: SQL keyword used for minimum aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de minimo.
    /// </summary>
    public const string MIN = "MIN";
    /// <summary>
    /// EN: SQL keyword used for maximum aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de maximo.
    /// </summary>
    public const string MAX = "MAX";
    /// <summary>
    /// EN: SQL keyword used for checksum aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de checksum.
    /// </summary>
    public const string CHECKSUM_AGG = "CHECKSUM_AGG";
    /// <summary>
    /// EN: SQL keyword used for any-value aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de qualquer valor.
    /// </summary>
    public const string ANY_VALUE = "ANY_VALUE";
    /// <summary>
    /// EN: SQL keyword used for bitwise AND aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao bitwise AND.
    /// </summary>
    public const string BIT_AND = "BIT_AND";
    /// <summary>
    /// EN: SQL keyword used for bitwise OR aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao bitwise OR.
    /// </summary>
    public const string BIT_OR = "BIT_OR";
    /// <summary>
    /// EN: SQL keyword used for bitwise XOR aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao bitwise XOR.
    /// </summary>
    public const string BIT_XOR = "BIT_XOR";
    /// <summary>
    /// EN: SQL keyword used for JSON array aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de array JSON.
    /// </summary>
    public const string JSON_ARRAYAGG = "JSON_ARRAYAGG";
    /// <summary>
    /// EN: SQL keyword used for JSON aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao JSON.
    /// </summary>
    public const string JSON_AGG = "JSON_AGG";
    /// <summary>
    /// EN: SQL keyword used for JSONB aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao JSONB.
    /// </summary>
    public const string JSONB_AGG = "JSONB_AGG";
    /// <summary>
    /// EN: SQL keyword used for array aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de arrays.
    /// </summary>
    public const string ARRAY_AGG = "ARRAY_AGG";
    /// <summary>
    /// EN: SQL keyword used for boolean AND aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao booleana AND.
    /// </summary>
    public const string BOOL_AND = "BOOL_AND";
    /// <summary>
    /// EN: SQL keyword used as a synonym for boolean AND aggregation.
    /// PT-br: Palavra-chave SQL usada como sinonimo de agregacao booleana AND.
    /// </summary>
    public const string EVERY = "EVERY";
    /// <summary>
    /// EN: SQL keyword used for boolean OR aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao booleana OR.
    /// </summary>
    public const string BOOL_OR = "BOOL_OR";
    /// <summary>
    /// EN: SQL keyword used for object collection aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de colecao de objetos.
    /// </summary>
    public const string COLLECT = "COLLECT";
    /// <summary>
    /// EN: SQL keyword used for total aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de total.
    /// </summary>
    public const string TOTAL = "TOTAL";
    /// <summary>
    /// EN: SQL keyword used for standard deviation aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de desvio padrao.
    /// </summary>
    public const string STDEV = "STDEV";
    /// <summary>
    /// EN: SQL keyword used for population standard deviation aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de desvio padrao populacional.
    /// </summary>
    public const string STDEVP = "STDEVP";
    /// <summary>
    /// EN: SQL keyword used for variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia.
    /// </summary>
    public const string VAR = "VAR";
    /// <summary>
    /// EN: SQL keyword used for population variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia populacional.
    /// </summary>
    public const string VARP = "VARP";
    /// <summary>
    /// EN: SQL keyword used for population variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia populacional.
    /// </summary>
    public const string VAR_POP = "VAR_POP";
    /// <summary>
    /// EN: SQL keyword used for variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia.
    /// </summary>
    public const string VARIANCE = "VARIANCE";
    /// <summary>
    /// EN: SQL keyword used for sample variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia amostral.
    /// </summary>
    public const string VARIANCE_SAMP = "VARIANCE_SAMP";
    /// <summary>
    /// EN: SQL keyword used for sample variance aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao de variancia amostral.
    /// </summary>
    public const string VAR_SAMP = "VAR_SAMP";
    /// <summary>
    /// EN: SQL keyword used for coefficient of variation aggregation.
    /// PT-br: Palavra-chave SQL usada para agregacao do coeficiente de variacao.
    /// </summary>
    public const string CV = "CV";


    #endregion

    #region Pagination and row limiting
    /// <summary>
    /// EN: SQL keyword used for TOP row limiting.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com TOP.
    /// </summary>
    public const string TOP = "TOP";
    /// <summary>
    /// EN: SQL keyword used for LIMIT row limiting.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com LIMIT.
    /// </summary>
    public const string LIMIT = "LIMIT";
    /// <summary>
    /// EN: SQL keyword used for OFFSET row skipping.
    /// PT-br: Palavra-chave SQL usada para pular linhas com OFFSET.
    /// </summary>
    public const string OFFSET = "OFFSET";
    /// <summary>
    /// EN: SQL keyword used for FETCH row limiting.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com FETCH.
    /// </summary>
    public const string FETCH = "FETCH";
    /// <summary>
    /// EN: SQL keyword used for SKIP row skipping.
    /// PT-br: Palavra-chave SQL usada para pular linhas com SKIP.
    /// </summary>
    public const string SKIP = "SKIP";
    /// <summary>
    /// EN: SQL keyword used for Oracle-style row limiting with ROWNUM.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas em estilo Oracle com ROWNUM.
    /// </summary>
    public const string ROWNUM = "ROWNUM";
    /// <summary>
    /// EN: SQL keyword used for NEXT row limiting.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com NEXT.
    /// </summary>
    public const string NEXT = "NEXT";
    /// <summary>
    /// EN: SQL keyword used for FIRST row limiting.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com FIRST.
    /// </summary>
    public const string FIRST = "FIRST";
    /// <summary>
    /// EN: SQL keyword used for row limiting with ROW.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com ROW.
    /// </summary>
    public const string ROW = "ROW";
    /// <summary>
    /// EN: SQL keyword used for row limiting with ROWS.
    /// PT-br: Palavra-chave SQL usada para limitacao de linhas com ROWS.
    /// </summary>
    public const string ROWS = "ROWS";
    /// <summary>
    /// EN: SQL keyword used to specify an upper bound for a row range.
    /// PT-br: Palavra-chave SQL usada para especificar o limite superior de um intervalo de linhas.
    /// </summary>
    public const string TO = "TO";
    /// <summary>
    /// EN: SQL keyword used to request a single row in limiting syntax.
    /// PT-br: Palavra-chave SQL usada para solicitar uma unica linha na sintaxe de limitacao.
    /// </summary>
    public const string ONLY = "ONLY";
    /// <summary>
    /// EN: Composite token used to describe OFFSET/FETCH pagination.
    /// PT-br: Token composto usado para descrever paginacao OFFSET/FETCH.
    /// </summary>
    public const string OFFSET_FETCH = "OFFSET/FETCH";
    /// <summary>
    /// EN: Composite token used to describe FETCH FIRST/NEXT pagination.
    /// PT-br: Token composto usado para descrever paginacao FETCH FIRST/NEXT.
    /// </summary>
    public const string FETCH_FIRST_NEXT = "FETCH FIRST/NEXT";
    #endregion

    #region Identity and sequence
    /// <summary>
    /// EN: SQL keyword used for case-insensitive string matching in PostgreSQL-style syntax.
    /// PT-br: Palavra-chave SQL usada para correspondencia de string sem diferenca de caixa em sintaxe estilo PostgreSQL.
    /// </summary>
    public const string ILIKE = "ILIKE";
    /// <summary>
    /// EN: SQL keyword used for identity columns.
    /// PT-br: Palavra-chave SQL usada para colunas identity.
    /// </summary>
    public const string IDENTITY = "IDENTITY";
    /// <summary>
    /// EN: SQL keyword used for auto-increment columns.
    /// PT-br: Palavra-chave SQL usada para colunas auto-incremento.
    /// </summary>
    public const string AUTO_INCREMENT = "AUTO_INCREMENT";
    /// <summary>
    /// EN: SQL keyword used for serial columns.
    /// PT-br: Palavra-chave SQL usada para colunas serial.
    /// </summary>
    public const string SERIAL = "SERIAL";
    /// <summary>
    /// EN: SQL keyword used for big serial columns.
    /// PT-br: Palavra-chave SQL usada para colunas bigserial.
    /// </summary>
    public const string BIGSERIAL = "BIGSERIAL";
    /// <summary>
    /// EN: SQL keyword used to combine queries with UNION.
    /// PT-br: Palavra-chave SQL usada para combinar consultas com UNION.
    /// </summary>
    public const string UNION = "UNION";
    /// <summary>
    /// EN: SQL keyword used for string concatenation in some dialects.
    /// PT-br: Palavra-chave SQL usada para concatenacao de strings em alguns dialetos.
    /// </summary>
    public const string CONCAT = "CONCAT";
    /// <summary>
    /// EN: SQL keyword used for string concatenation with a separator.
    /// PT-br: Palavra-chave SQL usada para concatenacao de strings com separador.
    /// </summary>
    public const string CONCAT_WS = "CONCAT_WS";
    /// <summary>
    /// EN: SQL keyword used to read the next value from a sequence.
    /// PT-br: Palavra-chave SQL usada para ler o proximo valor de uma sequence.
    /// </summary>
    public const string NEXTVAL = "NEXTVAL";
    /// <summary>
    /// EN: SQL keyword used to read the current value from a sequence.
    /// PT-br: Palavra-chave SQL usada para ler o valor atual de uma sequence.
    /// </summary>
    public const string CURRVAL = "CURRVAL";
    /// <summary>
    /// EN: SQL keyword used to read the last value from a sequence.
    /// PT-br: Palavra-chave SQL usada para ler o ultimo valor de uma sequence.
    /// </summary>
    public const string LASTVAL = "LASTVAL";
    /// <summary>
    /// EN: SQL keyword used to set the value of a sequence.
    /// PT-br: Palavra-chave SQL usada para definir o valor de uma sequence.
    /// </summary>
    public const string SETVAL = "SETVAL";
    /// <summary>
    /// EN: SQL keyword used to refer to a sequence object.
    /// PT-br: Palavra-chave SQL usada para referenciar um objeto sequence.
    /// </summary>
    public const string SEQUENCE = "SEQUENCE";
    /// <summary>
    /// EN: SQL keyword used to mark a sequence cycle option.
    /// PT-br: Palavra-chave SQL usada para marcar a opcao de ciclo de uma sequence.
    /// </summary>
    public const string CYCLE = "CYCLE";
    /// <summary>
    /// EN: SQL keyword used to define the minimum value of a sequence.
    /// PT-br: Palavra-chave SQL usada para definir o valor minimo de uma sequence.
    /// </summary>
    public const string MINVALUE = "MINVALUE";
    /// <summary>
    /// EN: SQL keyword used to define the maximum value of a sequence.
    /// PT-br: Palavra-chave SQL usada para definir o valor maximo de uma sequence.
    /// </summary>
    public const string MAXVALUE = "MAXVALUE";
    #endregion

    #region Predicate helpers
    /// <summary>
    /// EN: SQL quantifier used for universal predicates.
    /// PT-br: Quantificador SQL usado para predicados universais.
    /// </summary>
    public const string ALL = "ALL";
    /// <summary>
    /// EN: SQL keyword used to name aliases.
    /// PT-br: Palavra-chave SQL usada para nomear aliases.
    /// </summary>
    public const string AS = "AS";
    /// <summary>
    /// EN: SQL keyword used in searched CASE expressions.
    /// PT-br: Palavra-chave SQL usada em expressoes CASE pesquisadas.
    /// </summary>
    public const string WHEN = "WHEN";
    /// <summary>
    /// EN: SQL keyword used in searched CASE expressions.
    /// PT-br: Palavra-chave SQL usada em expressoes CASE pesquisadas.
    /// </summary>
    public const string THEN = "THEN";
    /// <summary>
    /// EN: SQL keyword used in CASE expressions.
    /// PT-br: Palavra-chave SQL usada em expressoes CASE.
    /// </summary>
    public const string ELSE = "ELSE";
    /// <summary>
    /// EN: SQL keyword used to close CASE and block constructs.
    /// PT-br: Palavra-chave SQL usada para encerrar CASE e construtos de bloco.
    /// </summary>
    public const string END = "END";
    /// <summary>
    /// EN: SQL keyword used to test for existence.
    /// PT-br: Palavra-chave SQL usada para testar existencia.
    /// </summary>
    public const string EXISTS = "EXISTS";
    /// <summary>
    /// EN: SQL keyword used in IN predicates.
    /// PT-br: Palavra-chave SQL usada em predicados IN.
    /// </summary>
    public const string IN = "IN";
    /// <summary>
    /// EN: SQL keyword used for scalar subquery contexts.
    /// PT-br: Palavra-chave SQL usada para contextos de subconsulta escalar.
    /// </summary>
    public const string SCALAR = "SCALAR";
    /// <summary>
    /// EN: Token prefix used for quantified ANY predicates.
    /// PT-br: Prefixo de token usado para predicados ANY quantificados.
    /// </summary>
    public const string QANY = "QANY_";
    /// <summary>
    /// EN: Token prefix used for quantified ALL predicates.
    /// PT-br: Prefixo de token usado para predicados ALL quantificados.
    /// </summary>
    public const string QALL = "QALL_";
    /// <summary>
    /// EN: SQL keyword used for logical OR.
    /// PT-br: Palavra-chave SQL usada para OR logico.
    /// </summary>
    public const string OR = "OR";
    /// <summary>
    /// EN: SQL keyword used for BETWEEN predicates.
    /// PT-br: Palavra-chave SQL usada para predicados BETWEEN.
    /// </summary>
    public const string BETWEEN = "BETWEEN";
    /// <summary>
    /// EN: SQL keyword used for logical AND.
    /// PT-br: Palavra-chave SQL usada para AND logico.
    /// </summary>
    public const string AND = "AND";
    /// <summary>
    /// EN: Helper token containing a spaced AND fragment.
    /// PT-br: Token auxiliar contendo um fragmento AND com espacos.
    /// </summary>
    public const string _AND_ = " AND ";
    /// <summary>
    /// EN: SQL keyword used in DML ON DUPLICATE DO/SET clauses.
    /// PT-br: Palavra-chave SQL usada em clausulas DO/SET de ON DUPLICATE.
    /// </summary>
    public const string DO = "DO";
    /// <summary>
    /// EN: SQL keyword used to set assignment targets.
    /// PT-br: Palavra-chave SQL usada para definir destinos de atribuicao.
    /// </summary>
    public const string SET = "SET";
    /// <summary>
    /// EN: SQL keyword used to ignore a duplicate-row outcome.
    /// PT-br: Palavra-chave SQL usada para ignorar um resultado de linha duplicada.
    /// </summary>
    public const string NOTHING = "NOTHING";
    /// <summary>
    /// EN: SQL keyword used to represent no value.
    /// PT-br: Palavra-chave SQL usada para representar nenhum valor.
    /// </summary>
    public const string NONE = "NONE";
    /// <summary>
    /// EN: SQL keyword used to return values from DML statements.
    /// PT-br: Palavra-chave SQL usada para retornar valores de instrucoes DML.
    /// </summary>
    public const string RETURNING = "RETURNING";
    /// <summary>
    /// EN: SQL keyword used to mark a matched merge branch.
    /// PT-br: Palavra-chave SQL usada para marcar um ramo matched do merge.
    /// </summary>
    public const string MATCHED = "MATCHED";
    /// <summary>
    /// EN: SQL keyword used to negate a predicate.
    /// PT-br: Palavra-chave SQL usada para negar um predicado.
    /// </summary>
    public const string NOT = "NOT";
    /// <summary>
    /// EN: SQL keyword used to denote the absence of a condition or source.
    /// PT-br: Palavra-chave SQL usada para denotar ausencia de condicao ou origem.
    /// </summary>
    public const string NO = "NO";
    /// <summary>
    /// EN: SQL keyword used to introduce a FOR clause.
    /// PT-br: Palavra-chave SQL usada para introduzir uma clausula FOR.
    /// </summary>
    public const string FOR = "FOR";
    /// <summary>
    /// EN: SQL keyword used for query or DML options.
    /// PT-br: Palavra-chave SQL usada para opcoes de consulta ou DML.
    /// </summary>
    public const string OPTION = "OPTION";
    /// <summary>
    /// EN: MySQL keyword used to request SQL_CALC_FOUND_ROWS behavior.
    /// PT-br: Palavra-chave MySQL usada para solicitar o comportamento SQL_CALC_FOUND_ROWS.
    /// </summary>
    public const string SQL_CALC_FOUND_ROWS = "SQL_CALC_FOUND_ROWS";
    #endregion

    #region Query shape and JSON/PIVOT
    /// <summary>
    /// EN: SQL keyword used to mark a DISTINCT result set.
    /// PT-br: Palavra-chave SQL usada para marcar um conjunto de resultados DISTINCT.
    /// </summary>
    public const string DISTINCT = "DISTINCT";
    /// <summary>
    /// EN: SQL keyword used to mark a recursive CTE.
    /// PT-br: Palavra-chave SQL usada para marcar uma CTE recursiva.
    /// </summary>
    public const string RECURSIVE = "RECURSIVE";
    /// <summary>
    /// EN: SQL keyword used to mark a materialized CTE.
    /// PT-br: Palavra-chave SQL usada para marcar uma CTE materializada.
    /// </summary>
    public const string MATERIALIZED = "MATERIALIZED";
    /// <summary>
    /// EN: SQL keyword used to reference a JSON table construct.
    /// PT-br: Palavra-chave SQL usada para referenciar uma construcao JSON_TABLE.
    /// </summary>
    public const string JSON_TABLE = "JSON_TABLE";
    /// <summary>
    /// EN: SQL keyword used to reference an OPENJSON construct.
    /// PT-br: Palavra-chave SQL usada para referenciar uma construcao OPENJSON.
    /// </summary>
    public const string OPENJSON = "OPENJSON";
    /// <summary>
    /// EN: SQL keyword used to introduce JSON_TABLE column definitions.
    /// PT-br: Palavra-chave SQL usada para introduzir definicoes de coluna do JSON_TABLE.
    /// </summary>
    public const string COLUMNS = "COLUMNS";
    /// <summary>
    /// EN: SQL keyword used to reference a STRING_SPLIT construct.
    /// PT-br: Palavra-chave SQL usada para referenciar uma construcao STRING_SPLIT.
    /// </summary>
    public const string STRING_SPLIT = "STRING_SPLIT";
    /// <summary>
    /// EN: SQL keyword used to introduce nested JSON_TABLE columns.
    /// PT-br: Palavra-chave SQL usada para introduzir colunas aninhadas do JSON_TABLE.
    /// </summary>
    public const string NESTED = "NESTED";
    /// <summary>
    /// EN: SQL keyword used to reference a PIVOT construct.
    /// PT-br: Palavra-chave SQL usada para referenciar uma construcao PIVOT.
    /// </summary>
    public const string PIVOT = "PIVOT";
    /// <summary>
    /// EN: SQL keyword used to reference an UNPIVOT construct.
    /// PT-br: Palavra-chave SQL usada para referenciar uma construcao UNPIVOT.
    /// </summary>
    public const string UNPIVOT = "UNPIVOT";
    /// <summary>
    /// EN: SQL keyword used to mark a JSON path.
    /// PT-br: Palavra-chave SQL usada para marcar um caminho JSON.
    /// </summary>
    public const string PATH = "PATH";
    /// <summary>
    /// EN: SQL keyword used to request AUTO JSON output.
    /// PT-br: Palavra-chave SQL usada para solicitar saida JSON AUTO.
    /// </summary>
    public const string AUTO = "AUTO";
    /// <summary>
    /// EN: SQL keyword used to request the root JSON object.
    /// PT-br: Palavra-chave SQL usada para solicitar o objeto JSON raiz.
    /// </summary>
    public const string ROOT = "ROOT";
    /// <summary>
    /// EN: SQL keyword used to request a USE hint.
    /// PT-br: Palavra-chave SQL usada para solicitar uma dica USE.
    /// </summary>
    public const string USE = "USE";
    /// <summary>
    /// EN: SQL keyword used to request a FORCE hint.
    /// PT-br: Palavra-chave SQL usada para solicitar uma dica FORCE.
    /// </summary>
    public const string FORCE = "FORCE";
    /// <summary>
    /// EN: SQL keyword used to mark a primary index or key.
    /// PT-br: Palavra-chave SQL usada para marcar um indice ou chave primaria.
    /// </summary>
    public const string PRIMARY = "PRIMARY";
    /// <summary>
    /// EN: SQL keyword used to mark a key column or key constraint.
    /// PT-br: Palavra-chave SQL usada para marcar uma coluna ou restricao de chave.
    /// </summary>
    public const string KEY = "KEY";
    /// <summary>
    /// EN: SQL keyword used to refer to a previous value in triggers or sequences.
    /// PT-br: Palavra-chave SQL usada para se referir a um valor anterior em triggers ou sequences.
    /// </summary>
    public const string PREVIOUS = "PREVIOUS";
    /// <summary>
    /// EN: SQL keyword used to mark a WITHIN GROUP clause.
    /// PT-br: Palavra-chave SQL usada para marcar uma clausula WITHIN GROUP.
    /// </summary>
    public const string WITHIN = "WITHIN";
    /// <summary>
    /// EN: SQL keyword used to include JSON null values.
    /// PT-br: Palavra-chave SQL usada para incluir valores JSON nulos.
    /// </summary>
    public const string INCLUDE_NULL_VALUES = "INCLUDE_NULL_VALUES";
    /// <summary>
    /// EN: SQL keyword used to suppress JSON array wrappers.
    /// PT-br: Palavra-chave SQL usada para suprimir wrappers de array JSON.
    /// </summary>
    public const string WITHOUT_ARRAY_WRAPPER = "WITHOUT_ARRAY_WRAPPER";
    /// <summary>
    /// EN: Composite token used to identify FOR JSON clauses.
    /// PT-br: Token composto usado para identificar clausulas FOR JSON.
    /// </summary>
    public const string FOR_JSON = "FOR JSON";
    /// <summary>
    /// EN: Composite token used to identify WITH CTE clauses.
    /// PT-br: Token composto usado para identificar clausulas WITH CTE.
    /// </summary>
    public const string WITH_CTE = "_WITH/CTE";
    #endregion

    #region Literals and values
    /// <summary>
    /// EN: SQL keyword used to provide a default value.
    /// PT-br: Palavra-chave SQL usada para fornecer um valor padrao.
    /// </summary>
    public const string DEFAULT = "DEFAULT";
    /// <summary>
    /// EN: SQL keyword used to represent a null literal.
    /// PT-br: Palavra-chave SQL usada para representar um literal nulo.
    /// </summary>
    public const string NULL = "NULL";
    /// <summary>
    /// EN: SQL keyword used to represent a true literal.
    /// PT-br: Palavra-chave SQL usada para representar um literal true.
    /// </summary>
    public const string TRUE = "TRUE";
    /// <summary>
    /// EN: SQL keyword used to represent a false literal.
    /// PT-br: Palavra-chave SQL usada para representar um literal false.
    /// </summary>
    public const string FALSE = "FALSE";
    /// <summary>
    /// EN: SQL keyword used to refer to a year part or interval unit.
    /// PT-br: Palavra-chave SQL usada para referenciar uma parte de ano ou unidade de intervalo.
    /// </summary>
    public const string YEAR = "YEAR";
    /// <summary>
    /// EN: SQL keyword used to request low-priority locking or execution.
    /// PT-br: Palavra-chave SQL usada para solicitar bloqueio ou execucao de baixa prioridade.
    /// </summary>
    public const string LOW_PRIORITY = "LOW_PRIORITY";
    /// <summary>
    /// EN: SQL keyword used to mark delayed processing.
    /// PT-br: Palavra-chave SQL usada para marcar processamento atrasado.
    /// </summary>
    public const string DELAYED = "DELAYED";
    /// <summary>
    /// EN: SQL keyword used to request high-priority locking or execution.
    /// PT-br: Palavra-chave SQL usada para solicitar bloqueio ou execucao de alta prioridade.
    /// </summary>
    public const string HIGH_PRIORITY = "HIGH_PRIORITY";
    /// <summary>
    /// EN: SQL keyword used to ignore conflicting rows in MySQL-style syntax.
    /// PT-br: Palavra-chave SQL usada para ignorar linhas em conflito em sintaxe estilo MySQL.
    /// </summary>
    public const string IGNORE = "IGNORE";
    #endregion
}
