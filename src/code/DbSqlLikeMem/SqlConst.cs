namespace DbSqlLikeMem;

/// <summary>
/// Contants
/// </summary>
public static class SqlConst
{
#pragma warning disable CS1591

    #region DML and DDL keywords
    public const string SELECT = "SELECT";
    public const string WITH = "WITH";
    public const string INSERT = "INSERT";
    public const string REPLACE = "REPLACE";
    public const string UPDATE = "UPDATE";
    public const string DELETE = "DELETE";
    public const string MERGE = "MERGE";
    public const string CREATE = "CREATE";
    public const string SCHEMA = "SCHEMA";
    public const string RECREATE = "RECREATE";
    public const string ALTER = "ALTER";
    public const string DROP = "DROP";
    public const string RESTART = "RESTART";
    public const string OWNED = "OWNED";
    public const string GENERATOR = "GENERATOR";
    public const string EXECUTE = "EXECUTE";
    public const string BLOCK = "BLOCK";
    public const string STATEMENT = "STATEMENT";
    public const string SUSPEND = "SUSPEND";
    public const string EXIT = "EXIT";
    public const string BREAK = "BREAK";
    public const string LEAVE = "LEAVE";
    public const string INTO = "INTO";
    public const string VALUE = "VALUE";
    public const string VALUES = "VALUES";
    public const string PARTITION = "PARTITION";
    public const string DUPLICATE = "DUPLICATE";
    public const string WHERE = "WHERE";
    public const string FROM = "FROM";
    public const string USING = "USING";
    public const string JOIN = "JOIN";
    public const string INNER = "INNER";
    public const string LEFT = "LEFT";
    public const string RIGHT = "RIGHT";
    public const string FULL = "FULL";
    public const string CROSS = "CROSS";
    public const string OUTER = "OUTER";
    public const string APPLY = "APPLY";
    public const string ON = "ON";
    public const string GROUP = "GROUP";
    public const string BY = "BY";
    public const string ORDER = "ORDER";
    public const string HAVING = "HAVING";
    public const string VIEW = "VIEW";
    public const string UNIQUE = "UNIQUE";
    public const string INDEX = "INDEX";
    public const string FUNCTION = "FUNCTION";
    public const string PROCEDURE = "PROCEDURE";
    public const string TRIGGER = "TRIGGER";
    public const string BEFORE = "BEFORE";
    public const string AFTER = "AFTER";
    public const string TEMPORARY = "TEMPORARY";
    public const string GLOBAL = "GLOBAL";
    public const string TEMP = "TEMP";
    public const string TABLE = "TABLE";
    public const string IF = "IF";
    public const string START = "START";
    public const string AUTONOMOUS = "AUTONOMOUS";
    public const string COMMON = "COMMON";
    public const string CALLER = "CALLER";
    public const string PRIVILEGES = "PRIVILEGES";
    public const string TRANSACTION = "TRANSACTION";
    public const string INCREMENT = "INCREMENT";
    public const string OUT = "OUT";
    public const string INOUT = "INOUT";
    public const string RETURNS = "RETURNS";
    public const string RETURN = "RETURN";
    public const string BEGIN = "BEGIN";
    public const string EACH = "EACH";
    public const string IS = "IS";
    public const string REFERENCING = "REFERENCING";
    public const string MODE = "MODE";
    public const string ATOMIC = "ATOMIC";
    public const string LANGUAGE = "LANGUAGE";
    public const string COLUMN = "COLUMN";
    public const string ADD = "ADD";
    public const string COUNT = "COUNT";
    public const string COUNT_BIG = "COUNT_BIG";
    public const string JSON_GROUP_OBJECT = "JSON_GROUP_OBJECT";
    public const string JSON_OBJECTAGG = "JSON_OBJECTAGG";
    public const string JSON_OBJECT_AGG = "JSON_OBJECT_AGG";
    public const string JSON_OBJECT_AGG_STRICT = "JSON_OBJECT_AGG_STRICT";
    public const string JSON_OBJECT_AGG_UNIQUE = "JSON_OBJECT_AGG_UNIQUE";
    public const string JSON_OBJECT_AGG_UNIQUE_STRICT = "JSON_OBJECT_AGG_UNIQUE_STRICT";
    public const string JSONB_OBJECT_AGG = "JSONB_OBJECT_AGG";
    public const string JSONB_OBJECT_AGG_STRICT = "JSONB_OBJECT_AGG_STRICT";
    public const string JSONB_OBJECT_AGG_UNIQUE = "JSONB_OBJECT_AGG_UNIQUE";
    public const string JSONB_OBJECT_AGG_UNIQUE_STRICT = "JSONB_OBJECT_AGG_UNIQUE_STRICT";

    public const string GROUP_CONCAT = "GROUP_CONCAT";
    public const string STRING_AGG = "STRING_AGG";
    public const string LISTAGG = "LISTAGG";
    public const string LIST = "LIST";
    public const string SUM = "SUM";
    public const string AVG = "AVG";
    public const string MIN = "MIN";
    public const string MAX = "MAX";
    public const string CHECKSUM_AGG = "CHECKSUM_AGG";
    public const string ANY_VALUE = "ANY_VALUE";
    public const string BIT_AND = "BIT_AND";
    public const string BIT_OR = "BIT_OR";
    public const string BIT_XOR = "BIT_XOR";
    public const string JSON_ARRAYAGG = "JSON_ARRAYAGG";
    public const string JSON_AGG = "JSON_AGG";
    public const string JSONB_AGG = "JSONB_AGG";
    public const string ARRAY_AGG = "ARRAY_AGG";
    public const string BOOL_AND = "BOOL_AND";
    public const string EVERY = "EVERY";
    public const string BOOL_OR = "BOOL_OR";
    public const string COLLECT = "COLLECT";
    public const string TOTAL = "TOTAL";
    public const string STDEV = "STDEV";
    public const string STDEVP = "STDEVP";
    public const string VAR = "VAR";
    public const string VARP = "VARP";
    public const string VAR_POP = "VAR_POP";
    public const string VARIANCE = "VARIANCE";
    public const string VARIANCE_SAMP = "VARIANCE_SAMP";
    public const string VAR_SAMP = "VAR_SAMP";
    public const string CV = "CV";


    #endregion

    #region Pagination and row limiting
    public const string TOP = "TOP";
    public const string LIMIT = "LIMIT";
    public const string OFFSET = "OFFSET";
    public const string FETCH = "FETCH";
    public const string SKIP = "SKIP";
    public const string ROWNUM = "ROWNUM";
    public const string NEXT = "NEXT";
    public const string FIRST = "FIRST";
    public const string ROW = "ROW";
    public const string ROWS = "ROWS";
    public const string TO = "TO";
    public const string ONLY = "ONLY";
    public const string OFFSET_FETCH = "OFFSET/FETCH";
    public const string FETCH_FIRST_NEXT = "FETCH FIRST/NEXT";
    #endregion

    #region Identity and sequence
    public const string ILIKE = "ILIKE";
    public const string IDENTITY = "IDENTITY";
    public const string AUTO_INCREMENT = "AUTO_INCREMENT";
    public const string SERIAL = "SERIAL";
    public const string BIGSERIAL = "BIGSERIAL";
    public const string UNION = "UNION";
    public const string CONCAT = "CONCAT";
    public const string CONCAT_WS = "CONCAT_WS";
    public const string NEXTVAL = "NEXTVAL";
    public const string CURRVAL = "CURRVAL";
    public const string LASTVAL = "LASTVAL";
    public const string SETVAL = "SETVAL";
    public const string SEQUENCE = "SEQUENCE";
    public const string CYCLE = "CYCLE";
    public const string MINVALUE = "MINVALUE";
    public const string MAXVALUE = "MAXVALUE";
    #endregion

    #region Predicate helpers
    public const string ALL = "ALL";
    public const string AS = "AS";
    public const string WHEN = "WHEN";
    public const string THEN = "THEN";
    public const string ELSE = "ELSE";
    public const string END = "END";
    public const string EXISTS = "EXISTS";
    public const string IN = "IN";
    public const string SCALAR = "SCALAR";
    public const string QANY = "QANY_";
    public const string QALL = "QALL_";
    public const string OR = "OR";
    public const string BETWEEN = "BETWEEN";
    public const string AND = "AND";
    public const string _AND_ = " AND ";
    public const string DO = "DO";
    public const string SET = "SET";
    public const string NOTHING = "NOTHING";
    public const string NONE = "NONE";
    public const string RETURNING = "RETURNING";
    public const string MATCHED = "MATCHED";
    public const string NOT = "NOT";
    public const string NO = "NO";
    public const string FOR = "FOR";
    public const string OPTION = "OPTION";
    public const string SQL_CALC_FOUND_ROWS = "SQL_CALC_FOUND_ROWS";
    #endregion

    #region Query shape and JSON/PIVOT
    public const string DISTINCT = "DISTINCT";
    public const string RECURSIVE = "RECURSIVE";
    public const string MATERIALIZED = "MATERIALIZED";
    public const string JSON_TABLE = "JSON_TABLE";
    public const string OPENJSON = "OPENJSON";
    public const string COLUMNS = "COLUMNS";
    public const string STRING_SPLIT = "STRING_SPLIT";
    public const string NESTED = "NESTED";
    public const string PIVOT = "PIVOT";
    public const string UNPIVOT = "UNPIVOT";
    public const string PATH = "PATH";
    public const string AUTO = "AUTO";
    public const string ROOT = "ROOT";
    public const string USE = "USE";
    public const string FORCE = "FORCE";
    public const string PRIMARY = "PRIMARY";
    public const string KEY = "KEY";
    public const string PREVIOUS = "PREVIOUS";
    public const string WITHIN = "WITHIN";
    public const string INCLUDE_NULL_VALUES = "INCLUDE_NULL_VALUES";
    public const string WITHOUT_ARRAY_WRAPPER = "WITHOUT_ARRAY_WRAPPER";
    public const string FOR_JSON = "FOR JSON";
    public const string WITH_CTE = "_WITH/CTE";
    #endregion

    #region Literals and values
    public const string DEFAULT = "DEFAULT";
    public const string NULL = "NULL";
    public const string TRUE = "TRUE";
    public const string FALSE = "FALSE";
    public const string YEAR = "YEAR";
    public const string LOW_PRIORITY = "LOW_PRIORITY";
    public const string DELAYED = "DELAYED";
    public const string HIGH_PRIORITY = "HIGH_PRIORITY";
    public const string IGNORE = "IGNORE";
    #endregion

#pragma warning restore CS1591
}
