#include "kafka_fdw.h"
#include "mb/pg_wchar.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct KafkaFdwOption
{
    const char *optname;
    Oid         optcontext; /* Oid of catalog in which option may appear */
    bool        librdkafka; /* true if used in librdkafka */
};

/*
 * Valid options for kafka_fdw.
 * These options are based on the options for the COPY FROM command.
 * But note that force_not_null and force_null are handled as boolean options
 * attached to a column, not as table options.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * fileGetOptions(), which currently doesn't bother to look at user mappings.
 */
static const struct KafkaFdwOption valid_options[] = {

    /* Connection options */
    { "brokers", ForeignServerRelationId, true },

    /* table options */
    { "topic", ForeignTableRelationId, true },
    { "batch_size", ForeignTableRelationId, true },
    { "buffer_delay", ForeignTableRelationId, true },
    { "strict", ForeignTableRelationId, true },
    { "ignore_junk", ForeignTableRelationId, true },

    /* Format options */
    /* oids option is not supported */
    { "format", ForeignTableRelationId, false },
    { "delimiter", ForeignTableRelationId, false },
    { "quote", ForeignTableRelationId, false },
    { "escape", ForeignTableRelationId, false },
    { "null", ForeignTableRelationId, false },
    { "encoding", ForeignTableRelationId, false },
    { "force_not_null", AttributeRelationId, false },
    { "force_null", AttributeRelationId, false },
    { "offset", AttributeRelationId, false },
    { "partition", AttributeRelationId, false },
    { "junk", AttributeRelationId, false },
    { "junk_error", AttributeRelationId, false },
    /*
     * force_quote is not supported by kafka_fdw because it's for COPY TO for now.
     */

    /* Sentinel */
    { NULL, InvalidOid, false }
};

/*
 * Helper functions
 */

static bool is_valid_option(const char *keyword, Oid context);

static void get_kafka_fdw_attribute_options(Oid relid, KafkaOptions *kafka_options);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses kafka_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */

PG_FUNCTION_INFO_V1(kafka_fdw_validator);
Datum kafka_fdw_validator(PG_FUNCTION_ARGS)
{
    DEBUGLOG("%s", __func__);

    List *    options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid       catalog      = PG_GETARG_OID(1);
    ListCell *cell;

    /*
     * Check that only options supported by kafka_fdw, and allowed for the
     * current object type, are given.
     */
    foreach (cell, options_list)
    {
        DefElem *def = (DefElem *) lfirst(cell);

        if (!is_valid_option(def->defname, catalog))
        {
            const struct KafkaFdwOption *opt;
            StringInfoData               buf;

            /*
             * Unknown option specified, complain about it. Provide a hint
             * with list of valid options for the object.
             */
            initStringInfo(&buf);
            for (opt = valid_options; opt->optname; opt++)
            {
                if (catalog == opt->optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->optname);
            }

            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\"", def->defname),
                     buf.len > 0 ? errhint("Valid options in this context are: %s", buf.data)
                                 : errhint("There are no valid options in this context.")));
        }
    }

    if (catalog == ForeignTableRelationId)
        KafkaProcessKafkaOptions(catalog, NULL, options_list);
    else
        KafkaProcessKafkaOptions(InvalidOid, NULL, options_list);
    /*
     * Now apply the core COPY code's validation logic for more checks.
    static  */
    KafkaProcessParseOptions(NULL, options_list);

    /*
     * topic option is required for kafka_fdw foreign tables.
     */

    PG_RETURN_VOID();
}

/*
 * Process the statement option list for Parsing.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into parse_options, applying appropriate error checking.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void
KafkaProcessParseOptions(ParseOptions *parse_options, List *options)
{
    ListCell *option;

    /* Support external use for option sanity checking */
    if (parse_options == NULL)
        parse_options = &(ParseOptions){};

    parse_options->file_encoding = -1;

    /* Extract options from the statement node tree */
    foreach (option, options)
    {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "format") == 0)
        {
            char *fmt = defGetString(defel);

            if (strcmp(fmt, "text") == 0)
                /* default format */;
            else if (strcmp(fmt, "csv") == 0)
                parse_options->csv_mode = true;
            else if (strcmp(fmt, "binary") == 0)
                parse_options->binary = true;
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("COPY format \"%s\" not recognized", fmt)));
        }
        else if (strcmp(defel->defname, "delimiter") == 0)
        {
            if (parse_options->delim)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            parse_options->delim = defGetString(defel);
        }
        else if (strcmp(defel->defname, "null") == 0)
        {
            if (parse_options->null_print)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            parse_options->null_print = defGetString(defel);
        }
        else if (strcmp(defel->defname, "quote") == 0)
        {
            if (parse_options->quote)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            parse_options->quote = defGetString(defel);
        }
        else if (strcmp(defel->defname, "escape") == 0)
        {
            if (parse_options->escape)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            parse_options->escape = defGetString(defel);
        }
        else if (strcmp(defel->defname, "force_not_null") == 0)
        {
            if (parse_options->force_notnull)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            if (defel->arg && IsA(defel->arg, List))
                parse_options->force_notnull = (List *) defel->arg;
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("argument to option \"%s\" must be a list of column names", defel->defname)));
        }
        else if (strcmp(defel->defname, "force_null") == 0)
        {
            if (parse_options->force_null)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            if (defel->arg && IsA(defel->arg, List))
                parse_options->force_null = (List *) defel->arg;
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("argument to option \"%s\" must be a list of column names", defel->defname)));
        }
        else if (strcmp(defel->defname, "encoding") == 0)
        {
            if (parse_options->file_encoding >= 0)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            parse_options->file_encoding = pg_char_to_encoding(defGetString(defel));
        }
    }

    /*
     * Check for incompatible options (must do these two before inserting
     * defaults)
     */
    if (parse_options->binary && parse_options->delim)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot specify DELIMITER in BINARY mode")));

    if (parse_options->binary && parse_options->null_print)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot specify NULL in BINARY mode")));

    /* Set defaults for omitted options */
    if (!parse_options->delim)
        parse_options->delim = parse_options->csv_mode ? "," : "\t";

    if (!parse_options->null_print)
        parse_options->null_print = parse_options->csv_mode ? "" : "\\N";
    parse_options->null_print_len = strlen(parse_options->null_print);

    if (parse_options->csv_mode)
    {
        if (!parse_options->quote)
            parse_options->quote = "\"";
        if (!parse_options->escape)
            parse_options->escape = parse_options->quote;
    }

    /* Only single-byte delimiter strings are supported. */
    if (strlen(parse_options->delim) != 1)
        ereport(
          ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw delimiter must be a single one-byte character")));

    /* Disallow end-of-line characters */
    if (strchr(parse_options->delim, '\r') != NULL || strchr(parse_options->delim, '\n') != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("KafkaFdw delimiter cannot be newline or carriage return")));

    if (strchr(parse_options->null_print, '\r') != NULL || strchr(parse_options->null_print, '\n') != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("KafkaFdw null representation cannot use newline or carriage return")));

    /*
     * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
     * backslash because it would be ambiguous.  We can't allow the other
     * cases because data characters matching the delimiter must be
     * backslashed, and certain backslash combinations are interpreted
     * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
     * more than strictly necessary, but seems best for consistency and
     * future-proofing.  Likewise we disallow all digits though only octal
     * digits are actually dangerous.
     */
    if (!parse_options->csv_mode && strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789", parse_options->delim[0]) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("KafkaFdw delimiter cannot be \"%s\"", parse_options->delim)));

    /* Check quote */
    if (!parse_options->csv_mode && parse_options->quote != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw quote available only in CSV mode")));

    if (parse_options->csv_mode && strlen(parse_options->quote) != 1)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw quote must be a single one-byte character")));

    if (parse_options->csv_mode && parse_options->delim[0] == parse_options->quote[0])
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("KafkaFdw delimiter and quote must be different")));

    /* Check escape */
    if (!parse_options->csv_mode && parse_options->escape != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw escape available only in CSV mode")));

    if (parse_options->csv_mode && strlen(parse_options->escape) != 1)
        ereport(
          ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw escape must be a single one-byte character")));

    /* Check force_notnull */
    if (!parse_options->csv_mode && parse_options->force_notnull != NIL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw force not null available only in CSV mode")));

    /* Check force_null */
    if (!parse_options->csv_mode && parse_options->force_null != NIL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw force null available only in CSV mode")));

    /* Don't allow the delimiter to appear in the null string. */
    if (strchr(parse_options->null_print, parse_options->delim[0]) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("KafkaFdw delimiter must not appear in the NULL specification")));

    /* Don't allow the CSV quote char to appear in the null string. */
    if (parse_options->csv_mode && strchr(parse_options->null_print, parse_options->quote[0]) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("CSV quote character must not appear in the NULL specification")));
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
    const struct KafkaFdwOption *opt;

    for (opt = valid_options; opt->optname; opt++)
    {
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
            return true;
    }
    return false;
}

/*
 * Separate out brokers, topic and column-specific options, since
 * ProcessCopyOptions won't accept them.
 */
void
KafkaProcessKafkaOptions(Oid relid, KafkaOptions *kafka_options, List *options)
{

    ListCell *option;

    /* Support external use for option sanity checking */
    if (kafka_options == NULL)
        kafka_options = &(KafkaOptions){ .offset_attnum = -1, .partition_attnum = -1, .junk_attnum = -1 };

    foreach (option, options)
    {
        DefElem *def = (DefElem *) lfirst(option);

        if (strcmp(def->defname, "topic") == 0)
        {
            if (kafka_options->topic)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options %s", def->defname)));
            kafka_options->topic = defGetString(def);
        }

        else if (strcmp(def->defname, "brokers") == 0)
        {
            if (kafka_options->brokers)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options %s", def->defname)));
            kafka_options->brokers = defGetString(def);
        }

        else if (strcmp(def->defname, "buffer_delay") == 0)
        {
            kafka_options->buffer_delay = strtol(defGetString(def), NULL, 10);
            if (kafka_options->buffer_delay <= 0)
                ereport(
                  ERROR,
                  (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires a non-negative integer value", def->defname)));
        }

        else if (strcmp(def->defname, "batch_size") == 0)
        {

            kafka_options->batch_size = strtol(defGetString(def), NULL, 10);
            if (kafka_options->batch_size <= 0)
                ereport(
                  ERROR,
                  (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires a non-negative integer value", def->defname)));
        }
        else if (strcmp(def->defname, "strict") == 0)
        {
            kafka_options->strict = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "ignore_junk") == 0)
        {
            kafka_options->ignore_junk = defGetBoolean(def);
        }
    }

    if (relid != InvalidOid)
        get_kafka_fdw_attribute_options(relid, kafka_options);

    kafka_options->ignore_junk =
      kafka_options->ignore_junk || kafka_options->junk_attnum != -1 || kafka_options->junk_error_attnum != -1;
}

/* reads per attribute options into kafka options */
static void
get_kafka_fdw_attribute_options(Oid relid, KafkaOptions *kafka_options)
{

    Relation   rel;
    TupleDesc  tupleDesc;
    AttrNumber natts;
    AttrNumber attnum;

    rel                          = heap_open(relid, AccessShareLock);
    tupleDesc                    = RelationGetDescr(rel);
    natts                        = tupleDesc->natts;
    kafka_options->num_parse_col = 0;

    /* Retrieve FDW options for all user-defined attributes. */
    for (attnum = 1; attnum <= natts; attnum++)
    {
        Form_pg_attribute attr = tupleDesc->attrs[attnum - 1];
        List *            options;
        ListCell *        lc;
        kafka_options->num_parse_col++;

        /* Skip dropped attributes. */
        if (attr->attisdropped)
            continue;

        options = GetForeignColumnOptions(relid, attnum);
        foreach (lc, options)
        {
            DefElem *def = (DefElem *) lfirst(lc);

            if (strcmp(def->defname, "partition") == 0)
            {
                if (kafka_options->partition_attnum != -1)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("duplicate option partition")));
                if (defGetBoolean(def))
                {
                    kafka_options->partition_attnum = attnum;
                }
            }
            else if (strcmp(def->defname, "offset") == 0)
            {
                if (kafka_options->offset_attnum != -1)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("duplicate option offset")));
                if (defGetBoolean(def))
                    kafka_options->offset_attnum = attnum;
            }
            else if (strcmp(def->defname, "junk") == 0)
            {
                if (kafka_options->junk_attnum != -1)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("duplicate option junk")));
                if (defGetBoolean(def))
                    kafka_options->junk_attnum = attnum;
            }
            else if (strcmp(def->defname, "junk_error") == 0)
            {
                if (kafka_options->junk_error_attnum != -1)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("duplicate option junk_error")));
                if (defGetBoolean(def))
                    kafka_options->junk_error_attnum = attnum;
            }
            /* maybe in future handle other options here */
        }
    }

    /* calculate number of parsable columns */

    if (kafka_options->partition_attnum != -1)
        kafka_options->num_parse_col--;
    if (kafka_options->offset_attnum != -1)
        kafka_options->num_parse_col--;
    if (kafka_options->junk_attnum != -1)
        kafka_options->num_parse_col--;
    if (kafka_options->junk_error_attnum != -1)
        kafka_options->num_parse_col--;

    /*
        if (kafka_options->partition_attnum == -1)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("no partition column set")));

        if (kafka_options->offset_attnum == -1)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("no offset column set")));
    */
    heap_close(rel, AccessShareLock);
}
