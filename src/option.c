#include "kafka_fdw.h"
#include "mb/pg_wchar.h"

#if PG_VERSION_NUM >= 130000
#include "access/relation.h"
#endif

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct KafkaFdwOption
{
    const char *optname;
    Oid         optcontext; /* Oid of catalog in which option may appear */
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
    { "brokers", ForeignServerRelationId },

    /* table options */
    { "topic", ForeignTableRelationId },
    { "batch_size", ForeignTableRelationId },
    { "buffer_delay", ForeignTableRelationId },
    { "strict", ForeignTableRelationId },
    { "ignore_junk", ForeignTableRelationId },
    { "num_partitions", ForeignTableRelationId },

    /* Format options */
    /* oids option is not supported */
    { "format", ForeignTableRelationId },
    { "delimiter", ForeignTableRelationId },
    { "quote", ForeignTableRelationId },
    { "escape", ForeignTableRelationId },
    { "null", ForeignTableRelationId },
    { "encoding", ForeignTableRelationId },
    { "force_not_null", AttributeRelationId },
    { "force_null", AttributeRelationId },
    { "offset", AttributeRelationId },
    { "partition", AttributeRelationId },
    { "junk", AttributeRelationId },
    { "junk_error", AttributeRelationId },
    { "json", AttributeRelationId },
    /*
     * force_quote is not supported by kafka_fdw because it's for COPY TO for now.
     */

    /* Sentinel */
    { NULL, InvalidOid }
};

/*
 * Helper functions
 */

static bool is_valid_option(const char *keyword, Oid context);
static void get_kafka_fdw_attribute_options(Oid relid, KafkaOptions *kafka_options);
static void KafkaProcessParseOptions(ParseOptions *parse_options, List *options);
static void KafkaProcessKafkaOptions(Oid foreigntableid, KafkaOptions *kafka_options, List *options);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses kafka_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */

PG_FUNCTION_INFO_V1(kafka_fdw_validator);
Datum kafka_fdw_validator(PG_FUNCTION_ARGS)
{
    List *    options_list;
    Oid       catalog = PG_GETARG_OID(1);
    ListCell *cell;

    DEBUGLOG("%s", __func__);

    /*
     * Check that only options supported by kafka_fdw, and allowed for the
     * current object type, are given.
     */
    options_list = untransformRelOptions(PG_GETARG_DATUM(0));
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
 * Fetch the options for a kafka_fdw foreign table.
 *
 * We have to separate separete Kafka Options from parsing options
 */
void
kafkaGetOptions(Oid foreigntableid, KafkaOptions *kafka_options, ParseOptions *parse_options)
{
    ForeignTable *      table;
    ForeignServer *     server;
    ForeignDataWrapper *wrapper;
    List *              options;

    /*
     * Extract options from FDW objects.  We ignore user mappings because
     * kafka_fdw doesn't have any options that can be specified there.
     */

    table   = GetForeignTable(foreigntableid);
    server  = GetForeignServer(table->serverid);
    wrapper = GetForeignDataWrapper(server->fdwid);

    options = NIL;
    options = list_concat(options, wrapper->options);
    options = list_concat(options, server->options);
    options = list_concat(options, table->options);

    KafkaProcessParseOptions(parse_options, options);
    KafkaProcessKafkaOptions(foreigntableid, kafka_options, options);
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
static void
KafkaProcessParseOptions(ParseOptions *parse_options, List *options)
{
    ListCell *option;
    bool      csv = false;
    ParseOptions    default_parse_options = { .format = INVALID };

    /* Support external use for option sanity checking */
    if (parse_options == NULL)
        parse_options = &default_parse_options;

    /* Extract options from the statement node tree */
    foreach (option, options)
    {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "format") == 0)
        {
            char *fmt = defGetString(defel);
            if (strcmp(fmt, "csv") == 0)
            {
                parse_options->format = CSV;
                csv                   = true;
            }
            else if (strcmp(fmt, "json") == 0)
                parse_options->format = JSON;
            else
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("format \"%s\" not recognized", fmt)));
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
    }

    /*
     * Check for incompatible options (must do these two before inserting
     * defaults)
     */

    /* Set defaults for omitted options */
    if (!parse_options->delim)
        parse_options->delim = csv ? "," : "\t";

    if (!parse_options->null_print)
        parse_options->null_print = csv ? "" : "\\N";
    parse_options->null_print_len = strlen(parse_options->null_print);

    if (csv)
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
    if (!csv && strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789", parse_options->delim[0]) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("KafkaFdw delimiter cannot be \"%s\"", parse_options->delim)));

    /* Check quote */
    if (!csv && parse_options->quote != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw quote available only in CSV mode")));

    if (csv && strlen(parse_options->quote) != 1)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw quote must be a single one-byte character")));

    if (csv && parse_options->delim[0] == parse_options->quote[0])
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("KafkaFdw delimiter and quote must be different")));

    /* Check escape */
    if (!csv && parse_options->escape != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw escape available only in CSV mode")));

    if (csv && strlen(parse_options->escape) != 1)
        ereport(
          ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("KafkaFdw escape must be a single one-byte character")));

    /* Don't allow the delimiter to appear in the null string. */
    if (strchr(parse_options->null_print, parse_options->delim[0]) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("KafkaFdw delimiter must not appear in the NULL specification")));

    /* Don't allow the CSV quote char to appear in the null string. */
    if (csv && strchr(parse_options->null_print, parse_options->quote[0]) != NULL)
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
 */
static void
KafkaProcessKafkaOptions(Oid relid, KafkaOptions *kafka_options, List *options)
{

    ListCell       *option;
    KafkaOptions    default_kafka_options = { .offset_attnum = -1,
                                              .partition_attnum = -1,
                                              .junk_attnum = -1 };

    /* Support external use for option sanity checking */
    if (kafka_options == NULL)
        kafka_options = &default_kafka_options;

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
        else if (strcmp(def->defname, "num_partitions") == 0)
        {
            kafka_options->num_partitions = strtol(defGetString(def), NULL, 10);
            if (kafka_options->num_partitions <= 1)
                ereport(
                  ERROR,
                  (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires an integer value bigger than 1", def->defname)));
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
    {
        get_kafka_fdw_attribute_options(relid, kafka_options);

        if (kafka_options->topic == NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("missing option \"topic\"")));
    }

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

#if PG_VERSION_NUM < 130000
    rel                          = heap_open(relid, AccessShareLock);
#else
    rel                          = relation_open(relid, AccessShareLock);
#endif
    tupleDesc                    = RelationGetDescr(rel);
    natts                        = tupleDesc->natts;
    kafka_options->num_parse_col = 0;

    /* Retrieve FDW options for all user-defined attributes. */
    for (attnum = 1; attnum <= natts; attnum++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum - 1);
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

#if PG_VERSION_NUM < 130000
    heap_close(rel, AccessShareLock);
#else
    relation_close(rel, AccessShareLock);
#endif
}
