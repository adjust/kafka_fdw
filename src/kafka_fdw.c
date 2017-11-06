#include "kafka_fdw.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

#define OPINT4EQ 96
#define OPINT84EQ 416
#define OPINT48EQ 416
#define OPINT8EQ 410

/*
 * FDW callback routines
 */
static void         kafkaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void         kafkaGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *kafkaGetForeignPlan(PlannerInfo *root,
                                        RelOptInfo * baserel,
                                        Oid          foreigntableid,
                                        ForeignPath *best_path,
                                        List *       tlist,
                                        List *       scan_clauses
#if PG_VERSION_NUM >= 90500
                                        ,
                                        Plan *outer_plan
#endif
);
static void            kafkaExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void            kafkaBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *kafkaIterateForeignScan(ForeignScanState *node);
static void            kafkaReScanForeignScan(ForeignScanState *node);
static void            kafkaEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static void kafkaGetOptions(Oid foreigntableid, KafkaOptions *kafka_options, ParseOptions *parse_options);
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel, KafkaFdwPlanState *fdw_private);
static bool check_selective_binary_conversion(RelOptInfo *baserel, Oid foreigntableid, List **columns);
static void estimate_costs(PlannerInfo *      root,
                           RelOptInfo *       baserel,
                           KafkaFdwPlanState *fdw_private,
                           Cost *             startup_cost,
                           Cost *             total_cost);
static void kafkaStop(KafkaFdwExecutionState *festate);
static void kafkaStart(KafkaFdwExecutionState *festate);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
PG_FUNCTION_INFO_V1(kafka_fdw_handler);
Datum kafka_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize   = kafkaGetForeignRelSize;
    fdwroutine->GetForeignPaths     = kafkaGetForeignPaths;
    fdwroutine->GetForeignPlan      = kafkaGetForeignPlan;
    fdwroutine->ExplainForeignScan  = kafkaExplainForeignScan;
    fdwroutine->BeginForeignScan    = kafkaBeginForeignScan;
    fdwroutine->IterateForeignScan  = kafkaIterateForeignScan;
    fdwroutine->ReScanForeignScan   = kafkaReScanForeignScan;
    fdwroutine->EndForeignScan      = kafkaEndForeignScan;
    fdwroutine->AnalyzeForeignTable = NULL; /* we don't analyze for now */
    /* making it parallelsafe seems hard but doable left out for now */
    // fdwroutine->IsForeignScanParallelSafe = topicIsForeignScanParallelSafe;

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Fetch the options for a kafka_fdw foreign table.
 *
 * We have to separate separete Kafka Options from csv parsing options
 */
static void
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
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel, KafkaFdwPlanState *fdw_private)
{
    double nrows;

    /* Estimate relation size we can't do better than hard code for now */
    fdw_private->ntuples = fdw_private->kafka_options.batch_size;

    /* now idea how to estimate number of pages */
    fdw_private->pages = fdw_private->ntuples / 3;

    /*
     * Now estimate the number of rows returned by the scan after applying the
     * baserestrictinfo quals.
     */
    nrows = fdw_private->ntuples * clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);

    nrows = clamp_row_est(nrows);

    /* Save the output-rows estimate for the planner */
    baserel->rows = nrows;
    baserel->rows = fdw_private->ntuples;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *      root,
               RelOptInfo *       baserel,
               KafkaFdwPlanState *fdw_private,
               Cost *             startup_cost,
               Cost *             total_cost)
{
    BlockNumber pages    = fdw_private->pages;
    double      ntuples  = fdw_private->ntuples;
    Cost        run_cost = 0;
    Cost        cpu_per_tuple;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan, to account
     * for the cost of parsing records.
     */
    run_cost += seq_page_cost * pages;

    *startup_cost = baserel->baserestrictcost.startup;
    cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;
    *total_cost = *startup_cost + run_cost;
}

/*
 * kafkaGetForeignRelSize
 *      Obtain relation size estimates for a foreign table
 */
static void
kafkaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

    KafkaFdwPlanState *fdw_private;

    /* Fetch options. */
    fdw_private = (KafkaFdwPlanState *) palloc0(sizeof(KafkaFdwPlanState));

    kafkaGetOptions(foreigntableid, &fdw_private->kafka_options, &fdw_private->parse_options);

    baserel->fdw_private = (void *) fdw_private;

    /* Estimate relation size */
    estimate_size(root, baserel, fdw_private);
}

/*
 * kafkaGetForeignPaths
 *      Create possible access paths for a scan on the kafka topic
 *
 *      We only push-down "offset" and "partition", an create only one
 *      possible access path, which simply returns all records of
 *      the kafka topic.
 */
static void
kafkaGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    KafkaFdwPlanState *fdw_private = (KafkaFdwPlanState *) baserel->fdw_private;
    Cost               startup_cost;
    Cost               total_cost;
    List *             columns;
    List *             options = NIL;
    TupleDesc          tupdesc;
    Relation           relation;
    List *             conditions = baserel->baserestrictinfo;
    ListCell *         lc;
    KafkaOptions *     kafka_options = &fdw_private->kafka_options;

    relation = relation_open(foreigntableid, AccessShareLock);
    tupdesc  = relation->rd_att;

    /*
     * write offset and partition to kafka_options.
     * general table options are already filled  during GetForeignRelSize
     */
    kafka_options->scan_params.offset_op = OP_INVALID;
    kafka_options->scan_params.partition = -1;
    kafka_options->scan_params.offset    = -1;

    foreach (lc, conditions)
    {
        kafkaParseExpression(kafka_options, ((RestrictInfo *) lfirst(lc))->clause);
    }

    if (kafka_options->scan_params.partition == -1 || kafka_options->scan_params.offset == -1)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("offset and partition must be set in WHERE clause")));
    }

    /* we pass the kafka and parse options for scanning */
    options = list_make2(kafka_options, &fdw_private->parse_options);

    /* Decide whether to selectively perform binary conversion */
    if (check_selective_binary_conversion(baserel, foreigntableid, &columns))
#if PG_VERSION_NUM >= 100000
        options = lappend(options, makeDefElem("convert_selectively", (Node *) columns, -1));
#else
        options = lappend(options, makeDefElem("convert_selectively", (Node *) columns));
#endif

    /* Estimate costs */
    estimate_costs(root, baserel, fdw_private, &startup_cost, &total_cost);

    relation_close(relation, AccessShareLock);
    /*
     * Create a ForeignPath node and add it as only possible path.  We use the
     * fdw_private list of the path to carry the convert_selectively option;
     * it will be propagated into the fdw_private list of the Plan node.
     */
    add_path(baserel,
             (Path *) create_foreignscan_path(root,
                                              baserel,
#if PG_VERSION_NUM >= 90600
                                              NULL, /* default pathtarget */
#endif
                                              baserel->rows,
                                              startup_cost,
                                              total_cost,
                                              NIL,  /* no pathkeys */
                                              NULL, /* no outer rel either */
#if PG_VERSION_NUM >= 90500
                                              NULL, /* no extra plan */
#endif
                                              options));
}

/*
 * kafkaGetForeignPlan
 *      Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
kafkaGetForeignPlan(PlannerInfo *root,
                    RelOptInfo * baserel,
                    Oid          foreigntableid,
                    ForeignPath *best_path,
                    List *       tlist,
                    List *       scan_clauses
#if PG_VERSION_NUM >= 90500
                    ,
                    Plan *outer_plan
#endif
)
{
    Index scan_relid = baserel->relid;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    // scan_clauses = extract_kafka_conditions(scan_clauses, plan_state->kafka_options);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL, /* no expressions to evaluate */
                            best_path->fdw_private
#if PG_VERSION_NUM >= 90500
                            ,
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */
                            outer_plan
#endif
    );
}

/* helper function to return a stringified version of scan params */
static char *
KafkaScanParamsToString(KafkaScanParams *param)
{
    StringInfoData buf;

    initStringInfo(&buf);
    appendStringInfo(&buf, "PARTITION = %d OFFSET %ld ", param->partition, param->offset);
    return buf.data;
}

/*
 * kafkaExplainForeignScan
 *      Produce extra output for EXPLAIN
 */
static void
kafkaExplainForeignScan(ForeignScanState *node, ExplainState *es)
{

    KafkaOptions kafka_options;
    ParseOptions parse_options;
    List *       fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    kafka_options            = *(KafkaOptions *) list_nth(fdw_private, 0);
    parse_options            = *(ParseOptions *) list_nth(fdw_private, 1);

    /* Fetch options --- we only need topic at this point */
    // kafkaGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &kafka_options, &parse_options);

    ExplainPropertyText("Kafka topic", kafka_options.topic, es);
    ExplainPropertyText("scanning", KafkaScanParamsToString(&kafka_options.scan_params), es);
}

/*
 kafkaBeginForeignScan
 *      Initiate access to the topic by creating festate
 */
static void
kafkaBeginForeignScan(ForeignScanState *node, int eflags)
{
    DEBUGLOG("%s", __func__);
    // ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    KafkaOptions            kafka_options = {};
    ParseOptions            parse_options = {};
    KafkaFdwExecutionState *festate;
    char                    errstr[KAFKA_MAX_ERR_MSG];
    TupleDesc               tupDesc;
    Form_pg_attribute *     attr;
    AttrNumber              num_phys_attrs;
    FmgrInfo *              in_functions;
    Oid *                   typioparams;
    int                     attnum;
    Oid                     in_func_oid;
    List *                  attnums     = NIL;
    List *                  fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    EState *                estate      = node->ss.ps.state;

    kafka_options = *(KafkaOptions *) list_nth(fdw_private, 0);
    parse_options = *(ParseOptions *) list_nth(fdw_private, 1);

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
        DEBUGLOG("explain only");
        return;
    }

    /* setup execution state */
    festate = (KafkaFdwExecutionState *) palloc0(sizeof(KafkaFdwExecutionState));

    festate->kafka_options = kafka_options;
    festate->parse_options = parse_options;
    /* initialize attribute buffer */
    initStringInfo(&festate->attribute_buf);

    tupDesc        = RelationGetDescr(node->ss.ss_currentRelation);
    attr           = tupDesc->attrs;
    num_phys_attrs = tupDesc->natts;

    /* allocate enough space for fields */
    festate->max_fields = num_phys_attrs;
    festate->raw_fields = palloc0(festate->max_fields * sizeof(char *));

    /*
     * Pick up the required catalog information for each attribute in the
     * relation, including the input function, the element type (to pass to
     * the input function), and info about defaults and constraints. (Which
     * input function we use depends on text/binary format choice.)
     */
    in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
    typioparams  = (Oid *) palloc(num_phys_attrs * sizeof(Oid));

    for (attnum = 1; attnum <= num_phys_attrs; attnum++)
    {
        /* We don't need info for dropped attributes */
        if (attr[attnum - 1]->attisdropped)
            continue;

        attnums = lappend_int(attnums, attnum);

        /* Fetch the input function and typioparam info */
        if (festate->parse_options.binary)
            getTypeBinaryInputInfo(attr[attnum - 1]->atttypid, &in_func_oid, &typioparams[attnum - 1]);
        else
            getTypeInputInfo(attr[attnum - 1]->atttypid, &in_func_oid, &typioparams[attnum - 1]);
        fmgr_info(in_func_oid, &in_functions[attnum - 1]);
    }

    /* We keep those variables in festate. */
    festate->in_functions = in_functions;
    festate->typioparams  = typioparams;
    festate->attnumlist   = attnums;

    node->fdw_state = (void *) festate;

    /*
     * Init Kafka-related stuff
     */

    /* Open connection if possible */
    if (festate->kafka_handle == NULL)
    {
        KafkaFdwGetConnection(festate, errstr);
    }
    if (festate->kafka_handle == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg_internal("kafka_fdw: Unable to connect to %s", kafka_options.brokers),
                 errdetail("%s", errstr)));
    }

    if (festate->kafka_topic_handle == NULL)
        ereport(
          ERROR,
          (errcode(ERRCODE_FDW_ERROR), errmsg_internal("kafka_fdw: Unable to create topic %s", kafka_options.topic)));

    // festate->buffer = palloc(sizeof(rd_kafka_message_t *) * (kafka_options.batch_size));
    festate->buffer = palloc0(sizeof(rd_kafka_message_t *) * (kafka_options.batch_size));

    kafkaStart(festate);
}

/*
 kafkaicIterateForeignScan
 *      Read next record from the kafka topic and store it into the
 *      ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
kafkaIterateForeignScan(ForeignScanState *node)
{
    // DEBUGLOG("%s", __func__);

    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;
    TupleTableSlot *        slot    = node->ss.ss_ScanTupleSlot;
    rd_kafka_message_t *    message;
    Datum *                 values;
    bool *                  nulls;
    int                     num_attrs, fldnum;
    ListCell *              cur;
    TupleDesc               tupDesc;
    Form_pg_attribute *     attr;

    /*
     * The protocol for loading a virtual tuple into a slot is first
     * ExecClearTuple, then fill the values/isnull arrays, then
     * ExecStoreVirtualTuple.  If we don't find another row in the topic, we
     * just skip the last step, leaving the slot empty as required.
     *
     * We can pass ExprContext = NULL because we read all columns from the
     * topic, so no need to evaluate default expressions.
     *
     * We can also pass tupleOid = NULL because we don't allow oids for
     * foreign tables.
     */
    ExecClearTuple(slot);

    /*
     * Request more messages
     * if we have already returned all the remaining ones
     */
    if (festate->buffer_cursor >= festate->buffer_count)
    {
        festate->buffer_count = rd_kafka_consume_batch(festate->kafka_topic_handle,
                                                       festate->kafka_options.scan_params.partition,
                                                       festate->kafka_options.buffer_delay,
                                                       festate->buffer,
                                                       festate->kafka_options.batch_size);

        if (festate->buffer_count == -1)
            ereport(
              ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg_internal("kafka_fdw got an error fetching data %s", rd_kafka_err2str(rd_kafka_last_error()))));

        festate->buffer_cursor = 0;
    }

    /* Still no data */
    if (festate->buffer_cursor >= festate->buffer_count)
        return slot;

    message = festate->buffer[festate->buffer_cursor];

    /* This also means there is no data */
    if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
    {
        DEBUGLOG("kafka_fdw has reached the end of the queue");
        return slot;
    }

    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw got an error %d when fetching a message from queue", message->err)));
    }

    tupDesc   = RelationGetDescr(node->ss.ss_currentRelation);
    attr      = tupDesc->attrs;
    num_attrs = list_length(festate->attnumlist);

    values = palloc(num_attrs * sizeof(Datum));
    nulls  = palloc(num_attrs * sizeof(bool));

    /* Initialize all values for row to NULL */
    MemSet(values, 0, num_attrs * sizeof(Datum));
    MemSet(nulls, false, num_attrs * sizeof(bool));

    int fldct;
    fldct = KafkaReadAttributesCSV(message->payload, message->len, festate);

    /* check that we have data for every column */
    if (fldct > list_length(festate->attnumlist))
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("extra data after last expected column")));

    /* Loop to read the user attributes on the line. */
    fldnum = 0;
    foreach (cur, festate->attnumlist)
    {
        int   attnum = lfirst_int(cur);
        int   m      = attnum - 1;
        char *string;
        if (fldnum >= fldct)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("missing data in Kafka Stream")));

        if (attnum == festate->kafka_options.partition_attnum)
            values[m] = Int32GetDatum(message->partition);
        else if (attnum == festate->kafka_options.offset_attnum)
            values[m] = Int64GetDatum(message->offset);
        else
        {
            string = festate->raw_fields[fldnum++];
            if (string == NULL)
                nulls[m] = true;
            else
                values[m] =
                  InputFunctionCall(&festate->in_functions[m], string, festate->typioparams[m], attr[m]->atttypmod);
        }
    }

    slot->tts_values = values;
    slot->tts_isnull = nulls;
    ExecStoreVirtualTuple(slot);

    rd_kafka_message_destroy(message);
    festate->buffer_cursor++;

    return slot;
}

/*
 * kafkaReScanForeignScan
 *      Rescan table, possibly with new parameters
 */
static void
kafkaReScanForeignScan(ForeignScanState *node)
{
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;

    kafkaStop(festate);
    kafkaStart(festate);
}

/*
 * kafkaEndForeignScan
 *      Finish scanning foreign table and dispose objects used for this scan
 */
static void
kafkaEndForeignScan(ForeignScanState *node)
{
    DEBUGLOG("%s", __func__);
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;

    /* if festate is NULL, we are in EXPLAIN; nothing to do */
    if (festate == NULL)
        return;

    /* Stop consuming */
    kafkaStop(festate);

    /* release unconsumed messages */
    while (festate->buffer_count > festate->buffer_cursor)
    {

        rd_kafka_message_t *message;
        message = festate->buffer[festate->buffer_cursor];

        rd_kafka_message_destroy(message);
        festate->buffer_cursor++;
    }

    // MemoryContextReset(festate->batch_cxt);
    kafkaCloseConnection(festate);

    pfree(festate->buffer);
}

/*
 * check_selective_binary_conversion
 *
 * Check to see if it's useful to convert only a subset of the topic's columns
 * to binary.  If so, construct a list of the column names to be converted,
 * return that at *columns, and return TRUE.  (Note that it's possible to
 * determine that no columns need be converted, for instance with a COUNT(*)
 * query.  So we can't use returning a NIL list to indicate failure.)
 */
static bool
check_selective_binary_conversion(RelOptInfo *baserel, Oid foreigntableid, List **columns)
{
    ForeignTable *table;
    ListCell *    lc;
    Relation      rel;
    TupleDesc     tupleDesc;
    AttrNumber    attnum;
    Bitmapset *   attrs_used   = NULL;
    bool          has_wholerow = false;
    int           numattrs;
    int           i;

    *columns = NIL; /* default result */

    /*
     * Check format of the topic.  If binary format, this is irrelevant.
     */
    table = GetForeignTable(foreigntableid);
    foreach (lc, table->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "format") == 0)
        {
            char *format = defGetString(def);

            if (strcmp(format, "binary") == 0)
                return false;
            break;
        }
    }

/* Collect all the attributes needed for joins or final output. */
#if PG_VERSION_NUM >= 90600
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &attrs_used);
#else
    pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &attrs_used);
#endif

    /* Add all the attributes used by restriction clauses. */
    foreach (lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause, baserel->relid, &attrs_used);
    }

    /* Convert attribute numbers to column names. */
    rel       = heap_open(foreigntableid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    while ((attnum = bms_first_member(attrs_used)) >= 0)
    {
        /* Adjust for system attributes. */
        attnum += FirstLowInvalidHeapAttributeNumber;

        if (attnum == 0)
        {
            has_wholerow = true;
            break;
        }

        /* Ignore system attributes. */
        if (attnum < 0)
            continue;

        /* Get user attributes. */
        if (attnum > 0)
        {
            Form_pg_attribute attr    = tupleDesc->attrs[attnum - 1];
            char *            attname = NameStr(attr->attname);

            /* Skip dropped attributes (probably shouldn't see any here). */
            if (attr->attisdropped)
                continue;
            *columns = lappend(*columns, makeString(pstrdup(attname)));
        }
    }

    /* Count non-dropped user attributes while we have the tupdesc. */
    numattrs = 0;
    for (i = 0; i < tupleDesc->natts; i++)
    {
        Form_pg_attribute attr = tupleDesc->attrs[i];

        if (attr->attisdropped)
            continue;
        numattrs++;
    }

    heap_close(rel, AccessShareLock);

    /* If there's a whole-row reference, fail: we need all the columns. */
    if (has_wholerow)
    {
        *columns = NIL;
        return false;
    }

    /* If all the user attributes are needed, fail. */
    if (numattrs == list_length(*columns))
    {
        *columns = NIL;
        return false;
    }

    return true;
}

static void
kafkaStop(KafkaFdwExecutionState *festate)
{
    if (rd_kafka_consume_stop(festate->kafka_topic_handle, festate->kafka_options.scan_params.partition) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Failed to stop consuming: %s", rd_kafka_err2str(err))));
    }
}
static void
kafkaStart(KafkaFdwExecutionState *festate)
{
    festate->buffer_count  = 0;
    festate->buffer_cursor = 0;
    /* Start consuming */
    if (rd_kafka_consume_start(festate->kafka_topic_handle,
                               festate->kafka_options.scan_params.partition,
                               festate->kafka_options.scan_params.offset) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Failed to start consuming: %s", rd_kafka_err2str(err))));
    }
}
