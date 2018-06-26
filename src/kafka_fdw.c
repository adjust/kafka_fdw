#include "kafka_fdw.h"
#include "compatibility.h"
#include "parser/parsetree.h"
#include "storage/spin.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define MAX(_a, _b) ((_a > _b) ? _a : _b)

/*
 * FDW callback routines
 */
static void            kafkaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void            kafkaGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *   kafkaGetForeignPlan(PlannerInfo *root,
                                           RelOptInfo * baserel,
                                           Oid          foreigntableid,
                                           ForeignPath *best_path,
                                           List *       tlist,
                                           List *       scan_clauses,
                                           Plan *       outer_plan);
static void            kafkaExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void            kafkaBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *kafkaIterateForeignScan(ForeignScanState *node);
static void            kafkaReScanForeignScan(ForeignScanState *node);
static void            kafkaEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */

static bool kafkaStop(KafkaFdwExecutionState *festate);
static bool kafkaStart(KafkaFdwExecutionState *festate);
static bool kafkaNext(KafkaFdwExecutionState *festate);

static List *kafkaPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
static void  kafkaBeginForeignModify(ModifyTableState *mtstate,
                                     ResultRelInfo *   rinfo,
                                     List *            fdw_private,
                                     int               subplan_index,
                                     int               eflags);

static TupleTableSlot *kafkaExecForeignInsert(EState *        estate,
                                              ResultRelInfo * rinfo,
                                              TupleTableSlot *slot,
                                              TupleTableSlot *planSlot);

static void kafkaEndForeignModify(EState *estate, ResultRelInfo *rinfo);

static int kafkaIsForeignRelUpdatable(Relation rel);

static char *getJsonAttname(Form_pg_attribute attr, StringInfo buff);
static int   next_work(KafkaScanPData *scan_p, KafkaScanDataDesc *scand);

/* parallel execution */
#ifdef DO_PARALLEL
static bool kafkaIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static Size kafkaEstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt);
static void kafkaInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt, void *coordinate);
static void kafkaReInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt, void *coordinate);
static void kafkaInitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc, void *coordinate);
static void kafkaShutdownForeignScan(ForeignScanState *node);
static void kafkaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
#endif

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

    fdwroutine->AddForeignUpdateTargets = NULL;
    fdwroutine->PlanForeignModify       = kafkaPlanForeignModify;
    fdwroutine->BeginForeignModify      = kafkaBeginForeignModify;
    fdwroutine->EndForeignModify        = kafkaEndForeignModify;
    fdwroutine->ExecForeignInsert       = kafkaExecForeignInsert;
    fdwroutine->ExecForeignUpdate       = NULL;
    fdwroutine->ExecForeignDelete       = NULL;
    fdwroutine->IsForeignRelUpdatable   = kafkaIsForeignRelUpdatable;
#ifdef DO_PARALLEL
    fdwroutine->IsForeignScanParallelSafe   = kafkaIsForeignScanParallelSafe;
    fdwroutine->EstimateDSMForeignScan      = kafkaEstimateDSMForeignScan;
    fdwroutine->InitializeDSMForeignScan    = kafkaInitializeDSMForeignScan;
    fdwroutine->ReInitializeDSMForeignScan  = kafkaReInitializeDSMForeignScan;
    fdwroutine->InitializeWorkerForeignScan = kafkaInitializeWorkerForeignScan;
    fdwroutine->ShutdownForeignScan         = kafkaShutdownForeignScan;
#endif
    PG_RETURN_POINTER(fdwroutine);
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
    fdw_private                = (KafkaFdwPlanState *) palloc0(sizeof(KafkaFdwPlanState));
    fdw_private->kafka_options = (KafkaOptions){ DEFAULT_KAFKA_OPTIONS };

    kafkaGetOptions(foreigntableid, &fdw_private->kafka_options, &fdw_private->parse_options);

    baserel->fdw_private = (void *) fdw_private;

    /* Estimate relation size */
    KafkaEstimateSize(root, baserel, fdw_private);
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
    Cost               startup_cost, total_cost, run_cost;
    Relation           relation;
    ForeignPath *      foreign_path;
    int                num_workers;

    DEBUGLOG("%s", __func__);

#ifdef DO_PARALLEL
    num_workers = Min(fdw_private->npart - 1, max_parallel_workers_per_gather);
#else
    num_workers = 0;
#endif

    relation = relation_open(foreigntableid, AccessShareLock);

    /* Estimate costs */
    KafkaEstimateCosts(root, baserel, fdw_private, &startup_cost, &total_cost, &run_cost);

    relation_close(relation, AccessShareLock);
    /*
     * Create a ForeignPath node and add it as only possible path.  We use the
     * fdw_private list of the path to carry the convert_selectively option;
     * it will be propagated into the fdw_private list of the Plan node.
     */

    foreign_path = kafka_create_foreignscan_path(root,
                                                 baserel,
                                                 NULL, /* default pathtarget */
                                                 baserel->rows,
                                                 startup_cost,
                                                 total_cost,
                                                 NIL,  /* no pathkeys */
                                                 NULL, /* no outer rel either */
                                                 NULL, /* no extra plan */
                                                 NIL);
#ifdef DO_PARALLEL
    if (num_workers > 0)
    {
        KafkaSetParallelPath((Path *) foreign_path, num_workers, startup_cost, total_cost, run_cost);
        add_partial_path(baserel, (Path *) foreign_path);
        return;
    }
#endif

    add_path(baserel, (Path *) foreign_path);
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
                    List *       scan_clauses,
                    Plan *       outer_plan)
{
    ListCell *         lc;
    List *             scan_list, *scan_node_list, *param_list;
    KafkaFdwPlanState *fdw_private   = (KafkaFdwPlanState *) baserel->fdw_private;
    List *             options       = NIL;
    Index              scan_relid    = baserel->relid;
    KafkaOptions *     kafka_options = &fdw_private->kafka_options;

    DEBUGLOG("%s", __func__);
    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* parse filter conditions to scan kafka */
    scan_list = NIL;
    foreach (lc, scan_clauses)
    {
        scan_list = applyKafkaScanOpList(
          scan_list, dnfNorm(lfirst(lc), kafka_options->partition_attnum, kafka_options->offset_attnum));
    }
    /* an empty list evaluates to true (scan default all) */
    if (list_length(scan_list) == 0)
    {
        scan_list = lappend(scan_list, NewKafkaScanOp());
    }

    /*
     * convert the list to a list of list of const
     * this is neede as the fdw_private list must be copiable by copyObject()
     * see comment for ForeignScan node in plannodes.h
     */
    scan_node_list = NIL;
    param_list     = NIL;

    foreach (lc, scan_list)
    {
        scan_node_list = lappend(scan_node_list, KafkaScanOpToList((KafkaScanOp *) lfirst(lc)));
        param_list     = list_concat(param_list, (((KafkaScanOp *) lfirst(lc))->p_params));
        param_list     = list_concat(param_list, (((KafkaScanOp *) lfirst(lc))->o_params));

        DEBUGLOG("part_low %d, part_high %d, offset_low %ld, offset_high %ld, phi: %d, ohi: %d, part_param: %d, "
                 "offset_param %d",
                 ((KafkaScanOp *) lfirst(lc))->pl,
                 ((KafkaScanOp *) lfirst(lc))->ph,
                 ((KafkaScanOp *) lfirst(lc))->ol,
                 ((KafkaScanOp *) lfirst(lc))->oh,
                 ((KafkaScanOp *) lfirst(lc))->ph_infinite,
                 ((KafkaScanOp *) lfirst(lc))->oh_infinite,
                 list_length(((KafkaScanOp *) lfirst(lc))->p_params),
                 list_length(((KafkaScanOp *) lfirst(lc))->o_params));
    }

    /* we pass the scan_node_list for scanning */
    options = list_make1(scan_node_list);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            param_list,
                            options,
                            NIL, /* no custom tlist */
                            NIL, /* no remote quals */
                            outer_plan);
}

/* helper function to return a stringified version of scan params */
static char *
KafkaScanOpListToString(List *scanop)
{
    StringInfoData buf;

    initStringInfo(&buf);

    if (ScanopListGetPh(scanop) == ScanopListGetPl(scanop))
        appendStringInfo(&buf, "PARTITION = %d", ScanopListGetPl(scanop));
    else
    {
        appendStringInfo(&buf, "PARTITION >= %d", ScanopListGetPl(scanop));
        if (!ScanopListGetPhInvinite(scanop))
            appendStringInfo(&buf, " AND PARTITION <= %d", ScanopListGetPh(scanop));
    }

    if (ScanopListGetOh(scanop) == ScanopListGetOl(scanop))
        appendStringInfo(&buf, " AND OFFSET = %ld", ScanopListGetOl(scanop));
    else
    {
        appendStringInfo(&buf, " AND OFFSET >= %ld", ScanopListGetOl(scanop));
        if (!ScanopListGetOhInvinite(scanop))
            appendStringInfo(&buf, " AND OFFSET <= %ld", ScanopListGetOh(scanop));
    }

    return buf.data;
}

/*
 * kafkaExplainForeignScan
 *      Produce extra output for EXPLAIN
 */
static void
kafkaExplainForeignScan(ForeignScanState *node, ExplainState *es)
{

    ListCell *              lc;
    List *                  scanop;
    KafkaScanP *            p;
    KafkaFdwExecutionState *festate;
    int                     i             = 0;
    KafkaOptions            kafka_options = { DEFAULT_KAFKA_OPTIONS };
    List *                  fdw_private   = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    List *                  scan_list     = (List *) list_nth(fdw_private, 0);

    DEBUGLOG("%s", __func__);

    /* Fetch options --- we only need topic at this point */
    kafkaGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &kafka_options, NULL);

    ExplainPropertyText("Kafka topic", kafka_options.topic, es);
    if (es->analyze)
    {
        festate = (KafkaFdwExecutionState *) node->fdw_state;
        if (festate)
        {
            /* output the real work */
            for (i = 0; i < festate->scan_data->len; i++)
            {
                StringInfoData buf;
                initStringInfo(&buf);
                p = &festate->scan_data->data[i];
                DEBUGLOG("p %d, of %ld, ofl %ld", p->partition, p->offset, p->offset_lim);
                appendStringInfo(&buf, "PARTITION %d AND OFFSET >= %ld", p->partition, p->offset);

                if (p->offset_lim != -1)
                    appendStringInfo(&buf, " AND OFFSET <= %ld", p->offset_lim);

                ExplainPropertyText("scanning", buf.data, es);
            }
        }
    }
    else
    {
        foreach (lc, scan_list)
        {
            scanop = (List *) lfirst(lc);
            if (kafka_valid_scanop_list(scanop))
                ExplainPropertyText("scanning", KafkaScanOpListToString(scanop), es);
        }
    }
}

static KafkaScanPData *
makeScanPData(void)
{
    KafkaScanPData *res;
    res          = (KafkaScanPData *) palloc(sizeof(KafkaScanPData));
    res->data    = (KafkaScanP *) palloc(sizeof(KafkaScanP));
    res->len     = 0;
    res->max_len = 1;
    res->cursor  = 0;

    return res;
}

/*
 kafkaBeginForeignScan
 *      Initiate access to the topic by creating festate
 */
static void
kafkaBeginForeignScan(ForeignScanState *node, int eflags)
{
    ForeignScan *           plan          = (ForeignScan *) node->ss.ps.plan;
    KafkaOptions            kafka_options = { DEFAULT_KAFKA_OPTIONS };
    ParseOptions            parse_options = { .format = -1 };
    KafkaFdwExecutionState *festate;
    char                    errstr[KAFKA_MAX_ERR_MSG];
    TupleDesc               tupDesc;
    Form_pg_attribute *     attr;
    AttrNumber              num_phys_attrs;
    FmgrInfo *              in_functions;
    Oid *                   typioparams;
    int                     attnum;
    Oid                     in_func_oid;
    List *                  attnums = NIL;
    List *                  fdw_private;
    List *                  scan_list;
    // int                     nworkers, nworkers_launched;

    DEBUGLOG("%s", __func__);

    fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    scan_list   = (List *) list_nth(fdw_private, 0);

    kafkaGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &kafka_options, &parse_options);

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
        DEBUGLOG("explain only");
        return;
    }

    /* setup execution state */
    festate               = (KafkaFdwExecutionState *) palloc0(sizeof(KafkaFdwExecutionState));
    node->fdw_state       = (void *) festate;
    festate->param_values = NULL;
    festate->scan_data    = makeScanPData();

    /* we we get a parallel scan_data_desc will point to a shared mem segment by InitializeDSMForeignScan */
    festate->scan_data_desc              = (KafkaScanDataDesc *) palloc0(sizeof(KafkaScanDataDesc));
#ifdef DO_PARALLEL
    pg_atomic_init_u32(&festate->scan_data_desc->next_scanp, 0);
#else
    festate->scan_data_desc->next_scanp  = 0;
#endif
    festate->kafka_options = kafka_options;
    festate->parse_options = parse_options;

    /* initialize attribute buffer for user in iterate*/
    initStringInfo(&festate->attribute_buf);

    /* when we have junk field we also need junk_buf */
    if (kafka_options.junk_error_attnum != -1)
        initStringInfo(&festate->junk_buf);

    tupDesc        = RelationGetDescr(node->ss.ss_currentRelation);
    attr           = tupDesc->attrs;
    num_phys_attrs = tupDesc->natts;

    /* allocate enough space for fields */
    festate->max_fields = num_phys_attrs;
    festate->raw_fields = palloc0(num_phys_attrs * sizeof(char *));

    /* if we use json we need attnames */
    if (parse_options.format == JSON)
    {
        initStringInfo(&festate->attname_buf);
        festate->attnames = palloc0(num_phys_attrs * sizeof(char *));
    }

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
        getTypeInputInfo(attr[attnum - 1]->atttypid, &in_func_oid, &typioparams[attnum - 1]);
        fmgr_info(in_func_oid, &in_functions[attnum - 1]);

        if (parse_options.format == JSON)
            festate->attnames[attnum - 1] = getJsonAttname(attr[attnum - 1], &festate->attname_buf);
    }

    /* We keep those variables in festate. */
    festate->in_functions = in_functions;
    festate->typioparams  = typioparams;
    festate->attnumlist   = attnums;

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

    festate->scanop_list = scan_list;
    festate->buffer      = palloc0(sizeof(rd_kafka_message_t *) * (kafka_options.batch_size));

    /*
     * Prepare parameters that need to be extracted from psql query.
     */

    if (list_length(plan->fdw_exprs) > 0)
    {
        ListCell *lc;
        Param *   p;
        int       i = 0;

        festate->exec_exprs   = ExecInitExprList(plan->fdw_exprs, (PlanState *) node);
        festate->param_values = palloc0(list_length(plan->fdw_exprs) * sizeof(KafkaParamValue));

        foreach (lc, plan->fdw_exprs)
        {
            p                                = (Param *) lfirst(lc);
            festate->param_values[i].paramid = p->paramid;
            festate->param_values[i].oid     = p->paramtype;
            i++;
        }
    }
    else
    {
        festate->param_values = NULL;
        festate->exec_exprs   = NIL;
    }
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

    KafkaFdwExecutionState *festate  = (KafkaFdwExecutionState *) node->fdw_state;
    ExprContext *           econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot *        slot     = node->ss.ss_ScanTupleSlot;
    rd_kafka_message_t *    message;
    Datum *                 values;
    bool *                  nulls;
    int                     num_attrs, fldnum;
    ListCell *              cur;
    TupleDesc               tupDesc;
    Form_pg_attribute *     attr;
    KafkaOptions *          kafka_options = &festate->kafka_options;
    ParseOptions *          parse_options = &festate->parse_options;
    bool                    catched_error = false;
    bool                    ignore_junk   = kafka_options->ignore_junk;
    MemoryContext           ccxt          = CurrentMemoryContext;
    KafkaScanDataDesc *     scand         = festate->scan_data_desc;
    bool                    error         = false;
    int                     fldct, param_num = 0;
    KafkaScanP *            scan_p;

    /* first run eval expressions and setup working list */
    if (festate->scan_data->len == 0)
    {
        DEBUGLOG("%s", __func__);

        /*
         * if we got parameters we evaluate them now
         * we do so in the short lived per tuple context to avoid any leaking
         */
        if (list_length(festate->exec_exprs) > 0)
        {
            MemoryContext oldcontext;
            ListCell *    lc_exp;
            oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

            foreach (lc_exp, festate->exec_exprs)
            {
                festate->param_values[param_num].value =
                  KafkaExecEvalExpr((ExprState *) lfirst(lc_exp), econtext, &festate->param_values[param_num].is_null);
                param_num++;
            }
            MemoryContextSwitchTo(oldcontext);
        }

        KafkaFlattenScanlist(festate->scanop_list,
                             festate->partition_list,
                             kafka_options->batch_size,
                             festate->param_values,
                             param_num,
                             festate->scan_data);

        /*
         * grap the next work item
         * note in case of parallel scan this isn't nessecarely the first one
         */

        festate->scan_data->cursor = next_work(festate->scan_data, scand);

        kafkaStart(festate);
    }

    if (kafka_options->junk_error_attnum != -1)
        resetStringInfo(&festate->junk_buf);

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

    /* get a message from the buffer if available */
    if (festate->buffer_cursor < festate->buffer_count)
    {
        message = festate->buffer[festate->buffer_cursor];
        /*
         * if the message gotten is the last one,
         * we iterate onto the next partition  and run into the while loop below
         */
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
        {
            DEBUGLOG("kafka_fdw has reached the end of the queue 1");
            if (!kafkaNext(festate))
                return slot;
        }
        else if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg_internal("kafka_fdw got an error %s when fetching a message from queue",
                                     rd_kafka_err2str(message->err))));
        }

        /*
         * if requested offset high is not infinite
         * check that we did not run over requested offset
         */

        if (festate->scan_data->cursor >= 0)
        {
            scan_p = (KafkaScanP *) &festate->scan_data->data[festate->scan_data->cursor];
            // list is done we're finished //
            if (scan_p->offset_lim >= 0 && scan_p->offset_lim < message->offset)
            {
                DEBUGLOG("kafka_fdw has reached the end of requested offset in queue");
                if (!kafkaNext(festate))
                    return slot;
            }
        }
    }

    /*
     * Request more messages
     * if we have already returned all the remaining ones
     */
    while (festate->buffer_cursor >= festate->buffer_count)
    {

        if (festate->scan_data->cursor == -1)
        { /* list is done we're finished */
            DEBUGLOG("done scanning");
            return slot;
        }

        scan_p = &festate->scan_data->data[festate->scan_data->cursor];

        DEBUGLOG("start consume");
        festate->buffer_count = rd_kafka_consume_batch(festate->kafka_topic_handle,
                                                       scan_p->partition,
                                                       kafka_options->buffer_delay,
                                                       festate->buffer,
                                                       kafka_options->batch_size);
        DEBUGLOG("done consume %zd", festate->buffer_count);
        festate->buffer_cursor = 0;

        if (festate->buffer_count == -1)
            ereport(
              ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg_internal("kafka_fdw got an error fetching data %s", rd_kafka_err2str(rd_kafka_last_error()))));

        if (festate->buffer_count <= 0) /* no more messages within timeout*/
        {
            if (!kafkaNext(festate))
                return slot;
        }
        else
        {
            message = festate->buffer[festate->buffer_cursor];
            if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
                DEBUGLOG("kafka_fdw has reached the end of the queue 2");
                if (!kafkaNext(festate))
                    return slot;
            }
            else if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_ERROR),
                         errmsg_internal("kafka_fdw got an error %s when fetching a message from queue",
                                         rd_kafka_err2str(message->err))));
            }
        }
    }

    tupDesc   = RelationGetDescr(node->ss.ss_currentRelation);
    attr      = tupDesc->attrs;
    num_attrs = list_length(festate->attnumlist);

    values = palloc0(num_attrs * sizeof(Datum));
    nulls  = palloc0(num_attrs * sizeof(bool));

    // DEBUGLOG("message: %s", message->payload);

    fldct = KafkaReadAttributes(message->payload, message->len, festate, parse_options->format, &error);

    /* unterminated quote, total junk */
    if (error && parse_options->format == CSV)
    {
        if (kafka_options->strict)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unterminated CSV quoted field")));
        else
        {
            catched_error = true;
            if (kafka_options->junk_error_attnum != -1)
                appendStringInfoString(&festate->junk_buf, "unterminated CSV quoted field");
            MemSet(nulls, true, num_attrs);
        }
    }
    else if (error && parse_options->format == JSON)
    {
        catched_error = true;
        MemSet(nulls, true, num_attrs);
    }
    /* to much data */
    else if (fldct > kafka_options->num_parse_col)
    {
        if (kafka_options->strict)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("extra data after last expected column")));

        if (ignore_junk)
        {
            catched_error = true;
            if (kafka_options->junk_error_attnum != -1)
                appendStringInfoString(&festate->junk_buf, "extra data after last expected column");
        }
    }
    /* to less data*/
    else if (fldct < kafka_options->num_parse_col)
    {
        if (kafka_options->strict)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("missing data in Kafka Stream")));

        if (ignore_junk)
        {
            catched_error = true;
            if (kafka_options->junk_error_attnum != -1)
                appendStringInfoString(&festate->junk_buf, "missing data in Kafka Stream");
        }
    }

    /* Loop to read the user attributes on the line. */
    fldnum = 0;
    foreach (cur, festate->attnumlist)
    {
        int   attnum = lfirst_int(cur);
        int   m      = attnum - 1;
        char *string;

        if (attnum == kafka_options->junk_attnum || attnum == kafka_options->junk_error_attnum)
        {
            nulls[m] = true;
            continue;
        }

        if (attnum == kafka_options->partition_attnum)
        {
            values[m] = Int32GetDatum(message->partition);
            nulls[m]  = false;
            continue;
        }
        if (attnum == kafka_options->offset_attnum)
        {
            values[m] = Int64GetDatum(message->offset);
            nulls[m]  = false;
            continue;
        }
        if (fldnum >= fldct)
        {
            nulls[m] = true;
            continue;
        }
        string = festate->raw_fields[fldnum++];

        if (string == NULL)
        {
            nulls[m] = true;
            continue;
        }
        if (ignore_junk)
        {
            PG_TRY();
            {
                values[m] =
                  InputFunctionCall(&festate->in_functions[m], string, festate->typioparams[m], attr[m]->atttypmod);
                nulls[m] = false;
            }
            PG_CATCH();
            {
                values[m]     = (Datum) 0;
                nulls[m]      = true;
                catched_error = true;

                MemoryContextSwitchTo(ccxt);

                /* accumulate errors if needed */
                if (kafka_options->junk_error_attnum != -1)
                {
                    ErrorData *errdata = CopyErrorData();

                    if (festate->junk_buf.len > 0)
                        appendStringInfoCharMacro(&festate->junk_buf, '\n');

                    appendStringInfoString(&festate->junk_buf, errdata->message);
                }
                FlushErrorState();
            }
            PG_END_TRY();
            continue;
        }

        values[m] = InputFunctionCall(&festate->in_functions[m], string, festate->typioparams[m], attr[m]->atttypmod);
        nulls[m]  = false;
    }

    if (catched_error)
    {
        if (kafka_options->junk_attnum != -1)
        {
            values[kafka_options->junk_attnum - 1] = PointerGetDatum(cstring_to_text(message->payload));
            nulls[kafka_options->junk_attnum - 1]  = false;
        }
        if (kafka_options->junk_error_attnum != -1)
        {
            values[kafka_options->junk_error_attnum - 1] = PointerGetDatum(cstring_to_text(festate->junk_buf.data));
            nulls[kafka_options->junk_error_attnum - 1]  = false;
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
    festate->scan_data->cursor = 0;
    kafkaStart(festate);
}

/*
 * kafkaEndForeignScan
 *      Finish scanning foreign table and dispose objects used for this scan
 */
static void
kafkaEndForeignScan(ForeignScanState *node)
{
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;

    DEBUGLOG("%s", __func__);

    /* if festate is NULL, we are in EXPLAIN; nothing to do */
    if (festate == NULL)
        return;

    /* Stop consuming */
    kafkaStop(festate);

    // MemoryContextReset(festate->batch_cxt);
    kafkaCloseConnection(festate);

    pfree(festate->buffer);
}

static bool
kafkaNext(KafkaFdwExecutionState *festate)
{
    if (!kafkaStop(festate))
        return false;
    if (!kafkaStart(festate))
        return false;

    return true;
}

static int
next_work(KafkaScanPData *scan_p, KafkaScanDataDesc *scand)
{
    int next;

    if (scand == NULL)
        return -1;

#ifdef DO_PARALLEL
    next = pg_atomic_fetch_add_u32(&scand->next_scanp, 1);
#else
    next = scand->next_scanp++;
#endif

    if (next >= scan_p->len)
        return -1;

    return next;
}

static bool
kafkaStop(KafkaFdwExecutionState *festate)
{

    KafkaScanP *       scan_p;
    KafkaScanDataDesc *scand = festate->scan_data_desc;

    DEBUGLOG("%s", __func__);
    if (festate->scan_data->cursor == -1)
        return false;

    scan_p = &festate->scan_data->data[festate->scan_data->cursor];

    if (rd_kafka_consume_stop(festate->kafka_topic_handle, scan_p->partition) == -1)
    {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Failed to stop consuming: %s", rd_kafka_err2str(err))));
    }

    /* release unconsumed messages */
    while (festate->buffer_count > festate->buffer_cursor)
    {

        rd_kafka_message_t *message;
        message = festate->buffer[festate->buffer_cursor];

        rd_kafka_message_destroy(message);
        festate->buffer_cursor++;
    }

    festate->scan_data->cursor = next_work(festate->scan_data, scand);

    if (festate->scan_data->cursor >= 0)
        return true;
    else
        return false;
}
static bool
kafkaStart(KafkaFdwExecutionState *festate)
{
    rd_kafka_resp_err_t err;
    KafkaScanP *        scan_p;
    int64_t             low, high;

    festate->buffer_count  = 0;
    festate->buffer_cursor = 0;

    DEBUGLOG("%s", __func__);

    if (festate->scan_data->cursor == -1)
        return false;

    scan_p = &festate->scan_data->data[festate->scan_data->cursor];

    err = rd_kafka_query_watermark_offsets(
      festate->kafka_handle, festate->kafka_options.topic, scan_p->partition, &low, &high, WARTERMARK_TIMEOUT);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Failed to get watermarks %s", rd_kafka_err2str(err))));

    DEBUGLOG("%s part: %d, offs: %lld (%ld / %lld), topic: %s",
             __func__,
             scan_p->partition,
             MAX(low, scan_p->offset),
             scan_p->offset,
             low,
             festate->kafka_options.topic);

    /* Start consuming */
    if (rd_kafka_consume_start(festate->kafka_topic_handle, scan_p->partition, MAX(low, scan_p->offset)) == -1)
    {
        err = rd_kafka_last_error();
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Failed to start consuming: %s", rd_kafka_err2str(err))));
    }
    return true;
}

/* operations for inserts */

static int
kafkaIsForeignRelUpdatable(Relation rel)
{
    return (1 << CMD_INSERT);
}

static List *
kafkaPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{

    Relation       rel;
    TupleDesc      tupdesc;
    int            attnum;
    List *         targetAttrs   = NIL;
    List *         returningList = NIL;
    RangeTblEntry *rte           = planner_rt_fetch(resultRelation, root);

    rel     = heap_open(rte->relid, NoLock);
    tupdesc = RelationGetDescr(rel);

    for (attnum = 1; attnum <= tupdesc->natts; attnum++)
    {
        Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

        if (!attr->attisdropped)
            targetAttrs = lappend_int(targetAttrs, attnum);
    }

    /*
     * Extract the relevant RETURNING list if any.
     */
    if (plan->returningLists)
        returningList = (List *) list_nth(plan->returningLists, subplan_index);

    if (plan->onConflictAction)
        elog(ERROR, "unexpected ON CONFLICT specification: %d", (int) plan->onConflictAction);

    heap_close(rel, NoLock);

    /*
     * Build the fdw_private list that will be available to the executor.
     * Items in the list must match enum FdwModifyPrivateIndex, above.
     */
    return list_make2(targetAttrs, returningList);
}

/*
 * Begin executing a foreign table modification operation. This routine is
 * called during executor startup. It should perform any initialization
 * needed prior to the actual table modifications. Subsequently,
 * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
 * called for each tuple to be inserted, updated, or deleted.
 *
 * mtstate is the overall state of the ModifyTable plan node being
 * executed; global data about the plan and execution state is available
 * via this structure. rinfo is the ResultRelInfo struct describing the
 * target foreign table. (The ri_FdwState field of ResultRelInfo is
 * available for the FDW to store any private state it needs for this
 * operation.) fdw_private contains the private data generated by
 * PlanForeignModify, if any. subplan_index identifies which target of the
 * ModifyTable plan node this is. eflags contains flag bits describing the
 * executor's operating mode for this plan node.
 *
 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
 * should not perform any externally-visible actions; it should only do
 * the minimum required to make the node state valid for
 * ExplainForeignModify and EndForeignModify.
 *
 * If the BeginForeignModify pointer is set to NULL, no action is taken
 * during executor startup.
 */

static void
kafkaBeginForeignModify(ModifyTableState *mtstate,
                        ResultRelInfo *   rinfo,
                        List *            fdw_private,
                        int               subplan_index,
                        int               eflags)
{
    KafkaFdwModifyState *festate;
    rd_kafka_t *         rk;
    rd_kafka_topic_t *   rkt;
    rd_kafka_conf_t *    conf;
    Oid                  typefnoid;
    bool                 isvarlena;
    int                  n_params, num = 0;
    ListCell *           lc, *prev, *next;
    KafkaOptions         kafka_options = { DEFAULT_KAFKA_OPTIONS };
    ParseOptions         parse_options = { .format = -1 };
    Relation             rel           = rinfo->ri_RelationDesc;
    char                 errstr[512]; /* librdkafka API error reporting buffer */

    DEBUGLOG("%s", __func__);

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    {
        DEBUGLOG("explain only");
        return;
    }
    kafkaGetOptions(RelationGetRelid(rel), &kafka_options, &parse_options);

    /* setup execution state */
    festate                = (KafkaFdwModifyState *) palloc0(sizeof(KafkaFdwModifyState));
    festate->kafka_options = kafka_options;
    festate->parse_options = parse_options;

    festate->attnumlist    = (List *) list_nth(fdw_private, 0);
    n_params               = list_length(festate->attnumlist);
    festate->out_functions = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);

    /* if we use json we need attnames and oids */
    if (parse_options.format == JSON)
    {
        initStringInfo(&festate->attname_buf);
        festate->attnames    = palloc0(sizeof(char *) * n_params);
        festate->typioparams = (Oid *) palloc(n_params * sizeof(Oid));
    }

    initStringInfo(&festate->attribute_buf);

    prev = NULL;
    for (lc = list_head(festate->attnumlist); lc; lc = next)
    {
        int attnum = lfirst_int(lc);
        next       = lnext(lc);

        if (!parsable_attnum(attnum, kafka_options))
        {
            festate->attnumlist = list_delete_cell(festate->attnumlist, lc, prev);
        }
        else
        {

            Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
            Assert(!attr->attisdropped);

            getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
            fmgr_info(typefnoid, &festate->out_functions[num]);

            if (parse_options.format == JSON)
            {
                festate->attnames[num]    = getJsonAttname(attr, &festate->attname_buf);
                festate->typioparams[num] = attr->atttypid;
                DEBUGLOG("type oid %u", attr->atttypid);
            }

            num++;
            prev = lc;
        }
    }

    conf = rd_kafka_conf_new();

    /* Set bootstrap broker(s) as a comma-separated list of
     * host or host:port (default port 9092).
     * librdkafka will use the bootstrap brokers to acquire the full
     * set of brokers from the cluster. */
    if (rd_kafka_conf_set(conf, "bootstrap.servers", kafka_options.brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        elog(ERROR, "%s\n", errstr);
    }
    /* Producer config */
    /*
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000", NULL, 0);
    rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
    rd_kafka_conf_set(conf, "retry.backoff.ms", "500", NULL, 0);
    */

    /*
     * Create producer instance.
     *
     * NOTE: rd_kafka_new() takes ownership of the conf object
     *       and the application must not reference it again after
     *       this call.
     */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        elog(ERROR, "%% Failed to create new producer: %s\n", errstr);
    }

    /* Create topic object that will be reused for each message
     * produced.
     *
     * Both the producer instance (rd_kafka_t) and topic objects (topic_t)
     * are long-lived objects that should be reused as much as possible.
     */
    rkt = rd_kafka_topic_new(rk, kafka_options.topic, NULL);
    if (!rkt)
    {
        elog(ERROR, "%% Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
    }

    festate->kafka_topic_handle = rkt;
    festate->kafka_handle       = rk;

    rinfo->ri_FdwState = festate;
}

/*
 * Insert one tuple into the foreign table. estate is global execution
 * state for the query. rinfo is the ResultRelInfo struct describing the
 * target foreign table. slot contains the tuple to be inserted; it will
 * match the rowtype definition of the foreign table. planSlot contains
 * the tuple that was generated by the ModifyTable plan node's subplan; it
 * differs from slot in possibly containing additional "junk" columns.
 * (The planSlot is typically of little interest for INSERT cases, but is
 * provided for completeness.)
 *
 * The return value is either a slot containing the data that was actually
 * inserted (this might differ from the data supplied, for example as a
 * result of trigger actions), or NULL if no row was actually inserted
 * (again, typically as a result of triggers). The passed-in slot can be
 * re-used for this purpose.
 *
 * The data in the returned slot is used only if the INSERT query has a
 * RETURNING clause. Hence, the FDW could choose to optimize away
 * returning some or all columns depending on the contents of the
 * RETURNING clause. However, some slot must be returned to indicate
 * success, or the query's reported rowcount will be wrong.
 *
 * If the ExecForeignInsert pointer is set to NULL, attempts to insert
 * into the foreign table will fail with an error message.
 *
 */
static TupleTableSlot *
kafkaExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    KafkaFdwModifyState *festate   = (KafkaFdwModifyState *) rinfo->ri_FdwState;
    int                  partition = RD_KAFKA_PARTITION_UA;
    int                  ret;
    Datum                value;
    bool                 isnull;

    DEBUGLOG("%s", __func__);

    resetStringInfo(&festate->attribute_buf);
    if (slot != NULL && festate->attnumlist != NIL)
    {
        KafkaWriteAttributes(festate, slot, festate->parse_options.format);
    }

    /* fetch partition if given */
    value = slot_getattr(slot, festate->kafka_options.partition_attnum, &isnull);
    if (!isnull)
        partition = DatumGetInt32(value);

    DEBUGLOG("Message: %s", festate->attribute_buf.data);

retry:

    ret = rd_kafka_produce(festate->kafka_topic_handle,
                           partition,
                           RD_KAFKA_MSG_F_COPY,         // Make a copy of the payload.
                           festate->attribute_buf.data, // Message payload (value) and length
                           festate->attribute_buf.len,
                           NULL, // Optional key and its length
                           0,    // and its length
                           NULL  // Message opaque, provided in  delivery report callback as* msg_opaque.
    );
    if (ret != 0)
    {

        /* Poll to handle delivery reports */
        if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL)
        {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            rd_kafka_poll(festate->kafka_handle, 1000 /*block for max 1000ms*/);
            goto retry;
        }

        elog(ERROR,
             "%% Failed to produce to topic %s: %s\n",
             rd_kafka_topic_name(festate->kafka_topic_handle),
             rd_kafka_err2str(rd_kafka_last_error()));
    }

    rd_kafka_poll(festate->kafka_handle, 0 /*non-blocking*/);

    return slot;
}

static void
kafkaEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
    KafkaFdwModifyState *festate = (KafkaFdwModifyState *) rinfo->ri_FdwState;

    rd_kafka_flush(festate->kafka_handle, 10 * 1000 /* wait for max 10 seconds */);

    /* Destroy topic object */
    rd_kafka_topic_destroy(festate->kafka_topic_handle);

    /* Destroy the producer instance */
    rd_kafka_destroy(festate->kafka_handle);
}

/*
    appends the attributes json option to buff and
    returns a pointer to it
    if no such option is found attributes attname is used
*/
static char *
getJsonAttname(Form_pg_attribute attr, StringInfo buff)
{
    List *    options;
    ListCell *lc;
    int       cur_start = buff->len == 0 ? 0 : buff->len + 1;

    if (buff->len != 0)
        appendStringInfoChar(buff, '\0');

    options = GetForeignColumnOptions(attr->attrelid, attr->attnum);
    foreach (lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "json") == 0)
        {
            appendStringInfoString(buff, defGetString(def));
            return &buff->data[cur_start];
        }
    }

    /* if we are here we did not find a json def so use attname */
    appendStringInfoString(buff, NameStr(attr->attname));

    return &buff->data[cur_start];
}

#ifdef DO_PARALLEL
static bool
kafkaIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    DEBUGLOG("%s", __func__);
    return true;
}

static Size
kafkaEstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
    DEBUGLOG("%s", __func__);
    return sizeof(KafkaScanDataDesc);
}

static void
kafkaInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt, void *coordinate)
{
    KafkaScanDataDesc *     scand   = (KafkaScanDataDesc *) coordinate;
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;

    scand->ps_relid                 = RelationGetRelid(node->ss.ss_currentRelation);
    pg_atomic_write_u32(&scand->next_scanp, 0);
    festate->scan_data_desc = scand;
}

static void
kafkaReInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt, void *coordinate)
{
    KafkaScanDataDesc *scand = (KafkaScanDataDesc *) coordinate;

    pg_atomic_write_u32(&scand->next_scanp, 0);
}

static void
kafkaInitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc, void *coordinate)
{
    KafkaScanDataDesc *     scand   = (KafkaScanDataDesc *) coordinate;
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;
    festate->scan_data_desc         = scand;
}

static void
kafkaShutdownForeignScan(ForeignScanState *node)
{
    KafkaFdwExecutionState *festate = (KafkaFdwExecutionState *) node->fdw_state;
    festate->scan_data_desc         = NULL;
}
#endif
