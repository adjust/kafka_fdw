/*
TODO consider and simplyfy
SELECT * FROM kafka_test_part where (part between 1 and 2 and offs > 100) OR (part between 1 and 2 and offs > 200);
SELECT * FROM kafka_test_part where (part between 3 and 2 and offs > 100) ;
*/

#include "kafka_fdw.h"

#include "catalog/pg_operator.h"
#include "optimizer/clauses.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= 120000
#include "nodes/nodeFuncs.h"
#else
#define is_orclause(expr) \
    or_clause((Node *) (expr))

#define is_andclause(expr) \
    and_clause((Node *) (expr))
#endif

#define canHandleType(x) ((x) == INT8OID || (x) == INT4OID || (x) == INT2OID)

#ifndef PG_INT64_MAX
#define PG_INT64_MAX INT64CONST(0x7FFFFFFFFFFFFFFF)
#endif

#define DatumGetIntArg(x, oid)                                                                                         \
    (oid == INT8OID ? DatumGetInt64(x) : oid == INT4OID ? DatumGetInt32(x) : DatumGetInt16(x))

typedef enum scanfield
{
    FIELD_INVALID = -1,
    FIELD_PARTITION,
    FIELD_OFFSET
} scanfield;

enum low_high
{
    LOW,
    HIGH
};

static void append_scan_p(KafkaScanPData *scand, KafkaScanP scan_p, int64 batch_size);

static KafkaScanOp *applyOperator(OpExpr *oper, int partition_attnum, int offset_attnum);
static KafkaScanOp *getKafkaScanOp(kafka_op op, scanfield field, Node *val_p);
static KafkaScanOp *intersectKafkaScanOp(KafkaScanOp *a, KafkaScanOp *b);

KafkaScanOp *
NewKafkaScanOp()
{
    KafkaScanOp *scan_op;

    scan_op = (KafkaScanOp *) palloc(sizeof(*scan_op));

    scan_op->pl          = 0;
    scan_op->ph          = -1;
    scan_op->ol          = 0;
    scan_op->oh          = -1;
    scan_op->oh_infinite = true;
    scan_op->ph_infinite = true;
    scan_op->p_params    = NIL;
    scan_op->o_params    = NIL;
    scan_op->p_param_ops = NIL;
    scan_op->o_param_ops = NIL;

    return scan_op;
}

static inline int32
max_int32(int32 a, int32 b)
{
    int32 res = a > b ? a : b;

    return res;
}

static inline int64
max_int64(int64 a, int64 b)
{
    int64 res = a > b ? a : b;

    return res;
}

static inline int32
min_int32(int32 a, int32 b)
{
    int32 res = a < b ? a : b;

    return res;
}

static inline int64
min_int64(int64 a, int64 b)
{
    int64 res = a < b ? a : b;

    return res;
}

static int64
get_int_const_val(Const *node)
{
    switch (node->consttype)
    {
        case INT8OID: return DatumGetInt64(node->constvalue);
        case INT4OID: return DatumGetInt32(node->constvalue);
        case INT2OID: return DatumGetInt16(node->constvalue);
        default: ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("only integer values are allowed")));
    }
}

static int
opername_to_op(const char *op)
{
    if (strcmp(op, "=") == 0)
        return OP_EQ;
    if (strcmp(op, ">") == 0)
        return OP_GT;
    if (strcmp(op, ">=") == 0)
        return OP_GTE;

    if (strcmp(op, "<") == 0)
        return OP_LT;
    if (strcmp(op, "<=") == 0)
        return OP_LTE;

    /*
        in a future release we might consider properly handling
        of other operators as well for now we stop here
    */
    return OP_INVALID;

    if (strcmp(op, "<>") == 0)
        return OP_NEQ;
    if (strcmp(op, "@>") == 0)
        return OP_ARRAYELEMS;

    return OP_INVALID;
}

List *
KafkaScanOpToList(KafkaScanOp *scan_op)
{
    List *res;

    res = list_make4(
      (void *) makeConst(INT8OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(scan_op->pl), false, true),
      (void *) makeConst(
        INT8OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(scan_op->ph), scan_op->ph_infinite, true),
      (void *) makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(scan_op->ol), false, FLOAT8PASSBYVAL),
      (void *) makeConst(
        INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(scan_op->oh), scan_op->oh_infinite, FLOAT8PASSBYVAL));

    res = lappend(res, parmaListToParmaId(scan_op->p_params));
    res = lappend(res, parmaListToParmaId(scan_op->o_params));
    res = lappend(res, scan_op->p_param_ops);
    res = lappend(res, scan_op->o_param_ops);
    return res;
}

List *
parmaListToParmaId(List *input)
{
    List *    res = NIL;
    ListCell *lc;
    Param *   p;
    foreach (lc, input)
    {
        p   = lfirst(lc);
        res = lappend_int(res, p->paramid);
    }
    return res;
}

static int
cmpfunc(const void *a, const void *b)
{
    return (*(int32 *) a - *(int32 *) b);
}

KafkaPartitionList *
getPartitionList(rd_kafka_t *kafka_handle,
                 rd_kafka_topic_t *kafka_topic_handle)
{
    KafkaPartitionList     *partition_list = NULL;
    rd_kafka_resp_err_t     err;
    const struct rd_kafka_metadata       *metadata;
    const struct rd_kafka_metadata_topic *topic;
    int     i;

    /* Fetch metadata */
    err = rd_kafka_metadata(kafka_handle, 0, kafka_topic_handle, &metadata, 5000);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        elog(ERROR, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));

    if (metadata->topic_cnt != 1)
    {
        rd_kafka_metadata_destroy(metadata);
        elog(ERROR, "%% Surprisingly got %d topics while 1 was expected", metadata->topic_cnt);
    }

    topic = &metadata->topics[0];

    /* Get partitions list for topic */
    partition_list = palloc0(offsetof(KafkaPartitionList, partitions) +
                             topic->partition_cnt * sizeof(int32));
    partition_list->partition_cnt = topic->partition_cnt;

    for (i = 0; i < partition_list->partition_cnt; i++)
        partition_list->partitions[i] = topic->partitions[i].id;

    rd_kafka_metadata_destroy(metadata);

    return partition_list;
}

static bool
partion_member(KafkaPartitionList *partition_list, int32 search_partition)
{
    int32 first, last, middle;
    first  = 0;
    last   = partition_list->partition_cnt;
    middle = (first + last) / 2;

    while (first <= last)
    {
        if (partition_list->partitions[middle] < search_partition)
            first = middle + 1;
        else if (partition_list->partitions[middle] == search_partition)
        {
            return true;
        }
        else
            last = middle - 1;

        middle = (first + last) / 2;
    }

    return false;
}

static int64
get_offset(List *           param_id_list,
           List *           param_op_list,
           KafkaParamValue *param_values,
           int64            initial,
           int              num_params,
           enum low_high    low_high,
           bool *           isnull)
{
    ListCell *lc_id, *lc_op;
    int       id, i = 0;
    int64     val;
    kafka_op  op;
    *isnull = false;

    forboth(lc_id, param_id_list, lc_op, param_op_list)
    {
        id = lfirst_int(lc_id);
        op = lfirst_int(lc_op);

        if (low_high == LOW && (op == OP_LT || op == OP_LTE))
            continue;
        if (low_high == HIGH && (op == OP_GT || op == OP_GTE))
            continue;

        for (i = 0; i < num_params; i++)
        {
            if (param_values[i].paramid == id)
            {
                if (param_values[i].is_null)
                {
                    /* the whole thing is invalid*/
                    *isnull = true;
                    return -1;
                }

                switch (param_values[i].oid)
                {
                    case INT8OID: val = DatumGetInt64(param_values[i].value); break;
                    case INT4OID: val = DatumGetInt32(param_values[i].value); break;
                    case INT2OID: val = DatumGetInt16(param_values[i].value); break;
                    default: elog(ERROR, "invalid paramtype %d", param_values[i].paramid);
                }

                if (op == OP_GT)
                    ++val;

                if (op == OP_LT)
                    --val;

                if (low_high == LOW)
                    initial = max_int64(initial, val);
                else
                    initial = min_int64(initial, val);
            }
        }
    }

    return initial;
}

/*
 * get the high/low partition number for given parameters and operatoer
 *  param_id_list is an inter list of parameter ids (aka the column  number)
 * param_op_list is the to be applied operater (typically > < =)
 */
static int32
get_partition(List *           param_id_list,
              List *           param_op_list,
              KafkaParamValue *param_values,
              int32            initial,
              int              num_params,
              enum low_high    low_high,
              bool *           isnull)
{
    ListCell *lc_id, *lc_op;
    int       id, i = 0;
    int64     val;
    kafka_op  op;
    *isnull = false;

    /*
     * loop through both lists find column and operator and evaluate
     * the parameter as needed
     * given low_high we return min/max
     */
    forboth(lc_id, param_id_list, lc_op, param_op_list)
    {
        id = lfirst_int(lc_id);
        op = lfirst_int(lc_op);

        for (i = 0; i < num_params; i++)
        {
            /*
             * Don't care for < and <= operator while considering lower
             * bound and the same thing with >/>= and upper bound
             */
            if (low_high == LOW && (op == OP_LT || op == OP_LTE))
                continue;
            if (low_high == HIGH && (op == OP_GT || op == OP_GTE))
                continue;

            if (param_values[i].paramid == id)
            {
                if (param_values[i].is_null)
                {
                    /* the whole thing is invalid*/
                    *isnull = true;
                    return -1;
                }

                switch (param_values[i].oid)
                {
                    case INT8OID: val = DatumGetInt64(param_values[i].value); break;
                    case INT4OID: val = DatumGetInt32(param_values[i].value); break;
                    case INT2OID: val = DatumGetInt16(param_values[i].value); break;
                    default: elog(ERROR, "invalid paramtype %d", param_values[i].paramid);
                }

                if (op == OP_GT)
                    ++val;

                if (op == OP_LT)
                    --val;

                if (val >= INT32_MAX)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition number out of range")));
                /* if we are looking for the low water mark we need max otherwisee min*/
                if (low_high == LOW)
                    initial = max_int64(initial, val);
                else
                    initial = min_int64(initial, val);
            }
        }
    }

    return initial;
}

/*
    create a list of KafkaScanP as a to scan list for kafka
    scan_list is a List of List of Const Nodes created during planning
    we go through each element of that list and check if we need to expand
    and existing item in the result list or append to it
*/
void
KafkaFlattenScanlist(List *              scan_list,
                     KafkaPartitionList *partition_list,
                     int64               batch_size,
                     KafkaParamValue *   param_values,
                     int                 num_params,
                     KafkaScanPData *    scanp_data)
{
    ListCell *lc;
    List *    scan_op_list;
    int32     p = 0;
    int32     lowest_p, highest_p;

    qsort(partition_list->partitions, partition_list->partition_cnt, sizeof(int32), cmpfunc);
    lowest_p  = partition_list->partitions[0];
    highest_p = partition_list->partitions[partition_list->partition_cnt - 1];

    DEBUGLOG("PARTITION LIST LOW %d, HIGH %d, LENGTH %d", lowest_p, highest_p, partition_list->partition_cnt);

    foreach (lc, scan_list)
    {
        int32 pl, ph;
        int64 ol, oh;
        bool  ph_infinite, oh_infinite, isnull = false;

        scan_op_list = (List *) lfirst(lc);
        /* deparse scan_op_list */

        pl          = ScanopListGetPl(scan_op_list);
        ph          = ScanopListGetPh(scan_op_list);
        ol          = ScanopListGetOl(scan_op_list);
        oh          = ScanopListGetOh(scan_op_list);
        ph_infinite = ScanopListGetPhInvinite(scan_op_list);
        oh_infinite = ScanopListGetOhInvinite(scan_op_list);
        ph          = ph_infinite ? highest_p : ph;
        oh          = oh_infinite ? PG_INT64_MAX : oh;

        /* if we have any params evaluate them */
        if (num_params > 0)
        {
            pl = get_partition(list_nth(scan_op_list, PartitionParamId),
                               list_nth(scan_op_list, PartitionParamOP),
                               param_values,
                               pl,
                               num_params,
                               LOW,
                               &isnull);
            if (isnull)
                continue;

            ph = get_partition(list_nth(scan_op_list, PartitionParamId),
                               list_nth(scan_op_list, PartitionParamOP),
                               param_values,
                               ph_infinite ? highest_p : ph,
                               num_params,
                               HIGH,
                               &isnull);
            if (isnull)
                continue;

            ol = get_offset(list_nth(scan_op_list, OffsetParamId),
                            list_nth(scan_op_list, OffsetParamOP),
                            param_values,
                            ol,
                            num_params,
                            LOW,
                            &isnull);

            if (isnull)
                continue;

            oh = get_offset(list_nth(scan_op_list, OffsetParamId),
                            list_nth(scan_op_list, OffsetParamOP),
                            param_values,
                            oh_infinite ? PG_INT64_MAX : oh,
                            num_params,
                            HIGH,
                            &isnull);

            if (isnull)
                continue;
        }

        /* if what we got makes any sense we create a KafkaScanP item per partition needed */
        if (pl <= ph && ol <= oh)
        {
            for (p = max_int32(pl, lowest_p); p <= ph; p++)
            {
                if (partion_member(partition_list, p))
                {
                    KafkaScanP scan_p = { .partition = p, .offset = ol, .offset_lim = (oh == PG_INT64_MAX ? -1 : oh) };
                    append_scan_p(scanp_data, scan_p, batch_size);
                }
            }
        }
    }
}

static void
appendKafkaScanPData(KafkaScanPData *scanp_data, KafkaScanP scan_p)
{
    int newlen;
    DEBUGLOG("%s ", __func__);

    /* enlarge if needed */
    if (scanp_data->len + 1 >= scanp_data->max_len)
    {
        newlen              = 2 * scanp_data->max_len;
        scanp_data->data    = (KafkaScanP *) repalloc(scanp_data->data, sizeof(KafkaScanP) * newlen);
        scanp_data->max_len = newlen;
    }
    scanp_data->data[scanp_data->len].partition  = scan_p.partition;
    scanp_data->data[scanp_data->len].offset     = scan_p.offset;
    scanp_data->data[scanp_data->len].offset_lim = scan_p.offset_lim;
    ++scanp_data->len;
}

/*
 * takes a list of KafkaScanP and
 * either expands an existing element with scan_p in case of overlap
 * or appends scan_p to the list
 */
static void
append_scan_p(KafkaScanPData *scanp_data, KafkaScanP scan_p, int64 batch_size)
{
    KafkaScanP *cur_scan_p;
    int         i = 0;

    if (scanp_data->len == 0)
    {
        appendKafkaScanPData(scanp_data, scan_p);
        return;
    }

    for (i = 0; i < scanp_data->len; i++)
    {
        cur_scan_p = &scanp_data->data[i];
        if (cur_scan_p->partition == scan_p.partition)
        {
            // expand if overlap
            // [a, b] overlaps with [x, y] if a <= y and x <= b.
            if ((cur_scan_p->offset <= scan_p.offset_lim + batch_size || scan_p.offset_lim == -1) &&
                (scan_p.offset <= cur_scan_p->offset_lim + batch_size || cur_scan_p->offset_lim == -1))
            {
                cur_scan_p->offset     = min_int64(cur_scan_p->offset, scan_p.offset);
                cur_scan_p->offset_lim = cur_scan_p->offset_lim == -1 || scan_p.offset_lim == -1
                                           ? -1
                                           : max_int64(cur_scan_p->offset_lim, scan_p.offset_lim);
                return;
            }
        }
    }

    // we did not return in the expand case thus append
    appendKafkaScanPData(scanp_data, scan_p);
}

bool
kafka_valid_scanop_list(List *scan_op_list)
{
    int32 pl, ph;
    int64 ol, oh;
    bool  ph_infinite, oh_infinite;

    pl          = ScanopListGetPl(scan_op_list);
    ph          = ScanopListGetPh(scan_op_list);
    ol          = ScanopListGetOl(scan_op_list);
    oh          = ScanopListGetOh(scan_op_list);
    ph_infinite = ScanopListGetPhInvinite(scan_op_list);
    oh_infinite = ScanopListGetOhInvinite(scan_op_list);

    DEBUGLOG("FOUND pl %d, ph %d, ol %ld, oh %ld, pli %d, ohi %d", pl, ph, ol, oh, ph_infinite, oh_infinite);

    return (ph_infinite || pl <= ph) && (oh_infinite || ol <= oh);
}
/*
    build a List of KafkaScanops for expr
    this basically builds an or_list
*/
List *
dnfNorm(Expr *expr, int partition_attnum, int offset_attnum)
{
    List *result;

    DEBUGLOG("%s", __func__);

    if (expr == NULL)
        return NIL;

    if (is_orclause(expr))
    {
        List *    orlist = NIL;
        ListCell *temp;
        foreach (temp, ((BoolExpr *) expr)->args)
        {
            Expr *arg = (Expr *) lfirst(temp);

            // orlist=lappend(orlist, dnfNorm(arg,partition_attnum,offset_attnum));
            orlist = list_concat(orlist, dnfNorm(arg, partition_attnum, offset_attnum));
        }
        return orlist;
    }
    else if (is_andclause(expr))
    {
        List *    andlist = NIL;
        ListCell *temp;

        /* Recurse */
        foreach (temp, ((BoolExpr *) expr)->args)
        {
            Expr *arg = (Expr *) lfirst(temp);
            andlist   = applyKafkaScanOpList(andlist, dnfNorm(arg, partition_attnum, offset_attnum));
        }
        return andlist;
    }

    if (nodeTag(expr) == T_OpExpr)
    {
        KafkaScanOp *scan_op = applyOperator((OpExpr *) expr, partition_attnum, offset_attnum);
        if (scan_op != NULL)
        {
            result = list_make1(scan_op);
            return result;
        }
    }

    return NIL;
}

/*
    parse operatore expressions to find relevant kafka scann infos
    and return a KafkaScanOp
    one side must be a kafka column (partition or offset)
    the other side a constant or parameter
*/

static KafkaScanOp *
applyOperator(OpExpr *oper, int partition_attnum, int offset_attnum)
{
    Node *           varatt, *valatt, *left, *right;
    bool             need_commute = false;
    int              varattno;
    Oid              rightargtype, op_oid;
    int              op;
    Form_pg_operator form;
    HeapTuple        tuple;
    if (list_length(oper->args) != 2)
        return NULL;

    left  = list_nth(oper->args, 0);
    right = list_nth(oper->args, 1);

    if (left == NULL)
    {
        DEBUGLOG("no left side parameter");
        return NULL;
    }
    if (right == NULL)
    {
        DEBUGLOG("no right side parameter");
        return NULL;
    }

    /* find wich part is the column */
    if (IsA(left, Var))
        varatt = left; /* the column */
    else if (IsA(right, Var))
    {
        varatt       = right; /* the column */
        need_commute = true;
    }
    else
        return NULL;

    /* check that it's the right column */
    varattno = (int) ((Var *) varatt)->varattno;
    if (varattno != partition_attnum && varattno != offset_attnum)
        return NULL;

    /*
        check that the other side is a constant or a param
    */
    if (need_commute && (IsA(left, Const) || IsA(left, Param)))
    {
        valatt = left; /* the value */
    }
    else if (!need_commute && (IsA(right, Const) || IsA(right, Param)))
    {
        valatt = right;
    }
    else
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("one side of operation must be a constant or param")));
    }

    /* commute if needed */
    if (need_commute)
        op_oid = get_commutator(oper->opno);
    else
        op_oid = oper->opno;

    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(op_oid));

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for operator %u", op_oid);
    form         = (Form_pg_operator) GETSTRUCT(tuple);
    rightargtype = form->oprright;
    op           = opername_to_op(NameStr(form->oprname));

    ReleaseSysCache(tuple);

    if (!canHandleType(rightargtype))
    {
        DEBUGLOG("can't handle type");
        return NULL;
    }
    if (op == OP_INVALID)
        return NULL;

    if (varattno == partition_attnum)
    {
        return getKafkaScanOp(op, FIELD_PARTITION, valatt);
    }
    else if (varattno == offset_attnum)
    {
        return getKafkaScanOp(op, FIELD_OFFSET, valatt);
    }

    return NULL;
}

/*
    create a KafkaScanOp for a given operator, kafkafield and value parameter
    if the parameter is a constand we apply directly param nodes are added to
    the relevant list for later porcesssing
*/
static KafkaScanOp *
getKafkaScanOp(kafka_op op, scanfield field, Node *val_p)
{
    KafkaScanOp *scan_op = NewKafkaScanOp();
    int64        val;

    if (field == FIELD_PARTITION)
    {
        /* const values can be applied and simplified directly */
        if (IsA(val_p, Const))
        {
            val = get_int_const_val((Const *) val_p);
            DEBUGLOG("set_scan_op part %ld", val);

            if (val >= INT32_MAX)
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition number out of range")));

            switch (op)
            {
                case OP_GTE: scan_op->pl = max_int32(scan_op->pl, val); break;
                case OP_GT: scan_op->pl = max_int32(scan_op->pl, ++val); break;
                case OP_EQ:
                    scan_op->pl          = val;
                    scan_op->ph          = val;
                    scan_op->ph_infinite = false;
                    break;
                case OP_LT:
                    /* if the high watermark is set we take the min otherwise the val */
                    scan_op->ph          = --val;
                    scan_op->ph_infinite = false;
                    break;
                case OP_LTE:
                    scan_op->ph          = val;
                    scan_op->ph_infinite = false;
                    break;
                default: break;
            }
        }
        else if (IsA(val_p, Param))
        {
            scan_op->p_params    = lappend(scan_op->p_params, val_p);
            scan_op->p_param_ops = lappend_int(scan_op->p_param_ops, op);
        }
        else
        {
            /* should not happen */
            elog(ERROR, "unexpected node type");
        }
    }
    else if (field == FIELD_OFFSET)
    {
        if (IsA(val_p, Const))
        {
            val = get_int_const_val((Const *) val_p);
            DEBUGLOG("set_scan_op offset %ld", val);
            switch (op)
            {
                case OP_GTE: scan_op->ol = max_int64(scan_op->ol, val); break;
                case OP_GT: scan_op->ol = max_int64(scan_op->ol, ++val); break;
                case OP_EQ:
                    scan_op->ol          = val;
                    scan_op->oh          = val;
                    scan_op->oh_infinite = false;
                    break;
                case OP_LT:
                    scan_op->oh          = --val;
                    scan_op->oh_infinite = false;
                    break;
                case OP_LTE:
                    scan_op->oh          = val;
                    scan_op->oh_infinite = false;
                    break;
                default: break;
            }
        }
        else if (IsA(val_p, Param))
        {
            scan_op->o_params    = lappend(scan_op->o_params, val_p);
            scan_op->o_param_ops = lappend_int(scan_op->o_param_ops, op);
        }
        else
        {
            /* should not happen */
            elog(ERROR, "unexpected node type");
        }
    }
    return scan_op;
}

/*
    applies one List of KafkaScanOps to another one
    building the cartesian product
*/

List *
applyKafkaScanOpList(List *a, List *b)
{
    ListCell *   lca, *lcb;
    List *       result = NIL;
    KafkaScanOp *scan_op;

    if (a == NIL)
    {
        return b;
    }
    if (b == NIL)
    {
        return a;
    }

    foreach (lca, a)
    {
        foreach (lcb, b)
        {
            KafkaScanOp *sca = (KafkaScanOp *) lfirst(lca);
            KafkaScanOp *scb = (KafkaScanOp *) lfirst(lcb);
            scan_op          = intersectKafkaScanOp(sca, scb);
            if (scan_op != NULL)
                result = lappend(result, scan_op);
        }
    }
    return result;
}

/*
    given two KafkaScanOp a and b
    return the intersection of both or NULL if empty
*/
static KafkaScanOp *
intersectKafkaScanOp(KafkaScanOp *a, KafkaScanOp *b)
{
    KafkaScanOp *result;
    if (a == NULL)
        return b;
    if (b == NULL)
        return a;

    result = NewKafkaScanOp();

    result->pl          = max_int32(a->pl, b->pl);
    result->ph          = b->ph_infinite ? a->ph : (a->ph_infinite ? b->ph : min_int32(a->ph, b->ph));
    result->ph_infinite = a->ph_infinite && b->ph_infinite;
    result->p_params    = list_concat(list_copy(a->p_params), b->p_params);
    result->p_param_ops = list_concat(list_copy(a->p_param_ops), b->p_param_ops);
    result->ol          = max_int64(a->ol, b->ol);
    result->oh          = b->oh_infinite ? a->oh : (a->oh_infinite ? b->oh : min_int64(a->oh, b->oh));
    result->oh_infinite = a->oh_infinite && b->oh_infinite;
    result->o_params    = list_concat(list_copy(a->o_params), b->o_params);
    result->o_param_ops = list_concat(list_copy(a->o_param_ops), b->o_param_ops);

    if ((result->ph_infinite || result->pl <= result->ph) && (result->oh_infinite || result->ol <= result->oh))
        return result;

    return NULL;
}
