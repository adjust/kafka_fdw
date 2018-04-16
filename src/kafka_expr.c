#include "kafka_fdw.h"

#include "catalog/pg_operator.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define canHandleType(x) ((x) == INT8OID || (x) == INT4OID || (x) == INT2OID)

#define DatumGetIntArg(x, oid)                                                                                         \
    (oid == INT8OID ? DatumGetInt64(x) : oid == INT4OID ? DatumGetInt32(x) : DatumGetInt16(x))

typedef enum scanfield
{
    FIELD_INVALID = -1,
    FIELD_PARTITION,
    FIELD_OFFSET
} scanfield;

static bool  valid_scanop(KafkaScanOp *scan_op);
static List *append_scan_p(List *list, KafkaScanP *scan_p, int64 batch_size);

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

static void
set_scan_op(List *scan_list, kafka_op op, scanfield field, int64 val, ListCell *apply_from)
{
    ListCell *   lc;
    KafkaScanOp *scan_op;

    for_each_cell(lc, apply_from)
    {
        scan_op = (KafkaScanOp *) lfirst(lc);

        if (field == FIELD_PARTITION)
        {
            DEBUGLOG("set_scan_op part %ld", val);
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
                    scan_op->ph          = scan_op->ph_infinite ? val : min_int32(scan_op->ph, --val);
                    scan_op->ph_infinite = false;
                    break;
                case OP_LTE:
                    scan_op->ph          = scan_op->ph_infinite ? val : min_int32(scan_op->ph, val);
                    scan_op->ph_infinite = false;
                    break;
                default: break;
            }
        }
        else if (field == FIELD_OFFSET)
        {
            DEBUGLOG("set_scan_op offset %ld", val);
            switch (op)
            {
                case OP_GTE: scan_op->ol = max_int64(scan_op->ol, val); break;
                case OP_GT: scan_op->ol = max_int64(scan_op->ol, ++val); break;
                case OP_EQ:
                    scan_op->ol = val;
                    scan_op->oh = val;
                    break;
                case OP_LT:
                    scan_op->oh          = scan_op->oh_infinite ? val : min_int64(scan_op->oh, --val);
                    scan_op->oh_infinite = false;
                    break;
                case OP_LTE:
                    scan_op->oh          = scan_op->oh_infinite ? val : min_int64(scan_op->oh, val);
                    scan_op->oh_infinite = false;
                    break;
                default: break;
            }
        }
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

/*
    parse expressions to find relevant kafka scann infos
    we consider && and || OPERATOR expressions here
    if it's one of usefull (see opername_to_op)
    one side must be a kafka column (partition or offset)
    the other side a constant
    if we see a bool exp we scan recursively

    scan_list is what we parsed so far
    expr is the Expression that we parse now
    apply_from is the starting point of list from where we need to apply expr
*/

List *
kafkaParseExpression(List *scan_list, Expr *expr, int partition_attnum, int offset_attnum, ListCell *apply_from)
{
    DEBUGLOG("%s", __func__);

    HeapTuple        tuple;
    OpExpr *         oper;
    BoolExpr *       boolexpr;
    Form_pg_operator form;
    Oid              rightargtype, op_oid;
    int              op;
    Node *           varatt, *constatt, *left, *right;
    int              varattno;
    bool             need_commute = false; /* if we need to commute the operator */

    if (expr == NULL)
        return scan_list;

    switch (nodeTag(expr))
    {
        case T_OpExpr:

            oper = (OpExpr *) expr;

            left  = list_nth(oper->args, 0);
            right = list_nth(oper->args, 1);

            if (left == NULL)
            {
                DEBUGLOG("no left side parameter");
                return scan_list;
            }
            if (right == NULL)
            {
                DEBUGLOG("no right side parameter");
                return scan_list;
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
                return scan_list;

            /* check that it's the right column */
            varattno = (int) ((Var *) varatt)->varattno;
            if (varattno != partition_attnum && varattno != offset_attnum)
                return scan_list;
            /*
                check that the other side is a constant
                potentiallty we could also handel Parma i.e. parameterized queries
                but that's additional effort and left for now
            */

            if (IsA(left, Const))
                constatt = left; /* the constant */
            else if (IsA(right, Const))
                constatt = right; /* the constant */
            else
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("one side of operation must be a constant")));

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
            // DEBUGLOG(
            // "T_OpExpr %s, leftargtype: %d, rightargtype: %d", NameStr(form->oprname), form->oprleft, rightargtype);

            ReleaseSysCache(tuple);

            if (!canHandleType(rightargtype))
            {
                DEBUGLOG("can't handle type");
                return scan_list;
            }
            if (op == OP_INVALID)
                return scan_list;

            int64 val = DatumGetIntArg(((Const *) constatt)->constvalue, rightargtype);

            if (varattno == partition_attnum)
            {

                if (val >= INT32_MAX)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition number out of range")));

                set_scan_op(scan_list, op, FIELD_PARTITION, val, apply_from);
                break;
            }
            if (varattno == offset_attnum)
            {
                set_scan_op(scan_list, op, FIELD_OFFSET, val, apply_from);
                break;
            }

            break;

        case T_BoolExpr:
            boolexpr = (BoolExpr *) expr;
            // AND_EXPR, OR_EXPR, NOT_EXPR
            switch (boolexpr->boolop)
            {
                ListCell *cell;
                ListCell *tail;
                case AND_EXPR:
                    /*
                        all subexpressions need to apply from the current (outer) tail of the list
                    */
                    tail = list_tail(scan_list);
                    DEBUGLOG("AND EXPR");
                    foreach (cell, boolexpr->args)
                    {
                        scan_list =
                          kafkaParseExpression(scan_list, (Expr *) lfirst(cell), partition_attnum, offset_attnum, tail);
                    }
                    break;

                case OR_EXPR:
                    DEBUGLOG("OR EXPR");
                    foreach (cell, boolexpr->args)
                    {
                        /* we apply the expression to the current tail */
                        scan_list = kafkaParseExpression(
                          scan_list, (Expr *) lfirst(cell), partition_attnum, offset_attnum, list_tail(scan_list));
                        /* the otherside is appened to the list*/
                        if (lnext(cell) != NULL)
                            scan_list = lappend(scan_list, NewKafkaScanOp());
                    }
                    break;

                case NOT_EXPR: return scan_list; break;
            }
        default: return scan_list;
    }
    return scan_list;
}

static int
cmpfunc(const void *a, const void *b)
{
    return (*(int32 *) a - *(int32 *) b);
}

static bool
partion_member(KafKaPartitionList *partition_list, int32 search_partition)
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

List *
KafkaFlattenScanlist(List *scan_list, KafKaPartitionList *partition_list, int64 batch_size)
{
    ListCell *   lc;
    KafkaScanOp *scan_op;
    KafkaScanP * scan_p;
    List *       result = NIL;
    int32        p      = 0;
    int32        pl, ph;

    qsort(partition_list->partitions, partition_list->partition_cnt, sizeof(int32), cmpfunc);
    pl = partition_list->partitions[0];
    ph = partition_list->partitions[partition_list->partition_cnt - 1];

    foreach (lc, scan_list)
    {
        scan_op = (KafkaScanOp *) lfirst(lc);
        if (valid_scanop(scan_op))
        {
            for (p = max_int32(scan_op->pl, pl); p <= (scan_op->ph_infinite ? ph : min_int32(scan_op->ph, ph)); p++)
            {
                if (partion_member(partition_list, p))
                {
                    scan_p             = palloc(sizeof(KafkaScanP));
                    scan_p->partition  = p;
                    scan_p->offset     = scan_op->ol;
                    scan_p->offset_lim = scan_op->oh_infinite ? -1 : scan_op->oh;
                    result             = append_scan_p(result, scan_p, batch_size);
                }
            }
        }
    }

    return result;
}

static List *
append_scan_p(List *list, KafkaScanP *scan_p, int64 batch_size)
{
    ListCell *  lc;
    KafkaScanP *cur_scan_p;

    if (list == NIL)
        return lappend(list, scan_p);

    foreach (lc, list)
    {
        cur_scan_p = (KafkaScanP *) lfirst(lc);
        if (cur_scan_p->partition == scan_p->partition)
        {
            // expand if overlap
            // [a, b] overlaps with [x, y] if a <= y and x <= b.
            if ((cur_scan_p->offset <= scan_p->offset_lim + batch_size || scan_p->offset_lim == -1) &&
                (scan_p->offset <= cur_scan_p->offset_lim + batch_size || cur_scan_p->offset_lim == -1))
            {
                cur_scan_p->offset     = min_int64(cur_scan_p->offset, scan_p->offset);
                cur_scan_p->offset_lim = cur_scan_p->offset_lim == -1 || scan_p->offset_lim == -1
                                           ? -1
                                           : max_int64(cur_scan_p->offset_lim, scan_p->offset_lim);
                return list;
            }
        }
    }
    return lappend(list, scan_p);
}

static bool
valid_scanop(KafkaScanOp *scan_op)
{
    return (scan_op->ph_infinite || scan_op->pl <= scan_op->ph) && (scan_op->oh_infinite || scan_op->ol <= scan_op->oh);
}
