#include "kafka_fdw.h"

#include "catalog/pg_operator.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define canHandleType(x) ((x) == INT8OID || (x) == INT4OID || (x) == INT2OID)

#define DatumGetIntArg(x, oid)                                                                                         \
    (oid == INT8OID ? DatumGetInt64(x) : oid == INT4OID ? DatumGetInt32(x) : DatumGetInt16(x))

static int
opername_to_op(const char *op)
{
    if (strcmp(op, "=") == 0)
        return OP_EQ;
    if (strcmp(op, ">") == 0)
        return OP_GT;
    if (strcmp(op, ">=") == 0)
        return OP_GTE;
    /*
        in a future release we might consider properly handling
        of other operators as well for now we stop here
    */

    return OP_INVALID;

    if (strcmp(op, "<>") == 0)
        return OP_NEQ;
    if (strcmp(op, "<") == 0)
        return OP_LT;
    if (strcmp(op, "<=") == 0)
        return OP_LTE;
    if (strcmp(op, "@>") == 0)
        return OP_ARRAYELEMS;

    return OP_INVALID;
}

/*
    parse expressions to find relevant kafka scann infos
    we consider only BOOL and OPERATOR expressions here
    if it's one of usefull (see opername_to_op)
    one side must be a kafka column (partition or offset)
    the other side a constant
    if we see a bool exp we scan recursively
*/

bool
kafkaParseExpression(KafkaOptions *kafka_options, Expr *expr)
{

    HeapTuple        tuple;
    OpExpr *         oper;
    BoolExpr *       boolexpr;
    Form_pg_operator form;
    Oid              rightargtype, op_oid;
    int              op;
    int              partition_attnum = kafka_options->partition_attnum;
    int              offset_attnum    = kafka_options->offset_attnum;
    Node *           varatt, *constatt, *left, *right;
    int              varattno;
    bool             need_commute = false; /* if we need to commute the operator */

    if (expr == NULL)
        return false;

    switch (nodeTag(expr))
    {
        case T_OpExpr:

            oper = (OpExpr *) expr;

            left  = list_nth(oper->args, 0);
            right = list_nth(oper->args, 1);

            if (left == NULL)
            {
                DEBUGLOG("no left side parameter");
                return false;
            }
            if (right == NULL)
            {
                DEBUGLOG("no right side parameter");
                return false;
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
                return false;

            /* check that it's the right column */
            varattno = (int) ((Var *) varatt)->varattno;
            if (varattno != partition_attnum && varattno != offset_attnum)
                return false;
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
            DEBUGLOG(
              "T_OpExpr %s, leftargtype: %d, rightargtype: %d", NameStr(form->oprname), form->oprleft, rightargtype);

            ReleaseSysCache(tuple);

            if (!canHandleType(rightargtype))
            {
                DEBUGLOG("can't handle type");
                return false;
            }
            if (op == OP_INVALID)
                return false;

            if (varattno == partition_attnum)
            {

                int64 val = DatumGetIntArg(((Const *) constatt)->constvalue, rightargtype);
                if (val < 0)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition must be greater than 0")));
                if (val >= INT32_MAX)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition number out of range")));
                if (op != OP_EQ)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("partition must be set using equal (=)")));
                if (kafka_options->scan_params.partition > 0 && kafka_options->scan_params.partition != val)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("scanning of multiple partitions not allowed")));
                kafka_options->scan_params.partition = val;
                break;
            }
            if (varattno == offset_attnum)
            {
                int64 val = DatumGetIntArg(((Const *) constatt)->constvalue, rightargtype);

                // the first offset we found
                if (kafka_options->scan_params.offset_op == OP_INVALID)
                {
                    // rewrite to GTE
                    if (op == OP_GT)
                    {
                        kafka_options->scan_params.offset    = ++val;
                        kafka_options->scan_params.offset_op = OP_GTE;
                    }
                    else
                    {
                        kafka_options->scan_params.offset    = val;
                        kafka_options->scan_params.offset_op = op;
                    }
                    break;
                }

                if (kafka_options->scan_params.offset_op == OP_EQ && op == OP_EQ &&
                    val != kafka_options->scan_params.offset)
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("scanning of multiple offsets not allowed")));

                // we already found a GTE offset let's see if this one is even greater
                if (kafka_options->scan_params.offset_op == OP_GTE && val >= kafka_options->scan_params.offset)
                {
                    // rewrite to GTE
                    if (op == OP_GT)
                    {
                        kafka_options->scan_params.offset    = ++val;
                        kafka_options->scan_params.offset_op = OP_GTE;
                    }
                    else
                    {
                        kafka_options->scan_params.offset    = val;
                        kafka_options->scan_params.offset_op = op;
                    }
                    break;
                }
            }

            break;

        case T_BoolExpr:
            DEBUGLOG("BoolExpr");
            boolexpr = (BoolExpr *) expr;
            // AND_EXPR, OR_EXPR, NOT_EXPR
            switch (boolexpr->boolop)
            {
                ListCell *cell;
                case AND_EXPR:
                    DEBUGLOG("AND EXPR");
                    foreach (cell, boolexpr->args)
                    {
                        kafkaParseExpression(kafka_options, (Expr *) lfirst(cell));
                    }
                    break;

                case OR_EXPR:
                    DEBUGLOG("OR EXPR");
                    foreach (cell, boolexpr->args)
                    {
                        kafkaParseExpression(kafka_options, (Expr *) lfirst(cell));
                    }
                    break;

                case NOT_EXPR: return false; break;
            }
        default: return false;
    }
    return true;
}
