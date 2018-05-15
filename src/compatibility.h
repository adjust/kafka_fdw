/* macros to allow compatibility between different postgres versions */
#include "executor/executor.h"
#include "optimizer/pathnode.h"

#ifndef COMPATIBILITY
#define COMPATIBILITY

/* ExecEvalExpr accross different pg versions */
#if PG_VERSION_NUM >= 100000
#define KafkaExecEvalExpr(expr, econtext, isNull) (ExecEvalExpr(expr, econtext, isNull))
#else
#define KafkaExecEvalExpr(expr, econtext, isNull) (ExecEvalExpr(expr, econtext, isNull, NULL))
#endif

/* this is already defined in pg 10 */
#if PG_VERSION_NUM < 100000
List *ExecInitExprList(List *nodes, PlanState *parent);

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List *
ExecInitExprList(List *nodes, PlanState *parent)
{
    List *    result = NIL;
    ListCell *lc;

    foreach (lc, nodes)
    {
        Expr *e = lfirst(lc);

        result = lappend(result, ExecInitExpr(e, parent));
    }

    return result;
}
#endif

/*
    create_foreignscan_path signature has changed accros different pg versions
    kafka_create_foreignscan_path harmonizes it
*/
#if PG_VERSION_NUM >= 90600
#define kafka_create_foreignscan_path(planner_info,                                                                    \
                                      reloptinfo,                                                                      \
                                      pathtarget,                                                                      \
                                      rows,                                                                            \
                                      startup_cost,                                                                    \
                                      total_cost,                                                                      \
                                      pathkeys,                                                                        \
                                      req_outer,                                                                       \
                                      fdw_outerpath,                                                                   \
                                      fdw_private)                                                                     \
    (create_foreignscan_path(planner_info,                                                                             \
                             reloptinfo,                                                                               \
                             pathtarget,                                                                               \
                             rows,                                                                                     \
                             startup_cost,                                                                             \
                             total_cost,                                                                               \
                             pathkeys,                                                                                 \
                             req_outer,                                                                                \
                             fdw_outerpath,                                                                            \
                             fdw_private))

#else
#define kafka_create_foreignscan_path(planner_info,                                                                    \
                                      reloptinfo,                                                                      \
                                      pathtarget,                                                                      \
                                      rows,                                                                            \
                                      startup_cost,                                                                    \
                                      total_cost,                                                                      \
                                      pathkeys,                                                                        \
                                      req_outer,                                                                       \
                                      fdw_outerpath,                                                                   \
                                      fdw_private)                                                                     \
    (create_foreignscan_path(                                                                                          \
      planner_info, reloptinfo, rows, startup_cost, total_cost, pathkeys, req_outer, fdw_outerpath, fdw_private))
#endif
#endif
