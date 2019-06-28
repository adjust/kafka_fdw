#include "kafka_fdw.h"

#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#endif


extern double kafka_tuple_cost;


/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
void
KafkaEstimateSize(PlannerInfo *root, RelOptInfo *baserel, KafkaFdwPlanState *fdw_private)
{
    double nrows = 0,
           nbatches = 0;
    int    npart = 0;

    ListCell *    lc;
    List *        scan_list     = NIL;
    KafkaOptions *kafka_options = &fdw_private->kafka_options;
    int           highest_p     = kafka_options->num_partitions - 1;

    /* parse filter conditions to scan kafka */
    scan_list = NIL;
    foreach (lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        scan_list = applyKafkaScanOpList(
          scan_list, dnfNorm(rinfo->clause, kafka_options->partition_attnum, kafka_options->offset_attnum));
    }
    /* an empty list evaluates to true (scan default all) */
    if (list_length(scan_list) == 0)
    {
        scan_list = lappend(scan_list, NewKafkaScanOp());
    }

    /* Use statistics if we have it */
    if (baserel->tuples)
    {
        /*
         * Now estimate the number of rows returned by the scan after applying the
         * baserestrictinfo quals.
         */
        nrows = baserel->tuples *
            clauselist_selectivity(root,
                                   baserel->baserestrictinfo,
                                   0,
                                   JOIN_INNER,
                                   NULL);
        npart = kafka_options->num_partitions;  /* can't get it from statistics */
    }
    /* No statistics, try to guess */
    else
    {
        foreach (lc, scan_list)
        {
            int          nt, np;
            KafkaScanOp *scan_op = (KafkaScanOp *) lfirst(lc);
            np                   = (scan_op->ph_infinite ? highest_p : scan_op->ph) - scan_op->pl + 1;
            nt                   = scan_op->oh_infinite ? 10000 : (scan_op->oh - scan_op->ol + 1);
            npart += np;
            nrows += np * nt;
        }
    }

    nrows = clamp_row_est(nrows);
    nbatches = nrows / kafka_options->batch_size;

    fdw_private->ntuples = (int) nrows;
    fdw_private->npart = npart;
    fdw_private->nbatches = nbatches;

    /* Save the output-rows estimate for the planner */
    DEBUGLOG("estimated nrows %f", nrows);
    DEBUGLOG("estimated ntuples %d", fdw_private->ntuples);
    DEBUGLOG("estimated npart %d", fdw_private->npart);
    baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
void
KafkaEstimateCosts(PlannerInfo *      root,
                   RelOptInfo *       baserel,
                   KafkaFdwPlanState *fdw_private,
                   Cost *             startup_cost,
                   Cost *             total_cost,
                   Cost *             run_cost)
{
    double nbatches = fdw_private->nbatches;
    double ntuples = baserel->rows;
    Cost   cpu_per_tuple;

    *run_cost = 0;
    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan, to account
     * for the cost of parsing records.
     */
    *run_cost = seq_page_cost * nbatches;

    *startup_cost = 100;
    cpu_per_tuple = kafka_tuple_cost + baserel->baserestrictcost.per_tuple;
    *run_cost += cpu_per_tuple * ntuples;
    *total_cost = *startup_cost + *run_cost;
    DEBUGLOG("startup cost: %f, total_cost: %f, cpu_per_tuple: %f", *startup_cost, *total_cost, cpu_per_tuple);
}

#ifdef DO_PARALLEL
void
KafkaSetParallelPath(Path *path, int num_workers, Cost startup_cost, Cost total_cost, Cost run_cost)
{
    double leader_contribution;
    double parallel_divisor = num_workers;
    leader_contribution     = 1.0 - (0.3 * num_workers);
    if (leader_contribution > 0)
        parallel_divisor += leader_contribution;

    path->rows /= parallel_divisor;
    path->total_cost       = startup_cost + run_cost / parallel_divisor;
    path->parallel_aware   = true;
    path->parallel_workers = num_workers;
}
#endif
