#include "kafka_fdw.h"

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
void
KafkaEstimateCosts(PlannerInfo *      root,
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
