#include "kafka_fdw.h"
#include "funcapi.h"

#if PG_VERSION_NUM >= 120000
#include "access/relation.h"
#endif


PG_FUNCTION_INFO_V1(kafka_get_watermarks);

Datum
kafka_get_watermarks(PG_FUNCTION_ARGS)
{
	ReturnSetInfo      *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Oid                 relid = PG_GETARG_OID(0);
    Relation            rel;
    KafkaOptions        kafka_options = { DEFAULT_KAFKA_OPTIONS };
    ParseOptions        parse_options = { .format = -1 };
    rd_kafka_t         *kafka_handle = NULL;
    rd_kafka_topic_t   *kafka_topic_handle = NULL;

    /* Lock relation */
    rel = relation_open(relid, AccessShareLock);

    if (RelationGetForm(rel)->relkind != RELKIND_FOREIGN_TABLE)
    {
        relation_close(rel, AccessShareLock);
        elog(ERROR,
             "relation '%s' is not a foreign table",
             RelationGetRelationName(rel));
    }

    kafkaGetOptions(relid, &kafka_options, &parse_options);
 
    PG_TRY();
    {
        KafkaPartitionList *partition_list;
        MemoryContext       oldcontext;
        Tuplestorestate    *tupstore;
        TupleDesc   tupdesc;
        Datum       values[3];
        bool        nulls[3] = {0};
        int         i;

        /* Establish connection */
        KafkaFdwGetConnection(&kafka_options,
                              &kafka_handle,
                              &kafka_topic_handle);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");

        partition_list = getPartitionList(kafka_handle, kafka_topic_handle);

        /* Build tuplestore to hold the result rows */
        oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

        tupstore = tuplestore_begin_heap(true, false, work_mem);
        rsinfo->returnMode = SFRM_Materialize;
        rsinfo->setResult = tupstore;
        rsinfo->setDesc = tupdesc;

        MemoryContextSwitchTo(oldcontext);

        for (i = 0; i < partition_list->partition_cnt; i++)
        {
            int     p = partition_list->partitions[i];
            int64_t low, high;
            rd_kafka_resp_err_t err;

            err = rd_kafka_query_watermark_offsets(kafka_handle,
                                                   kafka_options.topic,
                                                   p,
                                                   &low, &high,
                                                   WARTERMARK_TIMEOUT);

            if (err != RD_KAFKA_RESP_ERR_NO_ERROR
                && err != RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_ERROR),
                         errmsg_internal("kafka_fdw: Failed to get watermarks %s",
                                         rd_kafka_err2str(err))));
            }

            values[0] = p;
            values[1] = low;
            values[2] = high;
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        }

        tuplestore_donestoring(tupstore);
    }
    PG_CATCH();
    {
        /* Close the connection and rethrow an error */
        if (kafka_topic_handle)
            rd_kafka_topic_destroy(kafka_topic_handle);
        if (kafka_handle)
            rd_kafka_destroy(kafka_handle);
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Release everything */
    rd_kafka_topic_destroy(kafka_topic_handle);
    rd_kafka_destroy(kafka_handle);
    relation_close(rel, AccessShareLock);

    return (Datum) 0;
}
