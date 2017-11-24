#include "kafka_fdw.h"

static List *getPartitionList(rd_kafka_t *kafka_handle, rd_kafka_topic_t *kafka_topic_handle, char *topic);

/*
set a connection to festate
basically
    festate->kafka_handle
    festate->kafka_topic_handle
    festate->partition_list
 is set here
*/
void
KafkaFdwGetConnection(KafkaFdwPlanState *festate, char errstr[KAFKA_MAX_ERR_MSG])
{
    rd_kafka_topic_conf_t *topic_conf         = NULL;
    rd_kafka_t *           kafka_handle       = NULL;
    rd_kafka_topic_t *     kafka_topic_handle = NULL;
    List *                 partition_list     = NIL; /* this one probably need a different mem context */
    KafkaOptions           k_options          = festate->kafka_options;

    /* brokers and topic should be validated just double check */

    if (k_options.brokers == NULL || k_options.topic == NULL)
        elog(ERROR, "brokers and topic need to be set ");

    rd_kafka_conf_t *conf;

    conf       = rd_kafka_conf_new();
    topic_conf = rd_kafka_topic_conf_new();

    /* Setup topic conf */
    if (rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "smallest", errstr, KAFKA_MAX_ERR_MSG) !=
        RD_KAFKA_CONF_OK)
        ereport(
          ERROR,
          (errcode(ERRCODE_FDW_ERROR),
           errmsg_internal("kafka_fdw: could not set configuration option auto.offset.reset %s", k_options.topic)));

    kafka_handle   = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);
    partition_list = NIL;

    if (kafka_handle != NULL)
    {
        /* Add brokers */
        /* Check if at least 1 broker was added */
        if (rd_kafka_brokers_add(kafka_handle, k_options.brokers) < 1)
        {
            rd_kafka_destroy(kafka_handle);
            elog(ERROR, "No valid brokers specified");
        }

        /* Create topic handle */
        kafka_topic_handle = rd_kafka_topic_new(kafka_handle, k_options.topic, topic_conf);
        topic_conf         = NULL; /* Now owned by kafka_topic_handle */

        /* Fetch metadata */
        partition_list = getPartitionList(kafka_handle, kafka_topic_handle, k_options.topic);
    }

    if (kafka_handle == NULL || kafka_topic_handle == NULL || list_length(partition_list) == 0)
    {
        festate->kafka_handle       = NULL;
        festate->kafka_topic_handle = NULL;
        festate->partition_list     = NIL;
        return;
    }

    festate->kafka_handle       = kafka_handle;
    festate->kafka_topic_handle = kafka_topic_handle;
    festate->partition_list     = partition_list;
}

static List *
getPartitionList(rd_kafka_t *kafka_handle, rd_kafka_topic_t *kafka_topic_handle, char *topic)
{
    rd_kafka_resp_err_t             err;
    const struct rd_kafka_metadata *metadata;

    List *partition_list = NIL;
    err                  = rd_kafka_metadata(kafka_handle, 0, kafka_topic_handle, &metadata, 5000);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        rd_kafka_topic_destroy(kafka_topic_handle);
        rd_kafka_destroy(kafka_handle);
        elog(ERROR, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));
    }

    if (metadata->topic_cnt != 1)
    {
        rd_kafka_topic_destroy(kafka_topic_handle);
        rd_kafka_destroy(kafka_handle);
        elog(ERROR, "%% Surprisingly got %d topics while 1 was expected", metadata->topic_cnt);
    }

    const struct rd_kafka_metadata_topic *t = &metadata->topics[0];

    for (int i = 0; i < t->partition_cnt; i++)
    {
        const struct rd_kafka_metadata_partition *p;
        rd_kafka_resp_err_t                       err;
        KafkaPartitionMeta *                      p_meta = palloc(sizeof(KafkaPartitionMeta));
        p                                                = &t->partitions[i];
        p_meta->partition                                = p->id;
        int64_t high;
        int64_t low;

        err = rd_kafka_query_watermark_offsets(kafka_handle, topic, p->id, &low, &high, 5000);
        if (err)
        {
            rd_kafka_topic_destroy(kafka_topic_handle);
            rd_kafka_destroy(kafka_handle);
            elog(ERROR, "%% get_watermark_offsets() failed : %s\n ", rd_kafka_err2str(err));
        }
        p_meta->high = high;
        p_meta->low  = low;

        partition_list = lappend(partition_list, p_meta);
    }

    rd_kafka_metadata_destroy(metadata);
    return partition_list;
}

void
kafkaCloseConnection(KafkaFdwExecutionState *festate)
{
    DEBUGLOG("%s", __func__);
    rd_kafka_topic_destroy(festate->kafka_topic_handle);
    rd_kafka_destroy(festate->kafka_handle);
    festate->kafka_topic_handle = NULL;
    festate->kafka_handle       = NULL;
}