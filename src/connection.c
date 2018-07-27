#include "kafka_fdw.h"
/*
set a connection to festate
basically
    festate->kafka_handle
    festate->kafka_topic_handle
    festate->partition_list
 is set here
*/

void
KafkaFdwGetConnection(KafkaFdwExecutionState *festate)
{
    rd_kafka_topic_conf_t *topic_conf         = NULL;
    rd_kafka_t *           kafka_handle       = NULL;
    rd_kafka_topic_t *     kafka_topic_handle = NULL;
    KafkaOptions           k_options          = festate->kafka_options;
    int                    i;
    rd_kafka_resp_err_t    err;
    rd_kafka_conf_t *      conf;
    KafKaPartitionList *   partition_list = NULL;
    char                   errstr[KAFKA_MAX_ERR_MSG];

    /* brokers and topic should be validated just double check */

    if (k_options.brokers == NULL || k_options.topic == NULL)
        elog(ERROR, "brokers and topic need to be set ");

    conf = rd_kafka_conf_new();

    kafka_handle       = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);

    if (kafka_handle != NULL)
    {
        const struct rd_kafka_metadata *      metadata;
        const struct rd_kafka_metadata_topic *topic;

        /* Add brokers */
        /* Check if exactly 1 broker was added */
        if (rd_kafka_brokers_add(kafka_handle, k_options.brokers) < 1)
        {
            rd_kafka_destroy(kafka_handle);
            elog(ERROR, "No valid brokers specified");
            kafka_handle = NULL;
        }

        /* Create topic handle */
        topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_topic_conf_set(topic_conf, "auto.commit.enable", "false", errstr, KAFKA_MAX_ERR_MSG) !=
            RD_KAFKA_CONF_OK)
            ereport(
              ERROR,
              (errcode(ERRCODE_FDW_ERROR), errmsg_internal("kafka_fdw: Unable to create topic %s", k_options.topic)));

        kafka_topic_handle = rd_kafka_topic_new(kafka_handle, k_options.topic, topic_conf);
        if (!kafka_topic_handle)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg_internal("kafka_fdw: Unable to create topic %s", k_options.topic)));

        topic_conf = NULL;  /* Now owned by kafka_topic_handle */

        /* Fetch metadata */
        err = rd_kafka_metadata(kafka_handle, 0, kafka_topic_handle, &metadata, 5000);

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
            elog(ERROR, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));

        if (metadata->topic_cnt != 1)
            elog(ERROR, "%% Surprisingly got %d topics while 1 was expected", metadata->topic_cnt);

        topic = &metadata->topics[0];
        if (topic->partition_cnt == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg_internal("Topic %s has zero partitions", k_options.topic)));

        /* Get partitions list for topic */
        partition_list                = palloc(sizeof(KafKaPartitionList));
        partition_list->partition_cnt = topic->partition_cnt;
        partition_list->partitions    = palloc0(partition_list->partition_cnt * sizeof(int32));

        for (i = 0; i < partition_list->partition_cnt; i++)
        {
            const struct rd_kafka_metadata_partition *p;
            p                             = &topic->partitions[i];
            partition_list->partitions[i] = p->id;
        }

        rd_kafka_metadata_destroy(metadata);
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg_internal("kafka_fdw: Unable to connect to %s", k_options.brokers),
                 errdetail("%s", errstr)));
    }

    festate->kafka_handle       = kafka_handle;
    festate->kafka_topic_handle = kafka_topic_handle;
    festate->partition_list     = partition_list;
}

void
kafkaCloseConnection(KafkaFdwExecutionState *festate)
{
    rd_kafka_topic_destroy(festate->kafka_topic_handle);
    rd_kafka_destroy(festate->kafka_handle);
    festate->kafka_topic_handle = NULL;
    festate->kafka_handle       = NULL;
}
