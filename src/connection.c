#include "kafka_fdw.h"

void
KafkaFdwGetConnection(KafkaOptions *k_options,
                      rd_kafka_t **kafka_handle,
                      rd_kafka_topic_t **kafka_topic_handle)
{
    rd_kafka_topic_conf_t *topic_conf         = NULL;
    rd_kafka_conf_t *      conf;
    char                   errstr[KAFKA_MAX_ERR_MSG];

    /* brokers and topic should be validated just double check */

    if (k_options->brokers == NULL || k_options->topic == NULL)
        elog(ERROR, "brokers and topic need to be set ");

    conf = rd_kafka_conf_new();

    *kafka_handle = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);

    if (*kafka_handle != NULL)
    {
        /* Add brokers */
        /* Check if exactly 1 broker was added */
        if (rd_kafka_brokers_add(*kafka_handle, k_options->brokers) < 1)
        {
            rd_kafka_destroy(*kafka_handle);
            elog(ERROR, "No valid brokers specified");
            *kafka_handle = NULL;
        }

        /* Create topic handle */
        topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_topic_conf_set(topic_conf, "auto.commit.enable", "false", errstr, KAFKA_MAX_ERR_MSG) !=
            RD_KAFKA_CONF_OK)
            ereport(
              ERROR,
              (errcode(ERRCODE_FDW_ERROR), errmsg_internal("kafka_fdw: Unable to create topic %s", k_options->topic)));

        *kafka_topic_handle = rd_kafka_topic_new(*kafka_handle, k_options->topic, topic_conf);
        if (!*kafka_topic_handle)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg_internal("kafka_fdw: Unable to create topic %s", k_options->topic)));

        topic_conf = NULL;  /* Now owned by kafka_topic_handle */
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg_internal("kafka_fdw: Unable to connect to %s", k_options->brokers),
                 errdetail("%s", errstr)));
    }
}

void
kafkaCloseConnection(KafkaFdwExecutionState *festate)
{
    if (festate->kafka_topic_handle)
        rd_kafka_topic_destroy(festate->kafka_topic_handle);
    if (festate->kafka_handle)
        rd_kafka_destroy(festate->kafka_handle);
    festate->kafka_topic_handle = NULL;
    festate->kafka_handle       = NULL;
}
