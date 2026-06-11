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

    if (rd_kafka_conf_set(conf, "bootstrap.servers", k_options->brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        elog(ERROR, "%s\n", errstr);

    /*
     * Emit an explicit RD_KAFKA_RESP_ERR__PARTITION_EOF marker when a
     * partition is exhausted.  This defaults to false in librdkafka, in which
     * case the only end-of-partition signal would be an empty batch within the
     * poll timeout - which is unreliable because a slow/in-flight fetch also
     * yields an empty batch and would make us skip unread messages.
     */
    if (rd_kafka_conf_set(conf, "enable.partition.eof", "true",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        elog(ERROR, "%s\n", errstr);

    *kafka_handle = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);

    if (*kafka_handle != NULL)
    {
        /* Create topic handle */
        topic_conf = rd_kafka_topic_conf_new();

        if (rd_kafka_topic_conf_set(topic_conf, "auto.commit.enable", "false", errstr, KAFKA_MAX_ERR_MSG) !=
            RD_KAFKA_CONF_OK)
        {
            rd_kafka_topic_conf_destroy(topic_conf);
            ereport(
              ERROR,
              (errcode(ERRCODE_FDW_ERROR), errmsg_internal("kafka_fdw: Unable to create topic %s", k_options->topic)));
        }

        *kafka_topic_handle = rd_kafka_topic_new(*kafka_handle, k_options->topic, topic_conf);
        if (!*kafka_topic_handle)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg_internal("kafka_fdw: Unable to create topic %s", k_options->topic)));

        topic_conf = NULL;  /* Now owned by kafka_topic_handle */
    }
    else
    {
        /*
         * On failure rd_kafka_new() does NOT take ownership of conf, so we
         * must free it ourselves to avoid leaking it.
         */
        rd_kafka_conf_destroy(conf);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg_internal("kafka_fdw: Unable to connect to %s", k_options->brokers),
                 errdetail("%s", errstr)));
    }
}

/*
 * KafkaFdwGetConsumer
 *
 * Open a group-less high-level consumer for the scan path.  Unlike the legacy
 * simple consumer (rd_kafka_consume_start/_batch/_stop) this drives consumption
 * via rd_kafka_assign() + rd_kafka_consumer_poll().  We never subscribe and
 * never commit offsets, so no consumer group is joined.
 *
 * librdkafka requires a group.id to instantiate a consumer that uses
 * rd_kafka_assign() - without it the assign call fails with
 * "Local: Unknown group" (RD_KAFKA_RESP_ERR__UNKNOWN_GROUP).  We therefore
 * configure a fixed group.id together with enable.auto.commit=false.  Because
 * we only ever assign() (never subscribe()) and never commit offsets, no
 * actual consumer group is joined: there is no rebalancing and no group
 * coordinator traffic - the group.id is just a required configuration value.
 */
#define KAFKA_FDW_GROUP_ID "kafka_fdw"
void
KafkaFdwGetConsumer(KafkaOptions *k_options,
                    rd_kafka_t **kafka_handle,
                    rd_kafka_topic_t **kafka_topic_handle)
{
    rd_kafka_conf_t *conf;
    char             errstr[KAFKA_MAX_ERR_MSG];

    if (k_options->brokers == NULL || k_options->topic == NULL)
        elog(ERROR, "brokers and topic need to be set ");

    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", k_options->brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        rd_kafka_conf_destroy(conf);
        elog(ERROR, "%s", errstr);
    }

    /*
     * A group.id is required for rd_kafka_assign() (see function comment).
     * Combined with enable.auto.commit=false and assign-only usage this stays
     * effectively group-less: no rebalancing, no offset commits.
     */
    if (rd_kafka_conf_set(conf, "group.id", KAFKA_FDW_GROUP_ID, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        rd_kafka_conf_destroy(conf);
        elog(ERROR, "%s", errstr);
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        rd_kafka_conf_destroy(conf);
        elog(ERROR, "%s", errstr);
    }

    /* emit an explicit PARTITION_EOF marker when a partition is exhausted */
    if (rd_kafka_conf_set(conf, "enable.partition.eof", "true", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        rd_kafka_conf_destroy(conf);
        elog(ERROR, "%s", errstr);
    }

    *kafka_handle = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);
    if (*kafka_handle == NULL)
    {
        rd_kafka_conf_destroy(conf);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg_internal("kafka_fdw: Unable to connect to %s", k_options->brokers),
                 errdetail("%s", errstr)));
    }

    /* redirect the handle's main queue so rd_kafka_consumer_poll() works */
    rd_kafka_poll_set_consumer(*kafka_handle);

    /*
     * We still create a topic handle - it is only used for metadata lookups
     * (getPartitionList); it is NOT used for the legacy consume API here.
     */
    *kafka_topic_handle = rd_kafka_topic_new(*kafka_handle, k_options->topic, NULL);
    if (*kafka_topic_handle == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg_internal("kafka_fdw: Unable to create topic %s", k_options->topic)));
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
