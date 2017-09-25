#include "kafka_fdw.h"

/*
    Kafka connection handling
    To reuse kafka connections we setup a
    static per session connection Hash
    each broker / topic combination (i.e. each fdw table)
    gets at most one entry here
*/

typedef struct ConnCacheKey
{
    char *brokers;
    char *topic;
} ConnCacheKey;

typedef struct ConnCacheEntry
{
    ConnCacheKey      key;                /* hash key (must be first) */
    rd_kafka_t *      kafka_handle;       /* connection to kafka server, or NULL */
    rd_kafka_topic_t *kafka_topic_handle; /* kafka topic, or NULL */
    List *            partition_list;     /* list of available partitions, or NIL */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/*
set a connection to festate
basically
    festate->kafka_handle
    festate->kafka_topic_handle
    festate->partition_list
 is set here
*/

void
KafkaFdwGetConnection(KafkaFdwExecutionState *festate, char errstr[KAFKA_MAX_ERR_MSG])
{
    ConnCacheEntry *       entry;
    ConnCacheKey           key;
    bool                   found;
    rd_kafka_topic_conf_t *topic_conf         = NULL;
    rd_kafka_t *           kafka_handle       = NULL;
    rd_kafka_topic_t *     kafka_topic_handle = NULL;
    List *                 partition_list     = NIL; /* this one probably need a different mem context */
    KafkaOptions           k_options          = festate->kafka_options;
    int                    i;
    rd_kafka_resp_err_t    err;

    /* brokers and topic should be validated just double check */

    if (k_options.brokers == NULL || k_options.topic == NULL)
        elog(ERROR, "brokers and topic need to be set ");

    key = (ConnCacheKey){ .brokers = k_options.brokers, .topic = k_options.topic };

    /* First time through, initialize connection cache hashtable */
    if (ConnectionHash == NULL)
    {
        HASHCTL ctl;

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize   = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        /* allocate ConnectionHash in the cache context */
        ctl.hcxt       = CacheMemoryContext;
        ConnectionHash = hash_create("kafka_fdw connections", 8, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }

    entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);

    if (!found)
    {
        rd_kafka_conf_t *conf;

        /* initialize new hashtable entry (key is already filled in) */

        conf = rd_kafka_conf_new();

        kafka_handle       = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, KAFKA_MAX_ERR_MSG);
        kafka_topic_handle = NULL;
        partition_list     = NIL;

        if (kafka_handle != NULL)
        {
            /* Add brokers */
            /* Check if exactly 1 broker was added */
            if (rd_kafka_brokers_add(kafka_handle, key.brokers) != 1)
            {
                rd_kafka_destroy(kafka_handle);
                rd_kafka_conf_destroy(conf);
                elog(ERROR, "No valid brokers specified");
                kafka_handle = NULL;
            }

            /* Create topic handle */
            topic_conf = rd_kafka_topic_conf_new();

            if (rd_kafka_topic_conf_set(topic_conf, "auto.commit.enable", "false", errstr, KAFKA_MAX_ERR_MSG) !=
                RD_KAFKA_CONF_OK)
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_ERROR),
                         errmsg_internal("kafka_fdw: Unable to create topic %s", k_options.topic)));

            kafka_topic_handle = rd_kafka_topic_new(kafka_handle, k_options.topic, topic_conf);
            topic_conf         = NULL; /* Now owned by kafka_topic_handle */

            /* get metadata i.e. partitions for topic */

            const struct rd_kafka_metadata *metadata;

            /* Fetch metadata */
            err = rd_kafka_metadata(kafka_handle, 0, kafka_topic_handle, &metadata, 5000);

            if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                elog(ERROR, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));

            if (metadata->topic_cnt != 1)
                elog(ERROR, "%% Surprisingly got %d topics while 1 was expected", metadata->topic_cnt);

            const struct rd_kafka_metadata_topic *t = &metadata->topics[0];
            for (i = 0; i < t->partition_cnt; i++)
            {
                const struct rd_kafka_metadata_partition *p;
                p              = &t->partitions[i];
                partition_list = lappend_int(partition_list, p->id);
            }

            rd_kafka_metadata_destroy(metadata);
            entry->kafka_handle       = kafka_handle;
            entry->kafka_topic_handle = kafka_topic_handle;
            entry->partition_list     = partition_list;
        }
        else
            hash_search(ConnectionHash, &key, HASH_REMOVE, &found);
    }

    if (entry == NULL || entry->kafka_handle == NULL || entry->kafka_topic_handle == NULL ||
        list_length(entry->partition_list) == 0)
    {
        festate->kafka_handle       = NULL;
        festate->kafka_topic_handle = NULL;
        festate->partition_list     = NIL;
        return;
    }

    festate->kafka_handle       = entry->kafka_handle;
    festate->kafka_topic_handle = entry->kafka_topic_handle;
    festate->partition_list     = entry->partition_list;
}