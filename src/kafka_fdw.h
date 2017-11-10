#ifndef KAFKAFDW_H
#define KAFKAFDW_H

#include "postgres.h"

// librdkafka stuff
#include <librdkafka/rdkafka.h>

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#ifdef DO_DEBUG
#define DEBUGLOG(...) elog(DEBUG1, __VA_ARGS__)
#else
#define DEBUGLOG(...)
#endif

#define KAFKA_MAX_ERR_MSG 200

#ifndef ALLOCSET_DEFAULT_SIZES
#define ALLOCSET_DEFAULT_SIZES ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE
#endif

#define DEFAULT_KAFKA_OPTIONS                                                                                          \
    .batch_size = 1000, .buffer_delay = 100, .offset_attnum = -1, .partition_attnum = -1, .junk_attnum = -1,           \
    .junk_error_attnum = -1, .strict = false, .num_parse_col = 0, .ignore_junk = false

enum kafka_op
{
    OP_INVALID = -1,
    OP_NEQ,       /* <> */
    OP_EQ,        /* = */
    OP_LT,        /* < */
    OP_LTE,       /* <= */
    OP_GT,        /* > */
    OP_GTE,       /* >= */
    OP_ARRAYELEMS /* @> */
};

typedef struct KafkaScanParams
{
    enum kafka_op offset_op; /* current offset operator */
    int32         partition;
    int64         offset;
} KafkaScanParams;

typedef struct KafkaOptions
{
    char *          brokers;
    char *          topic;
    int             batch_size;
    int             buffer_delay;
    int             offset_attnum;     /* attribute number for offset col */
    int             partition_attnum;  /* attribute number for partition col */
    int             junk_attnum;       /* attribute number for junk col */
    int             junk_error_attnum; /* attribute number for junk_error col */
    bool            strict;            /* force strict parsing */
    bool            ignore_junk;       /* ignore junk data by setting it to null */
    int             num_parse_col;     /* number of parsable columns */
    KafkaScanParams scan_params;       /* the partition / offset tuple see above */
} KafkaOptions;

typedef struct ParseOptions
{
    char *format;
    char  delimiter;
    char *null;
    char *encoding;
    bool *force_not_null;
    int   file_encoding;        /* file or remote side's character encoding */
    bool  need_transcoding;     /* file encoding diff from server? */
    bool  csv_mode;             /* Comma Separated Value format? */
    bool  binary;               /* Binary format? */
    bool  header_line;          /* CSV header line? */
    char *null_print;           /* NULL marker string (server encoding!) */
    int   null_print_len;       /* length of same */
    char *null_print_client;    /* same converted to file encoding */
    char *delim;                /* column delimiter (must be 1 byte) */
    char *quote;                /* CSV quote char (must be 1 byte) */
    char *escape;               /* CSV escape char (must be 1 byte) */
    List *force_quote;          /* list of column names */
    bool  force_quote_all;      /* FORCE_QUOTE *? */
    bool *force_quote_flags;    /* per-column CSV FQ flags */
    List *force_notnull;        /* list of column names */
    bool *force_notnull_flags;  /* per-column CSV FNN flags */
    List *force_null;           /* list of column names */
    bool *force_null_flags;     /* per-column CSV FN flags */
    bool  convert_selectively;  /* do selective binary conversion? */
    List *convert_select;       /* list of column names (can be NIL) */
    bool *convert_select_flags; /* per-column CSV/TEXT CS flags */

} ParseOptions;

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct KafkaFdwPlanState
{
    KafkaOptions kafka_options; /* kafka optopns */
    ParseOptions parse_options; /* merged COPY options */
    BlockNumber  pages;         /* estimate of topic's physical size */
    double       ntuples;       /* estimate of number of rows in topic */
} KafkaFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct KafkaFdwExecutionState
{
    KafkaOptions         kafka_options;      /* kafka optopns */
    ParseOptions         parse_options;      /* merged COPY options */
    rd_kafka_t *         kafka_handle;       /* connection to act on */
    rd_kafka_topic_t *   kafka_topic_handle; /* topic to act on */
    rd_kafka_message_t **buffer;             /* message buffer */
    StringInfoData       attribute_buf;      /* reused attribute buffer */
    StringInfoData       junk_buf;           /* reused buffer for junk error messages */
    char **              raw_fields;         /* pointers into attribute_buf */
    int                  max_fields;         /* max number of raw_fields */
    ssize_t              buffer_count;       /* number of messages currently in buffer*/
    ssize_t              buffer_cursor;      /* current message */
    FmgrInfo *           in_functions;       /* array of input functions for each attrs */
    Oid *                typioparams;        /* array of element types for in_functions */
    List *               attnumlist;         /* integer list of attnums to copy */
    List *               partition_list;     /* integer list of partitions */

} KafkaFdwExecutionState;

/*
 * FDW-specific information for ForeignModifyState.fdw_state.
 */
typedef struct KafkaFdwModifyState
{
    KafkaOptions         kafka_options;      /* kafka optopns */
    ParseOptions         parse_options;      /* merged COPY options */
    rd_kafka_t *         kafka_handle;       /* connection to act on */
    rd_kafka_topic_t *   kafka_topic_handle; /* topic to act on */
    rd_kafka_message_t **buffer;             /* message buffer */
    StringInfoData       attribute_buf;      /* attribute buffer */
    char **              raw_fields;         /* pointers into attribute_buf */
    int                  max_fields;         /* max number of raw_fields */
    ssize_t              buffer_count;       /* number of messages currently in buffer*/
    ssize_t              buffer_cursor;      /* current message */
    FmgrInfo *           out_functions;      /* array of output functions for each attrs */
    Oid *                typioparams;        /* array of element types for out_functions */
    List *               attnumlist;         /* integer list of attnums to copy */
    List *               partition_list;     /* integer list of partitions */

} KafkaFdwModifyState;

void KafkaFdwGetConnection(KafkaFdwExecutionState *festate, char errstr[KAFKA_MAX_ERR_MSG]);
void kafkaCloseConnection(KafkaFdwExecutionState *festate);
void KafkaProcessParseOptions(ParseOptions *parse_options, List *options);
void KafkaProcessKafkaOptions(Oid foreigntableid, KafkaOptions *kafka_options, List *options);
int  KafkaReadAttributesCSV(char *msg, int msg_len, KafkaFdwExecutionState *festate, bool *unterminated_error);
bool kafkaParseExpression(KafkaOptions *kafka_options, Expr *expr);
void KafkaWriteAttributesCSV(KafkaFdwModifyState *festate, const char **values, int num_values);

#endif