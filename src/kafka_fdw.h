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
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#else
#include "optimizer/var.h"
#endif

#define WARTERMARK_TIMEOUT 1000     /* timeout to query watermark */
#define ESTIMATE_TUPLES 10000000000 /* current hard coded tuple estimate */

#ifdef DO_DEBUG
#define DEBUGLOG(...) elog(DEBUG1, __VA_ARGS__)
#else
#define DEBUGLOG(...)
#endif

#if PG_VERSION_NUM >= 100000
#define DO_PARALLEL
#endif

#define KAFKA_MAX_ERR_MSG 200

#ifndef ALLOCSET_DEFAULT_SIZES
#define ALLOCSET_DEFAULT_SIZES ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE
#endif

#define DEFAULT_KAFKA_OPTIONS                                                                                          \
    .batch_size = 1000, .buffer_delay = 100, .offset_attnum = -1, .partition_attnum = -1, .junk_attnum = -1,           \
    .junk_error_attnum = -1, .strict = false, .num_parse_col = 0, .ignore_junk = false, .num_partitions = 10

#define parsable_attnum(_attn, _kop)                                                                                   \
    (_attn != _kop.junk_attnum && _attn != _kop.junk_error_attnum && _attn != _kop.partition_attnum &&                 \
     _attn != _kop.offset_attnum)

#define ScanopListGetPl(l) (DatumGetInt32(((Const *) list_nth(l, PartitionLow))->constvalue))
#define ScanopListGetPh(l) (DatumGetInt32(((Const *) list_nth(l, PartitionHigh))->constvalue))
#define ScanopListGetOl(l) (DatumGetInt64(((Const *) list_nth(l, OffsetLow))->constvalue))
#define ScanopListGetOh(l) (DatumGetInt64(((Const *) list_nth(l, OffsetHigh))->constvalue))
#define ScanopListGetPhInvinite(l) (DatumGetInt32(((Const *) list_nth(l, PartitionHigh))->constisnull))
#define ScanopListGetOhInvinite(l) (DatumGetInt32(((Const *) list_nth(l, OffsetHigh))->constisnull))

enum kafka_msg_format
{
    INVALID = -1,
    JSON,
    CSV
};

typedef enum kafka_op
{
    OP_INVALID = -1,
    OP_NEQ,       /* <> */
    OP_EQ,        /* = */
    OP_LT,        /* < */
    OP_LTE,       /* <= */
    OP_GT,        /* > */
    OP_GTE,       /* >= */
    OP_ARRAYELEMS /* @> */
} kafka_op;

enum ScanOpListIndex
{
    PartitionLow,
    PartitionHigh,
    OffsetLow,
    OffsetHigh,
    PartitionParamId,
    OffsetParamId,
    PartitionParamOP,
    OffsetParamOP,
};

typedef struct KafkaScanOp
{
    int32 pl;          /* lower boundary of partition */
    int32 ph;          /* upper boundary of partition */
    int64 ol;          /* lower boundary of offset */
    int64 oh;          /* upper boundary of offset */
    bool  oh_infinite; /* offset high watermark infinite*/
    bool  ph_infinite; /* partition high watermark infinite*/
    List *p_params;    /* list of partition parameter for later evaluation*/
    List *o_params;    /* list of offset parameter for later evaluation*/
    List *p_param_ops; /* matching list of off kafka_op for partition parameter */
    List *o_param_ops; /* matching list of off kafka_op for offset parameter */
} KafkaScanOp;

/* Parameter Evaluatio*/
typedef struct KafkaParamValue
{
    int   paramid;
    Oid   oid;
    Datum value;
    bool  is_null;
} KafkaParamValue;

typedef struct KafkaScanP
{
    int32 partition;
    int64 offset;
    int64 offset_lim;
} KafkaScanP;

typedef struct KafkaPartitionList
{
    int32  partition_cnt;
    int32  partitions[FLEXIBLE_ARRAY_MEMBER];
} KafkaPartitionList;

typedef struct KafkaOptions
{
    char *brokers;
    char *topic;
    int   batch_size;
    int   buffer_delay;
    int   num_partitions;    /* number of partitions */
    int   offset_attnum;     /* attribute number for offset col */
    int   partition_attnum;  /* attribute number for partition col */
    int   junk_attnum;       /* attribute number for junk col */
    int   junk_error_attnum; /* attribute number for junk_error col */
    bool  strict;            /* force strict parsing */
    bool  ignore_junk;       /* ignore junk data by setting it to null */
    int   num_parse_col;     /* number of parsable columns */
} KafkaOptions;

typedef struct ParseOptions
{
    enum kafka_msg_format format;
    char                  delimiter;
    char *                null_print;     /* NULL marker string (server encoding!) */
    int                   null_print_len; /* length of same */
    char *                delim;          /* column delimiter (must be 1 byte) */
    char *                quote;          /* CSV quote char (must be 1 byte) */
    char *                escape;         /* CSV escape char (must be 1 byte) */
} ParseOptions;

/* scan koordination */
typedef struct KafkaScanDataDesc
{
    Oid     ps_relid;            /* OID of relation to scan */
#ifdef DO_PARALLEL
    pg_atomic_uint32 next_scanp; /* next scanp to fetch */
#else
    int     next_scanp;
#endif
} KafkaScanDataDesc;

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct KafkaFdwPlanState
{
    KafkaOptions kafka_options; /* kafka optopns */
    ParseOptions parse_options; /* merged COPY options */
    double       nbatches;      /* estimate of number of batches needed */
    double       ntuples;       /* estimate of number of rows to scan */
    int          npart;         /* estimate of number of partitions to scan */
} KafkaFdwPlanState;

/* holds information about extensible KafkaScanP list */
typedef struct KafkaScanPData
{
    int         max_len; /* the allocated size as number of possible entries */
    int         len;     /* is the current length */
    int         cursor;  /* current work item */
    KafkaScanP *data;    /* current buffer (allocated with palloc) */
} KafkaScanPData;

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
    Bitmapset *          attisarray;         /* bitmap of attributes of array type */
    List *               attnumlist;         /* integer list of attnums to copy */
    List *               scanop_list;        /* list of KafkaScanOpP to scan */
    List *               exec_exprs;         /* expressions to evaluate */
    KafkaParamValue *    param_values;       /* param_value List matching exec_expr */
    KafkaPartitionList * partition_list;     /* list and count of partitions */
    KafkaScanPData *     scan_data;          /* scan data list  */
    StringInfoData       attname_buf;        /* buffer holding attribute names for json format */
    char **              attnames;           /* pointer into attname_buf */
    KafkaScanDataDesc *  scan_data_desc;     /* coordination point for parallel scans */
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
    StringInfoData       attname_buf;        /* buffer holding attribute names for json format */
    char **              attnames;           /* pointer into attname_buf */

} KafkaFdwModifyState;
/* connection.c */
void KafkaFdwGetConnection(KafkaOptions *k_options,
                           rd_kafka_t **kafka_handle,
                           rd_kafka_topic_t **kafka_topic_handle);

void kafkaCloseConnection(KafkaFdwExecutionState *festate);

/* option.c */
void kafkaGetOptions(Oid foreigntableid, KafkaOptions *kafka_options, ParseOptions *parse_options);

/* kafka_expr.c */
KafkaPartitionList *getPartitionList(rd_kafka_t *kafka_handle,
                                     rd_kafka_topic_t *kafka_topic_handle);
void  KafkaFlattenScanlist(List *              scan_list,
                           KafkaPartitionList *partition_list,
                           int64               batch_size,
                           KafkaParamValue *   param_values,
                           int                 num_params,
                           KafkaScanPData *    scan_p);
List *KafkaScanOpToList(KafkaScanOp *scan_op);
bool  kafka_valid_scanop_list(List *scan_op_list);
List *dnfNorm(Expr *expr, int partition_attnum, int offset_attnum);
List *applyKafkaScanOpList(List *a, List *b);
List *parmaListToParmaId(List *input);

KafkaScanOp *NewKafkaScanOp(void);

/* parser.c */
int  KafkaReadAttributes(char *                  msg,
                         int                     msg_len,
                         KafkaFdwExecutionState *festate,
                         enum kafka_msg_format   format,
                         bool *                  unterminated_error);
void KafkaWriteAttributes(KafkaFdwModifyState *festate, TupleTableSlot *slot, enum kafka_msg_format format);

/* planning.c */
void KafkaEstimateSize(PlannerInfo *root, RelOptInfo *baserel, KafkaFdwPlanState *fdw_private);
void KafkaEstimateCosts(PlannerInfo *      root,
                        RelOptInfo *       baserel,
                        KafkaFdwPlanState *fdw_private,
                        Cost *             startup_cost,
                        Cost *             total_cost,
                        Cost *             run_cost);
#ifdef DO_PARALLEL
void KafkaSetParallelPath(Path *path, int num_workers, Cost startup_cost, Cost total_cost, Cost run_cost);
#endif
#endif
