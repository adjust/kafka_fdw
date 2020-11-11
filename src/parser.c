#include "kafka_fdw.h"
#include "parser/parse_coerce.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/json.h"
#if PG_VERSION_NUM >= 130000
#include "common/jsonapi.h"
#include "utils/jsonfuncs.h"
#include "mb/pg_wchar.h"
#else
#include "utils/jsonapi.h"
#endif
#include "utils/lsyscache.h"
#include "utils/typcache.h"

/* parse json */
static HTAB *get_json_as_hash(char *json, int len, const char *funcname);
static void  hash_object_field_start(void *state, char *fname, bool isnull);
static void  hash_object_field_end(void *state, char *fname, bool isnull);
static void  hash_array_start(void *state);
static void  hash_scalar(void *state, char *token, JsonTokenType tokentype);

/* encode json */

typedef enum /* type categories for datum_to_json */
{
    JSONTYPE_NULL,    /* null, so we didn't bother to identify */
    JSONTYPE_BOOL,    /* boolean (built-in types only) */
    JSONTYPE_NUMERIC, /* numeric (ditto) */
    JSONTYPE_DATE,    /* we use special formatting for datetimes */
    JSONTYPE_TIMESTAMP,
    JSONTYPE_TIMESTAMPTZ,
    JSONTYPE_JSON,      /* JSON itself (and JSONB) */
    JSONTYPE_ARRAY,     /* array */
    JSONTYPE_COMPOSITE, /* composite */
    JSONTYPE_CAST,      /* something with an explicit cast to JSON */
    JSONTYPE_OTHER      /* all else */
} JsonTypeCategory;

static void json_categorize_type(Oid typoid, JsonTypeCategory *tcategory, Oid *outfuncoid);
static void array_dim_to_json(StringInfo       result,
                              int              dim,
                              int              ndims,
                              int *            dims,
                              Datum *          vals,
                              bool *           nulls,
                              int *            valcount,
                              JsonTypeCategory tcategory,
                              Oid              outfuncoid,
                              bool             use_line_feeds);

static void array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds);
static void datum_to_json(Datum            val,
                          bool             is_null,
                          StringInfo       result,
                          JsonTypeCategory tcategory,
                          Oid              outfuncoid,
                          bool             key_scalar);
static void add_json(Datum val, bool is_null, StringInfo result, Oid val_type, bool key_scalar);
static void composite_to_json(Datum composite, StringInfo result, bool use_line_feeds);
static int  KafkaReadAttributesJson(char *msg, int msg_len, KafkaFdwExecutionState *festate, bool *unterminated_error);
static int  KafkaReadAttributesCSV(char *msg, int msg_len, KafkaFdwExecutionState *festate, bool *unterminated_error);
static void KafkaWriteAttributesCSV(KafkaFdwModifyState *festate, TupleTableSlot *slot);
static void KafkaWriteAttributesJson(KafkaFdwModifyState *festate, TupleTableSlot *slot);

/*
 * Parse the char into separate attributes (fields)
 * Returns number of fields or -1 in case of unterminated quoted string
 */
int
KafkaReadAttributes(char *                  msg,
                    int                     msg_len,
                    KafkaFdwExecutionState *festate,
                    enum kafka_msg_format   format,
                    bool *                  unterminated_error)
{
    if (format == CSV)
        return KafkaReadAttributesCSV(msg, msg_len, festate, unterminated_error);
    else if (format == JSON)
        return KafkaReadAttributesJson(msg, msg_len, festate, unterminated_error);

    return -1;
}

void
KafkaWriteAttributes(KafkaFdwModifyState *festate, TupleTableSlot *slot, enum kafka_msg_format format)
{
    if (format == CSV)
        KafkaWriteAttributesCSV(festate, slot);
    else if (format == JSON)
        KafkaWriteAttributesJson(festate, slot);
}

/*
 * Parse the char into separate attributes (fields)
 * Returns number of fields or -1 in case of unterminated quoted string
 */
static int
KafkaReadAttributesCSV(char *msg, int msg_len, KafkaFdwExecutionState *festate, bool *unterminated_error)
{
    char  delimc  = festate->parse_options.delim[0];
    char  quotec  = festate->parse_options.quote[0];
    char  escapec = festate->parse_options.escape[0];
    int   fieldno = 0;
    char *output_ptr;
    char *cur_ptr;
    char *line_end_ptr;

    // DEBUGLOG("%s", __func__);

    *unterminated_error = false;
    resetStringInfo(&festate->attribute_buf);
    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.  We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into festate->raw_fields[].
     */
    if (festate->attribute_buf.maxlen <= msg_len)
        enlargeStringInfo(&festate->attribute_buf, msg_len);
    output_ptr = festate->attribute_buf.data;

    /* set pointer variables for loop */
    cur_ptr      = msg;
    line_end_ptr = msg + msg_len;

    /* Outer loop iterates over fields */
    fieldno = 0;
    for (;;)
    {

        bool  found_delim = false;
        bool  saw_quote   = false;
        char *start_ptr;
        char *end_ptr;
        int   input_len;

        /* Make sure there is enough space for the next value */
        if (fieldno >= festate->max_fields)
        {
            festate->max_fields *= 2;
            festate->raw_fields = repalloc(festate->raw_fields, festate->max_fields * sizeof(char *));
        }

        /* Remember start of field on both input and output sides */
        start_ptr                    = cur_ptr;
        festate->raw_fields[fieldno] = output_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        for (;;)
        {
            char c;

            /* Not in quote */
            for (;;)
            {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                    goto endfield;
                c = *cur_ptr++;
                /* unquoted field delimiter */
                if (c == delimc)
                {
                    found_delim = true;
                    goto endfield;
                }
                /* start of quoted field (or part of field) */
                if (c == quotec)
                {
                    saw_quote = true;
                    break;
                }
                /* Add c to output string */
                *output_ptr++ = c;
            }

            /* In quote */
            for (;;)
            {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                {
                    *unterminated_error = true;
                    /* Terminatestring */
                    *output_ptr = '\0';
                    /* report a field less back */
                    return fieldno;
                }

                c = *cur_ptr++;

                /* escape within a quoted field */
                if (c == escapec)
                {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if (cur_ptr < line_end_ptr)
                    {
                        char nextc = *cur_ptr;

                        if (nextc == escapec || nextc == quotec)
                        {
                            *output_ptr++ = nextc;
                            cur_ptr++;
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if (c == quotec)
                    break;

                /* Add c to output string */
                *output_ptr++ = c;
            }
        }
    endfield:

        /* Terminate attribute value in output area */
        *output_ptr++ = '\0';

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (!saw_quote && input_len == festate->parse_options.null_print_len &&
            strncmp(start_ptr, festate->parse_options.null_print, input_len) == 0)
            festate->raw_fields[fieldno] = NULL;

        fieldno++;
        /* Done if we hit EOL instead of a delim */
        if (!found_delim)
            break;
    }

    /* Clean up state of attribute_buf */
    output_ptr--;
    Assert(*output_ptr == '\0');

    return fieldno;
}

#define DUMPSOFAR()                                                                                                    \
    do                                                                                                                 \
    {                                                                                                                  \
        if (ptr > start)                                                                                               \
            appendBinaryStringInfo(&festate->attribute_buf, start, ptr - start);                                       \
    } while (0)

/*
 *  Write a row of csv to festate->attribute_buf
 */

static void
KafkaWriteAttributesCSV(KafkaFdwModifyState *festate, TupleTableSlot *slot)
{
    ListCell *lc;

    char  delimc         = festate->parse_options.delim[0];
    char  quotec         = festate->parse_options.quote[0];
    char  escapec        = festate->parse_options.escape[0];
    char *null_print     = festate->parse_options.null_print;
    int   null_print_len = festate->parse_options.null_print_len;
    int   pindex         = 0;

    DEBUGLOG("%s", __func__);

    foreach (lc, festate->attnumlist)
    {
        int         attnum = lfirst_int(lc);
        bool        isnull;
        const char *ptr, *start;
        char        c;
        bool        use_quote = false;
        const char *val;
        const char *tptr;
        Datum       value = slot_getattr(slot, attnum, &isnull);
        if (isnull)
        {
            if (null_print)
                appendBinaryStringInfo(&festate->attribute_buf, null_print, null_print_len);
        }
        else
        {
            val  = OutputFunctionCall(&festate->out_functions[pindex], value);
            tptr = val;
            /*
             * Make a preliminary pass to discover if it needs quoting
             */
            while ((c = *tptr) != '\0')
            {
                if (c == delimc || c == quotec || c == '\n' || c == '\r')
                {
                    use_quote = true;
                    break;
                }
                tptr++;
            }

            ptr = val;

            if (use_quote)
            {
                start = ptr;
                appendStringInfoCharMacro(&festate->attribute_buf, quotec);

                while ((c = *ptr) != '\0')
                {
                    if (c == quotec || c == escapec)
                    {
                        DUMPSOFAR();
                        appendStringInfoCharMacro(&festate->attribute_buf, escapec);
                        start = ptr; /* we include char in next run */
                    }
                    ptr++;
                }
                DUMPSOFAR();
                appendStringInfoCharMacro(&festate->attribute_buf, quotec);
            }
            else
            {
                appendBinaryStringInfo(&festate->attribute_buf, ptr, strlen(ptr));
            }
        }
        pindex++;
        appendStringInfoCharMacro(&festate->attribute_buf, delimc);
    }
}

/* state for get_json_as_hash */
typedef struct JhashState
{
    JsonLexContext *lex;
    const char *    function_name;
    HTAB *          hash;
    char *          saved_scalar;
    char *          save_json_start;
} JHashState;

/* hashtable element */
typedef struct JsonHashEntry
{
    char  fname[NAMEDATALEN]; /* hash key (MUST BE FIRST) */
    char *val;
    char *json;
    bool  isnull;
} JsonHashEntry;

/*
 * get_json_as_hash
 *
 * decompose a json object into a hash table.
 */
static HTAB *
get_json_as_hash(char *json, int len, const char *funcname)
{
    HASHCTL         ctl;
    HTAB *          tab;
    JHashState *    state;
#if PG_VERSION_NUM >= 130000
    JsonLexContext *lex = makeJsonLexContextCstringLen(json, len,
                                                       GetDatabaseEncoding(),
                                                       true);
#else
    JsonLexContext *lex = makeJsonLexContextCstringLen(json, len, true);
#endif
    JsonSemAction * sem;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = NAMEDATALEN;
    ctl.entrysize = sizeof(JsonHashEntry);
    ctl.hcxt      = CurrentMemoryContext;
    tab           = hash_create("json object hashtable", 100, &ctl, HASH_ELEM | HASH_CONTEXT);

    state = palloc0(sizeof(JHashState));
    sem   = palloc0(sizeof(JsonSemAction));

    state->function_name = funcname;
    state->hash          = tab;
    state->lex           = lex;

    sem->semstate           = (void *) state;
    sem->array_start        = hash_array_start;
    sem->scalar             = hash_scalar;
    sem->object_field_start = hash_object_field_start;
    sem->object_field_end   = hash_object_field_end;

#if PG_VERSION_NUM < 130000
    pg_parse_json(lex, sem);
#else
    JsonParseErrorType error;
    if ((error = pg_parse_json(lex, sem)) != JSON_SUCCESS)
        json_ereport_error(error, lex);
#endif

    return tab;
}

static void
hash_object_field_start(void *state, char *fname, bool isnull)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level > 1)
        return;

    if (_state->lex->token_type == JSON_TOKEN_ARRAY_START || _state->lex->token_type == JSON_TOKEN_OBJECT_START)
    {
        /* remember start position of the whole text of the subobject */
        _state->save_json_start = _state->lex->token_start;
    }
    else
    {
        /* must be a scalar */
        _state->save_json_start = NULL;
    }
}

static void
hash_object_field_end(void *state, char *fname, bool isnull)
{
    JHashState *   _state = (JHashState *) state;
    JsonHashEntry *hashentry;
    bool           found;

    /*
     * Ignore nested fields.
     */
    if (_state->lex->lex_level > 1)
        return;

    /*
     * Ignore field names >= NAMEDATALEN - they can't match a record field.
     * (Note: without this test, the hash code would truncate the string at
     * NAMEDATALEN-1, and could then match against a similarly-truncated
     * record field name.  That would be a reasonable behavior, but this code
     * has previously insisted on exact equality, so we keep this behavior.)
     */
    if (strlen(fname) >= NAMEDATALEN)
        return;

    hashentry = hash_search(_state->hash, fname, HASH_ENTER, &found);

    /*
     * found being true indicates a duplicate. We don't do anything about
     * that, a later field with the same name overrides the earlier field.
     */

    hashentry->isnull = isnull;
    if (_state->save_json_start != NULL)
    {
        int   len = _state->lex->prev_token_terminator - _state->save_json_start;
        char *val = palloc((len + 1) * sizeof(char));

        memcpy(val, _state->save_json_start, len);
        val[len]       = '\0';
        hashentry->val = val;
    }
    else
    {
        /* must have had a scalar instead */
        hashentry->val = _state->saved_scalar;
    }
}

static void
hash_array_start(void *state)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level == 0)
        ereport(
          ERROR,
          (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot call %s on an array", _state->function_name)));
}

static void
hash_scalar(void *state, char *token, JsonTokenType tokentype)
{
    JHashState *_state = (JHashState *) state;

    if (_state->lex->lex_level == 0)
        ereport(
          ERROR,
          (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot call %s on a scalar", _state->function_name)));

    if (_state->lex->lex_level == 1)
        _state->saved_scalar = token;
}

static int
KafkaReadAttributesJson(char *msg, int msg_len, KafkaFdwExecutionState *festate, bool *error)
{
    ListCell *    cur;
    HTAB *        json_hash;
    volatile int  fieldno     = 0;
    int           num_ent     = 0;
    bool          ignore_junk = festate->kafka_options.ignore_junk;
    MemoryContext ccxt        = CurrentMemoryContext;

    *error = false;
    /* much like populate_record_worker but with error cathing if needed */
    if (ignore_junk)
    {
        PG_TRY();
        {
            json_hash = get_json_as_hash(msg, msg_len, "KafkaReadAttributesJson");
        }
        PG_CATCH();
        {
            *error = true;
            MemoryContextSwitchTo(ccxt);

            /* accumulate errors if needed */
            if (festate->kafka_options.junk_error_attnum != -1)
            {
                ErrorData *errdata = CopyErrorData();

                if (festate->junk_buf.len > 0)
                    appendStringInfoCharMacro(&festate->junk_buf, '\n');

                appendStringInfoString(&festate->junk_buf, errdata->message);
            }
            FlushErrorState();
            return 0;
        }
        PG_END_TRY();
    }
    else
        json_hash = get_json_as_hash(msg, msg_len, "KafkaReadAttributesJson");

    num_ent = hash_get_num_entries(json_hash);
    if (num_ent == 0)
    {
        return 0;
    }

    foreach (cur, festate->attnumlist)
    {
        int            attnum    = lfirst_int(cur);
        int            m         = attnum - 1;
        JsonHashEntry *hashentry = NULL;
        /* Make sure there is enough space for the next value */
        if (fieldno >= festate->max_fields)
        {
            festate->max_fields *= 2;
            festate->raw_fields = repalloc(festate->raw_fields, festate->max_fields * sizeof(char *));
        }

        if (parsable_attnum(attnum, festate->kafka_options))
        {
            hashentry = hash_search(json_hash, festate->attnames[m], HASH_FIND, NULL);
            if (hashentry == NULL || hashentry->isnull)
                festate->raw_fields[fieldno] = NULL;
            else
                festate->raw_fields[fieldno] = hashentry->val;

            fieldno++;
        }
    }
    /* report back total number of found fields to match against strict mode */
    return num_ent;
}

/*
 * Determine how we want to print values of a given type in datum_to_json.
 *
 * Given the datatype OID, return its JsonTypeCategory, as well as the type's
 * output function OID.  If the returned category is JSONTYPE_CAST, we
 * return the OID of the type->JSON cast function instead.
 */
static void
json_categorize_type(Oid typoid, JsonTypeCategory *tcategory, Oid *outfuncoid)
{
    bool typisvarlena;

    /* Look through any domain */
    typoid = getBaseType(typoid);

    *outfuncoid = InvalidOid;

    /*
     * We need to get the output function for everything except date and
     * timestamp types, array and composite types, booleans, and non-builtin
     * types where there's a cast to json.
     */

    switch (typoid)
    {
        case BOOLOID: *tcategory = JSONTYPE_BOOL; break;

        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
            *tcategory = JSONTYPE_NUMERIC;
            break;

        case DATEOID: *tcategory = JSONTYPE_DATE; break;

        case TIMESTAMPOID: *tcategory = JSONTYPE_TIMESTAMP; break;

        case TIMESTAMPTZOID: *tcategory = JSONTYPE_TIMESTAMPTZ; break;

        case JSONOID:
        case JSONBOID:
            getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
            *tcategory = JSONTYPE_JSON;
            break;

        default:
            /* Check for arrays and composites */
            if (OidIsValid(get_element_type(typoid)))
                *tcategory = JSONTYPE_ARRAY;
            else if (type_is_rowtype(typoid))
                *tcategory = JSONTYPE_COMPOSITE;
            else
            {
                /* It's probably the general case ... */
                *tcategory = JSONTYPE_OTHER;
                /* but let's look for a cast to json, if it's not built-in */
                if (typoid >= FirstNormalObjectId)
                {
                    Oid              castfunc;
                    CoercionPathType ctype;

                    ctype = find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT, &castfunc);
                    if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
                    {
                        *tcategory  = JSONTYPE_CAST;
                        *outfuncoid = castfunc;
                    }
                    else
                    {
                        /* non builtin type with no cast */
                        getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
                    }
                }
                else
                {
                    /* any other builtin type */
                    getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
                }
            }
            break;
    }
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void
array_dim_to_json(StringInfo       result,
                  int              dim,
                  int              ndims,
                  int *            dims,
                  Datum *          vals,
                  bool *           nulls,
                  int *            valcount,
                  JsonTypeCategory tcategory,
                  Oid              outfuncoid,
                  bool             use_line_feeds)
{
    int         i;
    const char *sep;

    Assert(dim < ndims);

    sep = use_line_feeds ? ",\n " : ",";

    appendStringInfoChar(result, '[');

    for (i = 1; i <= dims[dim]; i++)
    {
        if (i > 1)
            appendStringInfoString(result, sep);

        if (dim + 1 == ndims)
        {
            datum_to_json(vals[*valcount], nulls[*valcount], result, tcategory, outfuncoid, false);
            (*valcount)++;
        }
        else
        {
            /*
             * Do we want line feeds on inner dimensions of arrays? For now
             * we'll say no.
             */
            array_dim_to_json(result, dim + 1, ndims, dims, vals, nulls, valcount, tcategory, outfuncoid, false);
        }
    }

    appendStringInfoChar(result, ']');
}

/*
 * Turn an array into JSON.
 */
static void
array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds)
{
    ArrayType *      v            = DatumGetArrayTypeP(array);
    Oid              element_type = ARR_ELEMTYPE(v);
    int *            dim;
    int              ndim;
    int              nitems;
    int              count = 0;
    Datum *          elements;
    bool *           nulls;
    int16            typlen;
    bool             typbyval;
    char             typalign;
    JsonTypeCategory tcategory;
    Oid              outfuncoid;

    ndim   = ARR_NDIM(v);
    dim    = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    if (nitems <= 0)
    {
        appendStringInfoString(result, "[]");
        return;
    }

    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    json_categorize_type(element_type, &tcategory, &outfuncoid);

    deconstruct_array(v, element_type, typlen, typbyval, typalign, &elements, &nulls, &nitems);

    array_dim_to_json(result, 0, ndim, dim, elements, nulls, &count, tcategory, outfuncoid, use_line_feeds);

    pfree(elements);
    pfree(nulls);
}

/*
 * Turn a Datum into JSON text, appending the string to "result".
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is being printed as a key, so insist
 * it's of an acceptable type, and force it to be quoted.
 */
static void
datum_to_json(Datum val, bool is_null, StringInfo result, JsonTypeCategory tcategory, Oid outfuncoid, bool key_scalar)
{
    char *outputstr;
    text *jsontext;

    check_stack_depth();

    /* callers are expected to ensure that null keys are not passed in */
    Assert(!(key_scalar && is_null));

    if (is_null)
    {
        appendStringInfoString(result, "null");
        return;
    }

    if (key_scalar && (tcategory == JSONTYPE_ARRAY || tcategory == JSONTYPE_COMPOSITE || tcategory == JSONTYPE_JSON ||
                       tcategory == JSONTYPE_CAST))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("key value must be scalar, not array, composite, or json")));

    switch (tcategory)
    {
        case JSONTYPE_ARRAY: array_to_json_internal(val, result, false); break;
        case JSONTYPE_COMPOSITE: composite_to_json(val, result, false); break;
        case JSONTYPE_BOOL:
            outputstr = DatumGetBool(val) ? "true" : "false";
            if (key_scalar)
                escape_json(result, outputstr);
            else
                appendStringInfoString(result, outputstr);
            break;
        case JSONTYPE_NUMERIC:
            outputstr = OidOutputFunctionCall(outfuncoid, val);

            /*
             * Don't call escape_json for a non-key if it's a valid JSON
             * number.
             */
            if (!key_scalar && IsValidJsonNumber(outputstr, strlen(outputstr)))
                appendStringInfoString(result, outputstr);
            else
                escape_json(result, outputstr);
            pfree(outputstr);
            break;
        case JSONTYPE_DATE:
        {
            DateADT      date;
            struct pg_tm tm;
            char         buf[MAXDATELEN + 1];

            date = DatumGetDateADT(val);
            /* Same as date_out(), but forcing DateStyle */
            if (DATE_NOT_FINITE(date))
                EncodeSpecialDate(date, buf);
            else
            {
                j2date(date + POSTGRES_EPOCH_JDATE, &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
                EncodeDateOnly(&tm, USE_XSD_DATES, buf);
            }
            appendStringInfo(result, "\"%s\"", buf);
        }
        break;
        case JSONTYPE_TIMESTAMP:
        {
            Timestamp    timestamp;
            struct pg_tm tm;
            fsec_t       fsec;
            char         buf[MAXDATELEN + 1];

            timestamp = DatumGetTimestamp(val);
            /* Same as timestamp_out(), but forcing DateStyle */
            if (TIMESTAMP_NOT_FINITE(timestamp))
                EncodeSpecialTimestamp(timestamp, buf);
            else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
                EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
            else
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
            appendStringInfo(result, "\"%s\"", buf);
        }
        break;
        case JSONTYPE_TIMESTAMPTZ:
        {
            TimestampTz  timestamp;
            struct pg_tm tm;
            int          tz;
            fsec_t       fsec;
            const char * tzn = NULL;
            char         buf[MAXDATELEN + 1];

            timestamp = DatumGetTimestampTz(val);
            /* Same as timestamptz_out(), but forcing DateStyle */
            if (TIMESTAMP_NOT_FINITE(timestamp))
                EncodeSpecialTimestamp(timestamp, buf);
            else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
                EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
            else
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
            appendStringInfo(result, "\"%s\"", buf);
        }
        break;
        case JSONTYPE_JSON:
            /* JSON and JSONB output will already be escaped */
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            appendStringInfoString(result, outputstr);
            pfree(outputstr);
            break;
        case JSONTYPE_CAST:
            /* outfuncoid refers to a cast function, not an output function */
            jsontext  = DatumGetTextP(OidFunctionCall1(outfuncoid, val));
            outputstr = text_to_cstring(jsontext);
            appendStringInfoString(result, outputstr);
            pfree(outputstr);
            pfree(jsontext);
            break;
        default:
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            escape_json(result, outputstr);
            pfree(outputstr);
            break;
    }
}

/*
 * Turn a composite / record into JSON.
 */
static void
composite_to_json(Datum composite, StringInfo result, bool use_line_feeds)
{
    HeapTupleHeader td;
    Oid             tupType;
    int32           tupTypmod;
    TupleDesc       tupdesc;
    HeapTupleData   tmptup, *tuple;
    int             i;
    bool            needsep = false;
    const char *    sep;

    sep = use_line_feeds ? ",\n " : ",";

    td = DatumGetHeapTupleHeader(composite);

    /* Extract rowtype info and find a tupdesc */
    tupType   = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc   = lookup_rowtype_tupdesc(tupType, tupTypmod);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len  = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple         = &tmptup;

    appendStringInfoChar(result, '{');

    for (i = 0; i < tupdesc->natts; i++)
    {
        Datum            val;
        bool             isnull;
        char *           attname;
        JsonTypeCategory tcategory;
        Oid              outfuncoid;
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (attr->attisdropped)
            continue;

        if (needsep)
            appendStringInfoString(result, sep);
        needsep = true;

        attname = NameStr(attr->attname);
        escape_json(result, attname);
        appendStringInfoChar(result, ':');

        val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        if (isnull)
        {
            tcategory  = JSONTYPE_NULL;
            outfuncoid = InvalidOid;
        }
        else
            json_categorize_type(attr->atttypid, &tcategory, &outfuncoid);

        datum_to_json(val, isnull, result, tcategory, outfuncoid, false);
    }

    appendStringInfoChar(result, '}');
    ReleaseTupleDesc(tupdesc);
}

/*
 * Append JSON text for "val" to "result".
 *
 * This is just a thin wrapper around datum_to_json.  If the same type will be
 * printed many times, avoid using this; better to do the json_categorize_type
 * lookups only once.
 */
static void
add_json(Datum val, bool is_null, StringInfo result, Oid val_type, bool key_scalar)
{
    JsonTypeCategory tcategory;
    Oid              outfuncoid;

    if (val_type == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine input data type")));

    if (is_null)
    {
        tcategory  = JSONTYPE_NULL;
        outfuncoid = InvalidOid;
    }
    else
        json_categorize_type(val_type, &tcategory, &outfuncoid);

    datum_to_json(val, is_null, result, tcategory, outfuncoid, key_scalar);
}

static void
KafkaWriteAttributesJson(KafkaFdwModifyState *festate, TupleTableSlot *slot)
{
    /* much like json_build_object */
    ListCell *  lc;
    int         pindex   = 0;
    const char *sep      = ", ";
    char **     attnames = festate->attnames;
    StringInfo  result   = &festate->attribute_buf;

    DEBUGLOG("%s", __func__);

    appendStringInfoCharMacro(result, '{');
    foreach (lc, festate->attnumlist)
    {
        bool  isnull;
        int   attnum = lfirst_int(lc);
        Datum value  = slot_getattr(slot, attnum, &isnull);

        DEBUGLOG("attname %s", *attnames);
        DEBUGLOG("typeoid %u", *festate->typioparams);

        if (pindex > 0)
            appendStringInfoString(result, sep);

        appendStringInfoCharMacro(result, '"');
        appendStringInfoString(result, attnames[pindex]);
        appendStringInfoCharMacro(result, '"');

        appendStringInfoString(result, " : ");

        add_json(value, isnull, result, festate->typioparams[pindex], false);

        pindex++;
    }

    appendStringInfoCharMacro(result, '}');
}
/*












*/
