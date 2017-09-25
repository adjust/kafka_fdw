#include "kafka_fdw.h"

/*
 * Parse the char into separate attributes (fields)
 * Returns number of fields
 */
int
KafkaReadAttributesCSV(char *msg, int msg_len, KafkaFdwExecutionState *festate)
{
    char  delimc  = festate->parse_options.delim[0];
    char  quotec  = festate->parse_options.quote[0];
    char  escapec = festate->parse_options.escape[0];
    int   fieldno = 0;
    char *output_ptr;
    char *cur_ptr;
    char *line_end_ptr;

    resetStringInfo(&festate->attribute_buf);
    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.  We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
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
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unterminated CSV quoted field")));

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
