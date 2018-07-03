#include "postgres.h"
void kafka_custom_msglen(char *msgstr, size_t msg_len, Datum *values, bool *nulls, int num_attr, int pidx, int oidx);

/*
 * example for a custom scan implementation
 * we don't do anything with the message itself here just
 * callback the message size into the Datums array
 */
void
kafka_custom_msglen(char *msgstr, size_t msg_len, Datum *values, bool *nulls, int num_attr, int pidx, int oidx)
{
    for (int i = 0; i < num_attr; i++)
    {
        if (!(i == pidx || i == oidx))
        {
            nulls[i]  = false;
            values[i] = Int32GetDatum((int32) msg_len);
        }
    }
}