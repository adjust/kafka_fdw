#include "postgres.h"
#include "executor/tuptable.h"

void kafka_custom_msglen(const char *msgstr, size_t msg_len, TupleTableSlot *slot, int pidx, int oidx);

/*
 * example for a custom scan implementation
 * we don't do anything with the message itself here just
 * callback the message size into the Datums array
 */
void
kafka_custom_msglen(const char *msgstr, size_t msg_len, TupleTableSlot *slot, int pidx, int oidx)
{
	Datum  *values = slot->tts_values;
	bool   *nulls  = slot->tts_isnull;
	int 	num_attr = slot->tts_tupleDescriptor->natts;

    for (int i = 0; i < num_attr; i++)
    {
        if (!(i == pidx || i == oidx))
        {
            nulls[i]  = false;
            values[i] = Int32GetDatum((int32) msg_len);
        }
    }
}