#include <postgres.h>
#include <fmgr.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <nodes/value.h>
#include <utils/memutils.h>
#include <utils/tuplesort.h>

typedef struct MedianState
{
	Tuplesortstate *sort_state;
	int64		num_elems;
	Oid			element_type;
}			MedianState;

static Datum calculate_median(MedianState * state);

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	MemoryContext old_context;
	Oid			oper_oid;
	Oid			collation_oid;
	Oid			element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MedianState *median_state;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	if (!OidIsValid(element_type))
		elog(ERROR, "could not determine data type of input");

	oper_oid = OpernameGetOprid(list_make1(makeString("<")), element_type, element_type);

	if (!OidIsValid(oper_oid))
		elog(ERROR, "input data type cannot be sorted");

	if (PG_ARGISNULL(0))
	{
		collation_oid = CollationGetCollid("en_US");
		median_state = (MedianState *) palloc0(sizeof(MedianState));
		median_state->element_type = element_type;
		old_context = MemoryContextSwitchTo(MemoryContextGetParent(agg_context));
		median_state->sort_state = tuplesort_begin_datum(element_type, oper_oid, collation_oid, false, 64 * (int64) 1024, NULL, true);
		MemoryContextSwitchTo(old_context);
	}
	else
		median_state = (MedianState *) PG_GETARG_POINTER(0);

	/* Load the datum into the tuplesort object, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		tuplesort_putdatum(median_state->sort_state, PG_GETARG_DATUM(1), false);
		median_state->num_elems++;
	}

	PG_RETURN_POINTER(median_state);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	MedianState *median_state;
	Datum		median;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
	{
		median_state = (MedianState *) PG_GETARG_POINTER(0);
		if (median_state->num_elems == 0)
			PG_RETURN_NULL();

		tuplesort_performsort(median_state->sort_state);
	}

	median = calculate_median(median_state);
	tuplesort_end(median_state->sort_state);

	return median;
}

static Datum
calculate_median(MedianState * state)
{
	Datum		m1 = (Datum) 0;
	Datum		m2 = (Datum) 0;
	Datum		median = (Datum) 0;
	bool		is_null;

	Assert(state);

	tuplesort_skiptuples(state->sort_state, state->num_elems / 2, true);
	tuplesort_getdatum(state->sort_state, true, &m2, &is_null, NULL);

	if (state->num_elems & 0x1)
		PG_RETURN_DATUM(m2);

	/* even number of values */
	tuplesort_restorepos(state->sort_state);
	tuplesort_skiptuples(state->sort_state, state->num_elems / 2 - 1, true);
	tuplesort_getdatum(state->sort_state, true, &m1, &is_null, NULL);

	switch (state->element_type)
	{
		case INT2OID:
			PG_RETURN_INT16((DatumGetInt16(m1) + DatumGetInt16(m2)) / 2.0);
			break;
		case INT4OID:
			PG_RETURN_INT32((DatumGetInt32(m1) + DatumGetInt32(m2)) / 2.0);
			break;
		case INT8OID:
			PG_RETURN_INT64((DatumGetInt64(m1) + DatumGetInt64(m2)) / 2.0);
			break;
		case FLOAT4OID:
			PG_RETURN_FLOAT4((DatumGetFloat4(m1) + DatumGetFloat4(m2)) / 2.0);
			break;
		case FLOAT8OID:
			PG_RETURN_FLOAT8((DatumGetFloat8(m1) + DatumGetFloat8(m2)) / 2.0);
			break;
		default:
			median = m1;
			break;
	}

	PG_RETURN_DATUM(median);
}
