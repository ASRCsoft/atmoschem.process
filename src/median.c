#include <postgres.h>
#include <fmgr.h>

PG_MODULE_MAGIC;

#include "mediator.c"

typedef struct median_state {
  Mediator *mediator;
} median_state;


Datum median_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(median_transfn);

Datum
median_transfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  median_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "median_transfn called in non-aggregate context");
  }
  if (PG_ARGISNULL(0)) {
    // if the state hasn't been initialized yet
    state = MemoryContextAlloc(aggContext, sizeof(median_state));
    state->mediator = MediatorNew(15);
  } else {
    state = (median_state *)PG_GETARG_POINTER(0);
  }
  /* MediatorInsert(state->mediator, PG_GETARG_INT32(1)); */
  MediatorInsert(state->mediator, PG_GETARG_FLOAT8(1));
  PG_RETURN_POINTER(state);
}


Datum median_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(median_finalfn);

Datum
median_finalfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext;
  median_state *state;
  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "median_finalfn called in non-aggregate context");
  }
  state = PG_ARGISNULL(0) ? NULL : (median_state *)PG_GETARG_POINTER(0);
  if (state == NULL) PG_RETURN_NULL();
  /* PG_RETURN_INT32(MediatorMedian(state->mediator)); */
  PG_RETURN_FLOAT8(MediatorMedian(state->mediator));
}
