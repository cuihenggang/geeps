#ifndef __internal_config_hpp__
#define __internal_config_hpp__

/*
 * Copyright (C) 2013 by Carnegie Mellon University.
 */

// Configuration internal to the module

#define BIG_ITER         10000000
#define MAX_CLOCK        BIG_ITER
#define INITIAL_DATA_AGE -BIG_ITER
#define INITIAL_CLOCK    0
#define BIG_STALENESS    2 * BIG_ITER
/* The staleness to guarantee a local read.
 * We assume we won't have more than BIG_ITER iterations, and
 * the initial value for cache age is -BIG_ITER, so if we use
 * 2*BIG_ITER as the staleness, we can guarantee that we don't
 * need to fetch data from the server.
 */

#define INCREMENT_TIMING_FREQ 1
#define READ_TIMING_FREQ      1
#define SET_ROW_TIMING_FREQ   1
// #define INCREMENT_TIMING_FREQ 1000
// #define READ_TIMING_FREQ      1000
// #define SET_ROW_TIMING_FREQ   1000

#define ITERATE_CMD      1
#define OPSEQ_CMD        1

#define OP_BUFFER_SIZE   10

#endif  // defined __internal_config_hpp__
