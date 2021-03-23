#pragma once

#define RAND_CHOOSE_N 32

#define USE_HYPERBOLIC

//#define USE_LHD
//#define MAX_AGE 200000
//#define RECOMPUTE_INTVL 1000000
//#define AGE_COARSENING_ERROR_TOLERANCE 0.01
//#define USE_AGE_COARSENING

#define REBALANCE_INTVL 2000000

#define EVICTION_AGE        1
#define EVICTION_RATE       2       // this is not right, don't use
#define REBALANCE_METRIC    EVICTION_AGE

#define USE_RANDOM_EXPIRE    1

#define REAL_COPY

//#undef MAX_AGE
//#undef AGE_GRANULARITY
//#undef RECOMPUTE_INTVL
//#undef REBALANCE_INTVL
//
//#define MAX_AGE 2000000
//#define AGE_GRANULARITY 200
//#define RECOMPUTE_INTVL 100000000
//
//
//#define REBALANCE_INTVL 200000000
//
//#define USE_HYPERBOLIC
