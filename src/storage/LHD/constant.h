#pragma once

#define RAND_CHOOSE_N 32

//#define USE_HYPERBOLIC

#define USE_LHD
#define MAX_AGE 20000
#define NUM_LHD_CLASS_PER_NS 16
#define NUM_NAMESPACE 1
#define NUM_LHD_CLASS (NUM_LHD_CLASS_PER_NS * NUM_NAMESPACE)
#define EWMA_DECAY 0.9

#define RECONFIGURE_INTVL 1000000
#define AGE_COARSENING_ERROR_TOLERANCE 0.01



#define REBALANCE_INTVL (500 * 1000)

// #define EVICTION_AGE        1
// #define EVICTION_RATE       2       // this is not right, don't use
// #define REBALANCE_METRIC    EVICTION_AGE

#define USE_RANDOM_EXPIRE    1

#define REAL_COPY
// #define USE_SLAB_REBALANCE	 1



// #define USE_HYPERBOLIC
