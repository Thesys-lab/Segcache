//
// Created by Juncheng Yang on 12/7/21.
//

#ifndef PELIKAN_LHD_H
#define PELIKAN_LHD_H

#include "constant.h"


struct LHD_class {
    float hits[MAX_AGE];
    float evictions[MAX_AGE];
    float hit_density[MAX_AGE];
    float total_hits;
    float total_evictions;
};

extern struct LHD_class LHD_classes[NUM_LHD_CLASS];
extern uint64_t n_req;
extern uint64_t LHD_num_reconfig;
extern unsigned int ageCoarseningShift;
extern float ewmaNumObjects;
extern float ewmaNumObjectsMass;



static inline void init_lhd_class() {
    for (int i = 0; i < NUM_LHD_CLASS; i++) {
        for (int j = 0; j < MAX_AGE; j++) {
            LHD_classes[i].hit_density[j] = 1 * (i + 1) / (j + 1);
        }
    }
}

static void update_lhd_class() {
    for (int i = 0; i < NUM_LHD_CLASS; i++) {
        LHD_classes[i].total_hits = 0;
        LHD_classes[i].total_evictions = 0;

        for (int j = 0; j < MAX_AGE; j++) {
            LHD_classes[i].hits[j] *= EWMA_DECAY;
            LHD_classes[i].evictions[j] *= EWMA_DECAY;

            LHD_classes[i].total_hits += LHD_classes[i].hits[j];
            LHD_classes[i].total_evictions += LHD_classes[i].evictions[j];
        }
    }
}

static void age_coarsening() {
    int num_obj = 0;
    for (int i = SLABCLASS_MIN_ID; i <= profile_last_id; i++) {
        num_obj += slabclasses[i].nitem * slabclasses[i].nslabs;
    }

    ewmaNumObjects *= EWMA_DECAY;
    ewmaNumObjectsMass *= EWMA_DECAY;

    ewmaNumObjects += num_obj;
    ewmaNumObjectsMass += 1.;

    float numObjects = ewmaNumObjects / ewmaNumObjectsMass;
    float optimalAgeCoarsening = 1. * numObjects / (AGE_COARSENING_ERROR_TOLERANCE * MAX_AGE);




    // Simplify. Just do this once shortly after the trace starts and
    // again after 25 iterations. It only matters that we are within
    // the right order of magnitude to avoid tons of overflows.
    if (LHD_num_reconfig == 5 || LHD_num_reconfig == 25 || LHD_num_reconfig == 50 || LHD_num_reconfig == 100) {
        uint32_t optimalAgeCoarseningLog2 = 1;

        while ((1 << optimalAgeCoarseningLog2) < optimalAgeCoarsening) {
            optimalAgeCoarseningLog2 += 1;
        }

        int32_t delta = optimalAgeCoarseningLog2 - ageCoarseningShift;
        ageCoarseningShift = optimalAgeCoarseningLog2;
        log_info("agecoarsening %d", delta);

        // increase weight to delay another shift for a while
        ewmaNumObjects *= 8;
        ewmaNumObjectsMass *= 8;

        // compress or stretch distributions to approximate new scaling
        // regime
        if (delta < 0) {
            // stretch
            for (int i = 0; i < NUM_LHD_CLASS; i++) {
                for (uint64_t a = MAX_AGE >> (-delta); a < MAX_AGE - 1; a++) {
                    LHD_classes[i].hits[MAX_AGE - 1] += LHD_classes[i].hits[a];
                    LHD_classes[i].evictions[MAX_AGE - 1] += LHD_classes[i].evictions[a];
                }
                for (uint64_t a = MAX_AGE - 2; a < MAX_AGE; a--) {
                    LHD_classes[i].hits[a] = LHD_classes[i].hits[a >> (-delta)] / (1 << (-delta));
                    LHD_classes[i].evictions[a] = LHD_classes[i].evictions[a >> (-delta)] / (1 << (-delta));
                }
            }
        } else if (delta > 0) {
            // compress
            for (int i = 0; i < NUM_LHD_CLASS; i++) {
                for (uint64_t a = 0; a < MAX_AGE >> delta; a++) {
                    LHD_classes[i].hits[a] = LHD_classes[i].hits[a << delta];
                    LHD_classes[i].evictions[a] = LHD_classes[i].evictions[a << delta];
                    for (int j = 1; j < (1 << delta); j++) {
                        LHD_classes[i].hits[a] += LHD_classes[i].hits[(a << delta) + j];
                        LHD_classes[i].evictions[a] += LHD_classes[i].evictions[(a << delta) + j];
                    }
                }
                for (uint64_t a = (MAX_AGE >> delta); a < MAX_AGE - 1; a++) {
                    LHD_classes[i].hits[a] = 0;
                    LHD_classes[i].evictions[a] = 0;
                }
            }
        }
    }
}

static void model_hit_density() {
    for (int i = 0; i < NUM_LHD_CLASS; i++) {
        float total_events = LHD_classes[i].hits[MAX_AGE - 1] + LHD_classes[i].evictions[MAX_AGE - 1];
        float total_hits = LHD_classes[i].hits[MAX_AGE - 1];
        float lifetime_unconditioned = total_events;

        for (int j = MAX_AGE - 2; j > 0; j--) {
            total_hits += LHD_classes[i].hits[j];
            total_events += LHD_classes[i].hits[j] + LHD_classes[i].evictions[j];
            lifetime_unconditioned += total_events;

            if (total_events > 1e-5) {
                LHD_classes[i].hit_density[j] = total_hits / lifetime_unconditioned;
            } else {
                LHD_classes[i].hit_density[j] = 0;
            }
        }
    }
}

// returns something like log(maxAge - age)
static inline int lhd_class_id(struct item *it) {
    uint64_t age = it->last_age + it->last_last_age;

    if (age == 0) { return NUM_LHD_CLASS - 1; }
    int log = 0;
    while (age < MAX_AGE && log < NUM_LHD_CLASS - 1) {
        age <<= 1;
        log += 1;
    }

//    return it->ns;

    ASSERT(log < NUM_LHD_CLASS_PER_NS);
    return log;
//    return it->ns * NUM_LHD_CLASS_PER_NS + log;
}

static inline uint64_t lhd_age(struct item *it) {
    uint64_t age = (n_req - it->access_time) >> ageCoarseningShift;
    if (age >= MAX_AGE - 1) {
        return MAX_AGE - 1;
    }

    return age;
}

static inline float lhd_get_hit_density(struct item *it) {
    uint64_t age = lhd_age(it);
    if (age == MAX_AGE - 1) {
        return -1;
    }

    uint32_t cl = lhd_class_id(it);
    float density = LHD_classes[cl].hit_density[age] / item_size(it);

//    if (it.explorer) { density += 1.; }
    return density;
}

static void lhd_reconfigure() {
    LHD_num_reconfig += 1;

    update_lhd_class();

//    float total_hits = 0;
//    float total_evictions = 0;
//    for (int i = 0; i < NUM_LHD_CLASS; i++) {
//        total_hits += LHD_classes[i].total_hits;
//        total_evictions += LHD_classes[i].total_evictions;
//    }

    age_coarsening();

    model_hit_density();
}

#endif //PELIKAN_LHD_H
