//
// Created by Juncheng Yang on 12/7/21.
//

#ifndef PELIKAN_HYPERBOLIC_H
#define PELIKAN_HYPERBOLIC_H

#include "item.h"

static inline float cal_hyperbolic_score(struct item *it) {
    return -(double) (it->freq)/(time_proc_sec() - it->create_at);
}


#endif //PELIKAN_HYPERBOLIC_H
