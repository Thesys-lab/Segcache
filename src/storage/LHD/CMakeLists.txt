set(SOURCE
    hashtable.c
    item.c
    slab.c LHD.h hyperbolic.h)

add_library(LHD ${SOURCE})
target_link_libraries(LHD datapool)
