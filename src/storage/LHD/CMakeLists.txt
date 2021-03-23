set(SOURCE
    hashtable.c
    item.c
    slab.c)

add_library(LHD ${SOURCE})
target_link_libraries(LHD datapool)
