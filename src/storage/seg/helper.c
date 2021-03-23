//
// Created by Juncheng Yang on 2/25/21.
//

change log
    hashtable:
            mark all item deleted in item_free
//            change locking in hashtable_get
            !!! add locks to all hashtable func (get and relink mostly)
//            disable efficient put (no more check)
//            change bucket lock from __ATOMIC_RELEASE and __ATOMIC_REQUIRE to __ATOMIC_RELAXED

    trace_replay:
//            change all op to get;
//            change key to str for debug
//            change ttl to user-specified ttl instead of the one from trace
    seg:
//            enable memset
//            remove deleted item from hash table (this is a must have change)
//            force check hash table when finish removing object from seg

    merge:
//        it->deleted add remove (this is a must have change)
//        not use n_live_item to track the end of seg_copy, use n_rm_item instead


//change item.c the order of add n_total_item and n_live_item
//    add w_refcount assert



USE_FREQ_IN_HASHTABLE:
    FIFO passes
    merge pass


store freq with obj:
    FIFO passes
    merge: pass


note:
    do not set min_mature_time too small, it is possible that a segment is
        evicted and init in one thread before the other thread incr w_refcnt (after incr offset)

    moving hashtable_get lock down will cause seg_id_non_decr != seg_id (both FIFO and merge, and whether freq in hash table or not)
    when using very small cache size, remember to change to not use the TTL from traces so that not tons of buckets are used

to optimize:
    reduce hashtable_get and hashtable_relink lock critical section
    remove memset in seg_init
