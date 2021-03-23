

#include "ttlbucket.h"


#define SANITY_CHECK(it)                                                       \
    do {                                                                       \
        ASSERT(it->magic == ITEM_MAGIC);                                       \
        ASSERT(*(uint64_t *)(seg_get_data_start(it->seg_id)) == SEG_MAGIC);    \
    } while (0)


//extern struct seg_heapinfo heap;
extern struct ttl_bucket ttl_buckets[MAX_N_TTL_BUCKET];


static inline void
verify_ttlbucket_seg_list(void)
{
    int i;
    struct seg *seg;
    int32_t seg_id;

    pthread_mutex_lock(&heap.mtx);

    for (i = 0; i < MAX_N_TTL_BUCKET; i++) {
        seg_id = ttl_buckets[i].first_seg_id;
        if (seg_id == -1) {
            continue;
        }
        while (seg_id >= 0) {
            seg = &heap.segs[seg_id];
            ASSERT(seg->ttl == ttl_buckets[i].ttl);
            if (seg->next_seg_id != -1) {
                ASSERT(heap.segs[seg->next_seg_id].prev_seg_id == seg_id);
            }
            seg_id = seg->next_seg_id;
        }
    }

    pthread_mutex_unlock(&heap.mtx);
}


/*
 * verify the integrity of segments, items and hashtable
 */

static void
verify_objstore(void)
#if defined CC_ASSERT_PANIC || defined CC_ASSERT_LOG
{
    int32_t seg_id;
    struct seg *seg;
    uint8_t *seg_data, *curr;
    struct item *it, *it2;

    for (seg_id = 0; seg_id < heap.max_nseg; seg_id++) {
        seg = &heap.segs[seg_id];
        if (seg->next_seg_id == -1 || !seg_is_accessible(seg_id))
            continue;
        ASSERT(seg->seg_id == seg_id);
        seg_data = curr = get_seg_data_start(seg_id);

        ASSERT(*(uint64_t *)(curr) == SEG_MAGIC);
        curr += sizeof(uint64_t);

        ASSERT(seg->r_refcount < 64);
        ASSERT(seg->w_refcount < 64);

        while (curr - seg_data < seg->write_offset) {
            it = (struct item *)curr;
            if (it->klen == 0 && it->vlen == 0)
                break;

            ASSERT(it->magic == ITEM_MAGIC);
            ASSERT(item_ntotal(it) > 0);

            if (it->deleted) {
                curr += item_ntotal(it);
                continue;
            }

            struct bstring key = {.data = item_key(it), .len = item_nkey(it)};
            it2 = item_get(&key, NULL, true);
            if (it2 != NULL) {
                /* item might be deleted */
                ASSERT(item_nkey(it) == item_nkey(it2));
                ASSERT(item_nval(it) == item_nval(it2));
                item_release(it2);
            }

            curr += item_ntotal(it);
        }
    }
#endif
}


//static inline int cmp(const void *d1, const void *d2){
//    return *(uint16_t *) d2 - *(uint16_t *) d1;
//}
////static inline int print_n_hit_distr(int32_t seg_id) {
////    static char buffer[1048576];
////    int pos = 0;
////    uint16_t n_hit_cnt[131072];
////    struct seg *seg = &heap.segs[seg_id];
////    uint8_t *seg_data = seg_get_data_start(seg_id);
////
////    memcpy(n_hit_cnt, heap.segs[seg_id].active_obj, sizeof(uint16_t) * 131072);
////    qsort(n_hit_cnt, 131072, sizeof(uint16_t), cmp);
////    pos += sprintf(buffer, "seg id %8d, create_at %8d, age %8d/%8d, ttl %8d %8d items, ", seg_id,
////            heap.segs[seg_id].create_at,
////            time_proc_sec() - heap.segs[seg_id].create_at,
////            time_proc_sec() - heap.segs[seg_id].merge_at,
////            heap.segs[seg_id].ttl,
////            heap.segs[seg_id].n_item);
////    int i;
////    int n_active = 0;
////    for (i = 0; i < 131072; i++) {
////        if (heap.segs[seg_id].active_obj[i] != 0 && !((struct item*) (seg_data+8*i))->deleted ) {
////            n_active += 1;
////        }
////    }
////    pos += sprintf(buffer + pos, "%d active, 0.2 cut off freq %d: ", n_active,
////            n_hit_cnt[(int)(heap.segs[seg_id].n_item * 0.2)]);
////
////    for (i = 0; i < 240; i++) {
////        if (n_hit_cnt[i] == 0) {
////            break;
////        }
////        pos += sprintf(buffer + pos, "%4d, ", n_hit_cnt[i]);
////    }
////    pos += sprintf(buffer + pos, "\n");
//////    printf("%s", buffer);
////
////    FILE *f = fopen("merge_info", "a");
////    fwrite(buffer, pos, 1, f);
////
////    int cutoff = n_hit_cnt[(int)(0.4 * seg->n_item)]-1;
////    int n_chosen = 0;
////    for (i = 0; i < 131072; i++) {
////        if (heap.segs[seg_id].active_obj[i] > cutoff && !((struct item*) (seg_data+8*i))->deleted ) {
////            n_chosen += 1;
////        }
////    }
////    log_info("cutoff %d, active %d, %d sl", cutoff, n_active, n_chosen);
////
////    return cutoff;
////}
//#endif
//
//
//static inline bool
//_check_merge_seg1(void)
//{
//    struct seg *seg;
//    int32_t seg_id;
//    static int32_t last_merged_seg_id = -1;
//
//    if (heap.n_free_seg > 8) {
//        return false;
//    }
//
//    /* scan through all seg_id instead of going down ttl_bucket seg list
//     * allows us not to use lock */
//    for (seg_id = last_merged_seg_id + 1; seg_id != last_merged_seg_id; seg_id++) {
//        if (seg_id >= heap.max_nseg) {
//            seg_id = 0;
////            merge_epoch += 1;
//        }
//        seg = &heap.segs[seg_id];
//        if (seg_mergeable(seg) && seg_mergeable(&heap.segs[seg->next_seg_id])) {
//            last_merged_seg_id = seg_id;
//
//            FILE *f = fopen("merge_info", "a");
//            fprintf(f, "start seg %d\n", seg->seg_id);
//
//            merge_segs(seg->seg_id, -1);
//            return true;
//        }
//    }
//    ASSERT(0);
//}
//
//
//
//
//
//
//



static inline void
print_free_seg_list(void)
{
    log_debug(" free seg: ");

    if (heap.free_seg_id == -1) {
        return;
    }

    int seg_id = heap.free_seg_id;
    while (seg_id != -1) {
        SEG_PRINT(seg_id, "", log_debug);
        seg_id = heap.segs[seg_id].next_seg_id;
    }
}


static inline void
print_ttlbucket_seg_list(struct ttl_bucket *ttl_bucket)
{
    log_debug(" ttl bucket (ttl %8d): ", ttl_bucket->ttl);

    if (ttl_bucket->first_seg_id == -1) {
        return;
    }

    int seg_id = ttl_bucket->first_seg_id;
    while (seg_id != -1) {
        SEG_PRINT(seg_id, "", log_debug);
        seg_id = heap.segs[seg_id].next_seg_id;
    }
}

