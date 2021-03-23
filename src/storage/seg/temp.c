/* merge two consecutive segs */
void merge_seg(int32_t seg_id1, int32_t seg_id2) {

    int accessible, evictable;
    struct seg *seg1 = &heap.segs[seg_id1];
    struct seg *seg2 = &heap.segs[seg_id2];

    ASSERT(seg1->next_seg_id == seg_id2);

    /* prevent the seg from being evicted */
    evictable = __atomic_exchange_n(&(seg1->evictable), 0, __ATOMIC_RELAXED);
    if (evictable == 0) {
        /* being evicted by another thread */
        return;
    }

    evictable = __atomic_exchange_n(&(seg2->evictable), 0, __ATOMIC_RELAXED);
    if (evictable == 0) {
        /* being evicted by another thread */
        return;
    }

    int32_t ttl_bucket_idx = find_ttl_bucket_idx(seg1->ttl);
    struct ttl_bucket *ttl_bucket = &ttl_buckets[ttl_bucket_idx];


//    int32_t new_seg_id = seg_get_new();
    int32_t new_seg_id = _seg_get_from_free_pool(true);
    _seg_init(new_seg_id);
    struct seg *new_seg = &heap.segs[new_seg_id];

    ASSERT(new_seg->evictable == 0);
    /* make sure this will not be picked for eviction */
    ASSERT(new_seg->next_seg_id == -1);

    new_seg->create_at = seg1->create_at;
    new_seg->ttl = seg1->ttl;

    _seg_copy(new_seg_id, seg_id1);
    accessible = __atomic_exchange_n(&(seg1->accessible), 0, __ATOMIC_RELAXED);
    ASSERT(accessible == 1);

    _seg_copy(new_seg_id, seg_id2);
    accessible = __atomic_exchange_n(&(seg2->accessible), 0, __ATOMIC_RELAXED);
    ASSERT(accessible == 1);

    _seg_wait_refcnt(seg_id1);
    _seg_wait_refcnt(seg_id2);

    /* change ttl bucket */
    int status = pthread_mutex_lock(&heap.mtx);
    ASSERT(status == 0);

    int32_t prev_seg_id = seg1->prev_seg_id;
    int32_t next_seg_id = seg2->next_seg_id;

    new_seg->prev_seg_id = prev_seg_id;
    new_seg->next_seg_id = next_seg_id;

    /* we should not merge the last seg */
    ASSERT(next_seg_id != -1);

    if (prev_seg_id == -1) {
        ASSERT(ttl_bucket->first_seg_id == seg_id1);

        ttl_bucket->first_seg_id = new_seg_id;
    } else {
        heap.segs[prev_seg_id].next_seg_id = new_seg_id;
    }

    heap.segs[next_seg_id].prev_seg_id = new_seg_id;

    ttl_bucket->n_seg -= 1;

    seg_return_seg(seg_id1);
    seg_return_seg(seg_id2);

    pthread_mutex_unlock(&heap.mtx);

    /* in seg_copy, we could copy over unused bytes */
    memset(seg_get_data_start(new_seg_id) + new_seg->write_offset,
            0, heap.seg_size - new_seg->write_offset);

    new_seg->evictable = 1;

    log_info("merge seg %d and %d to seg %d in ttl bucket %d first %d last %d",
            seg_id1, seg_id2, new_seg_id, ttl_bucket_idx,
            ttl_bucket->first_seg_id, ttl_bucket->last_seg_id);
}




void
hashtable_put(struct item *it, uint64_t seg_id, uint64_t offset)
{
    struct item *oit;
    uint64_t hv = GET_HV(item_key(it), it->klen);
    uint64_t tag = GET_TAG(hv);
    uint64_t *bucket = GET_BUCKET(hv);
    uint8_t *bucket_u8 = (uint8_t *)bucket;

    /* 16-bit tag, 28-bit seg id, 20-bit offset (in the unit of 8-byte) */
    uint64_t item_info, insert_item_info = tag;
    insert_item_info |= (seg_id << 20u) | offset;
    bool inserted = false, deleted = false;

    /* 8-bit lock, 8-bit bucket array cnt, 16-bit unused, 32-bit cas */
    uint64_t bucket_stat;

    //    _insert_item_in_bucket_array(bucket + 1, BUCKET_SIZE - 1, it, tag,
    //            insert_item_info, &inserted, &deleted);
    //
    //    if (deleted) {
    //        ASSERT(inserted);
    //
    //        return;
    //    }
    //
    //    /* we have to scan the second array */
    //    bucket_stat = __atomic_load_n(bucket, __ATOMIC_RELAXED);
    //
    //    uint8_t n_array = __atomic_load_n(bucket_u8, __ATOMIC_RELAXED);
    //    uint64_t *bucket_array;
    //
    //    if (n_array == 0) {
    //        if (inserted) {
    //            return;
    //        }
    //
    //        bucket_array = cc_zalloc(sizeof(uint64_t) * BUCKET_SIZE);
    //        bucket_array[0] = insert_item_info;
    //    }
}





























///* the segment points to by seg_id_pmem is empty and ready to use */
// static bool
// migrate_dram_to_pmem(uint64_t seg_id_dram, uint64_t seg_id_pmem)
//{
//    /* first thing, we lock the dram seg to prevent future access to it */
//    /* TODO(jason): change function signature to use struct seg instead of
//     * seg_id */
//
//    log_verb("migrate DRAM seg %" PRIu32 " to PMem seg %" PRIu32, seg_id_dram,
//            seg_id_pmem);
//
//    if (!_seg_lock(seg_id_dram)) {
//        return false;
//    }
//
//    struct item *oit, *nit;
//    struct seg *seg_dram = &heap.segs[seg_id_dram];
//    struct seg *seg_pmem = &heap.segs[seg_id_pmem];
//    uint8_t *seg_dram_data = seg_get_data_start(seg_id_dram);
//    uint8_t *seg_pmem_data = seg_get_data_start(seg_id_pmem);
//
//    cc_memcpy(seg_dram, seg_pmem, sizeof(struct seg));
//    cc_memcpy(seg_pmem_data, seg_dram_data, heap.seg_size);
//
//    seg_pmem->refcount = 0;
//    seg_pmem->locked = 0;
//    seg_pmem->seg_id = seg_id_pmem;
//    seg_pmem->in_pmem = 1;
//
//    /* relink hash table, this needs to be thread-safe
//     * we don't need lock here,
//     * since we require hashtable update to be atomic
//     */
//    uint32_t offset = 0;
//#if defined CC_ASSERT_PANIC || defined CC_ASSERT_LOG
//    ASSERT(*(uint64_t *)(seg_dram_data + offset) == SEG_MAGIC);
//    offset += sizeof(uint64_t);
//#endif
//    while (offset < heap.seg_size) {
//        oit = (struct item *)(seg_dram_data + offset);
//        nit = (struct item *)(seg_pmem_data + offset);
//        item_relink(oit, nit);
//    }
//    _seg_wait_refcnt(seg_id_dram);
//    return true;
//}




//void
//item_free(struct item *it, uint32_t seg_id, uint32_t sz)
//{
//    struct seg *seg = item_to_seg(it);
//    /* this is protected by hashtable lock */
//    //    seg->occupied_size -= item_ntotal(it);
//    //    seg->n_item -= 1;
//    __atomic_sub_fetch(&seg->occupied_size, item_ntotal(it), __ATOMIC_SEQ_CST);
//    __atomic_sub_fetch(&seg->n_item, 1, __ATOMIC_SEQ_CST);
//
//    /* TODO(jason): what is the overhead of tracking PERTTL metric
//     * consider removing the metrics since we can get them from
//     * iterating over all seg headers */
//    uint32_t sz = item_ntotal(it);
//    uint16_t ttl_bucket_idx = find_ttl_bucket_idx(seg->ttl);
//
//    DECR(seg_metrics, item_curr);
//    DECR_N(seg_metrics, item_curr_bytes, sz);
//
//    PERTTL_DECR(ttl_bucket_idx, item_curr);
//    PERTTL_DECR_N(ttl_bucket_idx, item_curr_bytes, sz);
//}


/*
 * this assumes the inserted item is not in the hashtable
 *
 * we do not need this under multi-threading because we cannot guarantee the
 * item checked is not in the hashtable without lock
 */
// void
// item_insert(struct item *it)
//{
//    ASSERT(hashtable_get(item_key(it), item_nkey(it), hash_table, NULL) ==
//    NULL);
//
//    hashtable_put(it, hash_table);
//
//    _item_w_deref(it);
//
//    log_verb("insert it %p (%.*s) of size %zu in seg %" PRIu32,
//        it, it->klen, item_key(it), item_ntotal(it), it->seg_id);
//}

/*
 * this assumes the updated item is in the hashtable,
 * we delete the item first (update metrics), then insert into hashtable
 */
// void
// item_update(struct item *nit)
//{
//    hashtable_delete(item_key(nit), item_nkey(nit), hash_table, false);
//
//    hashtable_put(nit, hash_table);
//
//    _item_w_deref(nit);
//
//    log_verb("update it %p (%.*s) of size %zu in seg %" PRIu32, nit,
//    nit->klen,
//            item_key(nit), item_ntotal(nit), nit->seg_id);
//}




#ifdef do_not_define
/* given an old item, recreate a new item */
item_rstatus_e
item_recreate(struct item **nit_p, struct item *oit, delta_time_i ttl,
        delta_time_i create_at)
{
    item_rstatus_e status;
    struct item *it;

    status = _item_alloc(nit_p, oit->klen, oit->vlen, oit->olen, ttl);
    if (status != ITEM_OK) {
        log_debug("item reservation failed");
        return status;
    }

    it = *nit_p;

    it->olen = oit->olen;
    if (it->olen > 0) {
        cc_memcpy(item_optional(it), item_optional(oit), oit->olen);
    }
    cc_memcpy(item_key(it), item_key(oit), oit->klen);
    it->klen = oit->klen;
    cc_memcpy(item_val(it), item_val(it), oit->vlen);
    it->vlen = oit->vlen;

    log_verb("recreate it %p (%.*s) of size %" PRIu32 " in seg %" PRIu16, it,
            it->klen, item_key(it), item_ntotal(it), it->seg_id);

    return ITEM_OK;
}
#endif







#ifdef do_not_define
/*
 * Recreate items from a persisted segment, used when shared memory or
 * external storage is enabled
 * new_seg points to a dynamically allocated in-DRAM data structure that
 * holds the current segment, we copy the valid item from seg_old to
 */
static void
_seg_recreate_items(uint32_t seg_id, struct seg *new_seg)
{
    struct item *it;
    uint32_t i;

    /* we copy the seg header from shared memory/PMem/external storage
     * to avoid repeated small read from PMem/external storage */
    struct seg *seg_old_p =
            (struct seg *)(heap.base + sizeof(struct seg) * seg_id);
    struct seg seg_old = *seg_old_p;
    uint8_t *data_start = seg_old.data_start;

    //  p = &segclass[seg->id];
    //  p->nfree_item = p->nitem;
    for (i = 0; i < p->nitem; i++) {
        it = _seg_to_item(seg, i, p->size);
        if (it->is_linked) {
            p->next_item_in_seg = (struct item *)&seg->data[0];
            INCR(seg_metrics, item_curr);
            INCR(seg_metrics, item_alloc);
            PERSEG_INCR(seg->id, item_curr);
            item_relink(it);
            if (--p->nfree_item != 0) {
                p->next_item_in_seg =
                        (struct item *)((char *)p->next_item_in_seg + p->size);
            } else {
                p->next_item_in_seg = NULL;
            }
        } else if (it->in_freeq) {
            _seg_put_item_into_freeq(it, seg->id);
        } else if (it->klen && !_seg_check_no_refcount(seg)) {
            /* before reset, item could be only reserved
             * ensure that seg has a reserved item(s)
             */
            item_release(&it);
        }
    }
}


/* given the new mapping address, recover the memory address in seg headers */
static rstatus_i
_seg_recover_seg_header(void)
{
    uint32_t i;

    for (i = 0; i < heap.max_nseg; i++) {
        heap.persisted_seg_hdr[i].
    }
}

/* recover the items on the old segment,
 * this comes with compaction, meaning deleted item and expired item are not
 * recovered
 */
static rstatus_i
_seg_recover_one_seg(uint32_t old_seg_id)
{
    uint16_t ttl_bucket_idx;
    struct ttl_bucket *ttl_bucket;
    uint8_t *curr, *tmp_seg_data = NULL;
    struct item *oit, *nit;
    struct seg *seg; /* the seg we write re-created item to */
    struct seg *old_segs = heap.persisted_seg_hdr;
    struct seg *old_seg = &heap.persisted_seg_hdr[old_seg_id];
    uint8_t *old_seg_data;
    if (old_seg_id >= heap.max_nseg) {
        old_seg_data =
                heap.base_pmem + heap.seg_size * (old_seg_id - heap.max_nseg);
    } else {
        old_seg_data = heap.base + heap.seg_size * old_seg_id;
    }

    /* we may use this segment for newly rewritten data, so we make a copy
     * of the segment data before we recover
     */
    tmp_seg_data = cc_zalloc(heap.seg_size);
    cc_memcpy(tmp_seg_data, old_seg_data, heap.seg_size);
    curr = tmp_seg_data;

    /* clear the old seg */

    while (curr < tmp_seg_data + heap.seg_size) {
#    if defined CC_ASSERT_PANIC || defined CC_ASSERT_LOG
        ASSERT(*(uint64_t *)curr == SEG_MAGIC);
        curr += 8;
#    endif
        oit = (struct item *)curr;
        curr += item_ntotal(oit);
        if (!oit->valid) {
            continue;
        }

        if (old_seg_id != oit->seg_id) {
            log_warn("detect seg_id inconsistency in seg_recovery\n");
            return CC_ERROR;
        }

        if (old_seg->create_at + old_seg->ttl < time_proc_sec()) {
            /* expired */
            continue;
        }

        /* find the segment where we can write */
        nit = ttl_bucket_reserve_item(
                old_seg->ttl, item_ntotal(oit), false, NULL, NULL);
        if (nit == NULL) {
            /* if current ttl_bucket does not have a segment allocated
             * or the allocated seg is full, we will reuse this old_sg */


            new_seg->ttl = ttl;
            TAILQ_INSERT_TAIL(&ttl_bucket->seg_q, new_seg, seg_tqe);
            curr_seg->sealed = 1;

            PERTTL_INCR(ttl_bucket_idx, seg_curr);

            seg_data_start = seg_get_data_start(curr_seg->seg_id);
            offset = __atomic_fetch_add(
                    &(curr_seg->write_offset), sz, __ATOMIC_SEQ_CST);

            uint32_t occupied_size = __atomic_add_fetch(
                    &(curr_seg->occupied_size), sz, __ATOMIC_SEQ_CST);
            ASSERT(occupied_size <= heap.seg_size);

            ASSERT(seg_data_start != NULL);
            it = (struct item *)(seg_data_start + offset);
            if (seg_p) {
                *seg_p = curr_seg;
            }

            PERTTL_INCR(ttl_bucket_idx, item_curr);
            PERTTL_INCR_N(ttl_bucket_idx, item_curr_bytes, sz);
        }

        ttl_bucket_idx = find_ttl_bucket_idx(old_seg->ttl);
        ttl_bucket = &ttl_buckets[ttl_bucket_idx];
        seg = TAILQ_LAST(&ttl_bucket->seg_q, seg_tqh);
        bool reuse_old_seg = false;
        if (seg == NULL) {
            reuse_old_seg = true;
        } else {
            uint8_t *seg_data_start = seg_get_data_start(seg->seg_id);
            uint32_t offset = __atomic_fetch_add(
                    &(seg->write_offset), item_ntotal(oit), __ATOMIC_SEQ_CST);
        }

        new_seg->create_at = old_seg->create_at;

        struct seg *curr_seg, *new_seg;


        /* recreate item on the heap */
        item_recreate(&nit, oit, old_seg->ttl, old_seg->create_at);
        /* insert into hash table */
        key = (struct bstring){nit->klen, item_key(nit)};
        item_insert(nit, &key);
    }

    qsort()
}

/*
 * Recreate segs structure when persistent memory/external features are enabled
 * first we build expiration tree and segment score tree
 * second we check whether there are any expired segment
 * third, we start with the least occupied seg, recreate items in the seg
 *
 * NOTE: we might lose one (or more) segments of items if the heap is full
 *
 * time_since_create: time since the datapool is created
 */
static rstatus_i
_seg_recovery(uint8_t *base, uint32_t max_nseg)
{
    uint32_t i;
    struct bstring key;
    uint8_t *curr;
    struct seg *new_seg, *old_seg;
    struct seg *old_segs = heap.persisted_seg_hdr;
    struct item *oit, *nit; /* item on the old segment and recreated item */
    uint32_t n_seg_to_recover = max_nseg;

    /* we need to update old_set ttl first */
    /* TODO(jason): ASSUME we have updated time_started and proc_sec when
     * loading datapool */


    /* copy the seg to DRAM, then scan the copied seg
     *
     * when DRAM+PMem tiered storage is used,
     * we discard all objects in DRAM
     * this may or may not be wise decision, but we do this for now
     *
     * */

    /* we need to allocate new seg from start of the datapool
     * so we don't need to explicitly change ttl_bucket_reserve_item
     * but then we need to backup the overwritten data */
    delta_time_i create_at_earliest;
    uint32_t earliest_seg_id = UINT32_MAX;
    while (n_seg_to_recover > 0) {
        /* TODO (jason) currently a O(N^2) metadata scanning,
         * might want to change to O(NlogN), but given this is one-time
         * initialization and the number of segments are limited,
         * keep this for now
         */
        create_at_earliest = time_proc_sec() + 1;
        earliest_seg_id = UINT32_MAX;
        curr = base;
        /* find the seg with the earliest creation time */
        for (i = 0; i < max_nseg; i++) {
            if (old_segs[i].recovered == 1) {
                continue;
            }
            oit = (struct item *)curr;
            if (old_segs[i].create_at < create_at_earliest) {
                create_at_earliest = old_segs[i].create_at;
                earliest_seg_id = i;
            }
            curr += heap.seg_size;
        }
        _seg_recover_one_seg(earliest_seg_id);
        old_segs[earliest_seg_id].recovered = 1;
        n_seg_to_recover -= 1;
    }

    /* all segs have been recovered, hashtable has been rebuilt */


    return CC_OK;
}
#endif





static inline void
_insert_item_in_bucket_array(uint64_t *array, int n_array_item, struct item *it,
                             uint64_t tag, uint64_t insert_item_info, bool *inserted, bool *deleted)
{
    uint64_t item_info;

    for (int i = 0; i < n_array_item; i++) {
        item_info = __atomic_load_n(&array[i], __ATOMIC_ACQUIRE);

        if (GET_TAG(item_info) != tag) {
            if (!*inserted && item_info == 0) {
                *inserted = CAS_SLOT(array + i, &item_info, insert_item_info);
                if (*inserted) {
                    /* we have inserted, so when we encounter old entry,
                     * just reset the slot */
                    insert_item_info = 0;
                }
            }
            continue;
        }
        /* a potential hit */
        if (!_same_item(item_key(it), it->klen, item_info)) {
            continue;
        }
        /* we have found the item, now atomic update */
        *deleted = CAS_SLOT(array + i, &item_info, insert_item_info);
        if (*deleted) {
            /* update successfully */
            *inserted = true;
            return;
        }

        /* the slot has changed, double-check this updated item */
        if (item_info == 0) {
            /* the item is evicted */
            *inserted = CAS_SLOT(array + i, &item_info, insert_item_info);
            /* whether it succeeds or fails, we return,
             * see below for why we return when it fails, as an alternative
             * we can re-start the put here */
            return;
        }

        if (!_same_item(item_key(it), it->klen, item_info)) {
            /* original item evicted, a new item is inserted - rare */
            continue;
        }
        /* the slot has been updated with the same key, replace it */
        *deleted = CAS_SLOT(array + i, &item_info, insert_item_info);
        if (*deleted) {
            /* update successfully */
            *inserted = true;
            return;
        } else {
            /* AGAIN? this should be very rare, let's give up
             * the possible consequences of giving up:
             * 1. current item might not be inserted, not a big deal
             * because at such high concurrency, we cannot tell whether
             * current item is the most updated one or the the one in the
             * slot
             * 2. current item is inserted in an early slot, then we
             * will have two entries for the same key, this if fine as
             * well, because at eviction time, we will remove them
             **/
            return;
        }
    }
}






bool
hashtable_evict0(const char *oit_key, const uint32_t oit_klen,
                 const uint64_t seg_id, const uint64_t offset)
{
    INCR(seg_metrics, hash_remove);

    bool deleted = false;

    uint64_t hv = GET_HV(oit_key, oit_klen);
    uint64_t tag = CAL_TAG_FROM_HV(hv);
    uint64_t *bucket = GET_BUCKET(hv);
    uint64_t *array = bucket;

    /* 16-bit tag, 28-bit seg id, 20-bit offset (in the unit of 8-byte) */
    uint64_t item_info;
    uint64_t oit_info = tag | (seg_id << 20u) | (offset >> 3u);

    /* we only want to delete entries of the object as old as oit,
     * so we need to find oit first, once we find it, we will delete
     * all entries of this key */
    bool delete_rest = false;

    lock(bucket);

    int extra_array_cnt = GET_ARRAY_CNT(bucket);
    int array_size;
    do {
        array_size = extra_array_cnt > 0 ? BUCKET_SIZE - 1 : BUCKET_SIZE;

        for (int i = 0; i < array_size; i++) {
            if (array == bucket && i == 0) {
                continue;
            }

            item_info = array[i];
            if (GET_TAG(item_info) != tag) {
                continue;
            }
            /* a potential hit */
            if (!_same_item(oit_key, oit_klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }

            if (item_info == oit_info) {
                _item_free(item_info, false);
                deleted = true;
                delete_rest = true;
                array[i] = 0;
            } else {
                if (delete_rest) {
                    _item_free(item_info, true);
                    array[i] = 0;
                } else {
                    /* this is the newest entry */
                    delete_rest = true;
                }
            }
        }
        extra_array_cnt -= 1;
        array = (uint64_t *)(array[BUCKET_SIZE - 1]);
    } while (extra_array_cnt >= 0);

    unlock(bucket);

    return deleted;
}











