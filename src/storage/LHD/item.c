#include "slab.h"

#include "constant.h"

#include <cc_debug.h>

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

extern delta_time_i max_ttl;
proc_time_i flush_at = -1;

extern pthread_mutex_t item_lock[1u<<16u];
//struct item     tags[100000000];
uint64_t    tag_pos = 0;

static __thread __uint128_t g_lehmer64_state = 1;


static inline uint64_t prand(void) {
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return g_lehmer64_state >> 64u;
}

static uint64_t n_req = 0;
pthread_mutex_t temp_lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t LHD_num_reconfig = 0;
unsigned int ageCoarseningShift = 8;


static struct item * rand_evict(struct slabclass *p);

void lock_item(struct item *it) {
    uint32_t lock_idx = get_hv(item_key(it), it->klen) & 0x0000ffffu;
    struct slabclass *p = &slabclasses[it->id];
    pthread_mutex_lock(&item_lock[lock_idx]);
    pthread_mutex_lock(&(p->lock));
}

void unlock_item(struct item *it) {
    uint32_t lock_idx = get_hv(item_key(it), it->klen) & 0x0000ffffu;
    struct slabclass *p = &slabclasses[it->id];
    pthread_mutex_unlock(&item_lock[lock_idx]);
    pthread_mutex_unlock(&(p->lock));
}

void lock_bucket(const char *key, uint32_t key_len) {
    uint32_t lock_idx = get_hv(key, key_len) & 0x0000ffffu;
    int status = pthread_mutex_lock(&item_lock[lock_idx]);
    ASSERT(status == 0);
}

void unlock_bucket(const char *key, uint32_t key_len) {
    uint32_t lock_idx = get_hv(key, key_len) & 0x0000ffffu;
    int status = pthread_mutex_unlock(&item_lock[lock_idx]);
    ASSERT(status == 0);
}

void lock_slabclass(int id) {
    ASSERT(id >= SLABCLASS_MIN_ID && id <= profile_last_id);
//    log_info("lock slabclass id %d", id);
    int status = pthread_mutex_lock(&(slabclasses[id].lock));
    ASSERT(status == 0);
    ASSERT(pthread_mutex_trylock(&slabclasses[id].lock) != 0);
}

void unlock_slabclass(int id) {
    ASSERT(id >= SLABCLASS_MIN_ID && id <= profile_last_id);
//    log_info("unlock slabclass id %d", id);
    struct slabclass *p = &slabclasses[id];
    int status = pthread_mutex_unlock(&(p->lock));
    ASSERT(status == 0);
}


static void
_item_delete(struct item **it);


static inline bool
_item_expired(struct item *it)
{
    return (it->expire_at < time_proc_sec() || it->create_at <= flush_at);
}

//static inline bool
//_item_expired_tag(int32_t tag_idx)
//{
//    struct item *it = &(tags[tag_idx]);
//    return (it->expire_at < time_proc_sec() || it->create_at <= flush_at);
//}


//static inline void
//_copy_key_item(struct item *nit, struct item *oit)
//{
//    nit->olen = oit->olen;
//    cc_memcpy(item_key(nit), item_key(oit), oit->klen);
//    nit->klen = oit->klen;
//}

void
item_hdr_init(struct item *it, uint32_t offset, uint8_t id)
{
    ASSERT(offset >= SLAB_HDR_SIZE && offset < slab_size);

#if CC_ASSERT_PANIC == 1 || CC_ASSERT_LOG == 1
    it->magic = ITEM_MAGIC;
#endif
    it->offset = offset;
    it->id = id;
    it->is_linked = it->in_freeq = it->is_raligned = 0;
    it->locked = 0;
    it->tag_idx = __atomic_fetch_add(&tag_pos, 1, __ATOMIC_RELAXED);
#ifdef USE_LHD
    it->v_create_time = 0;
#elif defined(USE_HYPERBOLIC)
    it->freq = 0;
#endif
}

static inline void
_item_reset(struct item *it)
{
    it->is_linked = 0;
    it->in_freeq = 0;
    it->is_raligned = 0;
    it->refcount = 0;
    it->vlen = 0;
    it->klen = 0;
    it->olen = 0;
    it->expire_at = 0;
    it->create_at = 0;
    it->locked = 0;

#ifdef USE_TAG
    tags[it->tag_idx].is_linked = 0;
    tags[it->tag_idx].in_freeq = 0;
    tags[it->tag_idx].is_raligned = 0;
    tags[it->tag_idx].refcount = 0;
    tags[it->tag_idx].vlen = 0;
    tags[it->tag_idx].klen = 0;
    tags[it->tag_idx].olen = 0;
    tags[it->tag_idx].expire_at = 0;
    tags[it->tag_idx].create_at = 0;
    tags[it->tag_idx].locked = 0;
#endif

#ifdef USE_LHD
    it->v_create_time = 0;
#ifdef USE_TAG
    tags[it->tag_idx].v_create_time = 0;
#endif
#elif defined(USE_HYPERBOLIC)
    it->freq = 0;
#ifdef USE_TAG
    tags[it->tag_idx].freq = 0;
#endif
#endif
}

/*
 * Allocate an item. We allocate an item by consuming the next free item
 * from slab of the item's slab class.
 *
 * On success we return the pointer to the allocated item.
 */
static item_rstatus_e
_item_alloc(struct item **it_p, uint8_t klen, uint32_t vlen, uint8_t olen)
{
    uint8_t id = slab_id(item_ntotal(klen, vlen, olen));
    struct slabclass *p = &slabclasses[id];
    struct item *it;

    log_verb("allocate item with klen %u vlen %u", klen, vlen);

    *it_p = NULL;
    if (id == SLABCLASS_INVALID_ID) {
        return ITEM_EOVERSIZED;
    }

    if (p->nslabs == 0) {
        /* no slab for this slab class */
        _slab_get(id);
    }

//    lock_slabclass(id);

    it = slab_get_item(id);

//    unlock_slabclass(id);

    *it_p = it;
    rstatus_i status;

    if (it != NULL) {
        _item_reset(it);
        int refcount = __atomic_fetch_add(&it->refcount, 1, __ATOMIC_RELAXED);
        ASSERT(refcount == 0);
        INCR(slab_metrics, item_curr);
        INCR(slab_metrics, item_alloc);
        PERSLAB_INCR(id, item_curr);

        log_verb("alloc it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
                it->offset);

        status = ITEM_OK;

        ASSERT(it->locked == 0);
    } else {
        it = rand_evict(p);
        if (it != NULL) {
            ASSERT(it->locked == 1);
        } else {
            it = slab_get_item(id);
            ASSERT(it != NULL);
        }
        _item_reset(it);
        __atomic_fetch_add(&it->refcount, 1, __ATOMIC_RELAXED);
        *it_p = it;

        INCR(slab_metrics, item_curr);
        INCR(slab_metrics, item_alloc);
        PERSLAB_INCR(id, item_curr);

        log_verb("alloc it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
                it->offset);

        status = ITEM_OK;
    }

    ASSERT(it->is_linked == 0);
    ASSERT(it->in_freeq == 0);
    ASSERT(it->refcount == 1);


    return status;
}

static inline void
_item_dealloc(struct item **it_p)
{
    uint8_t id = (*it_p)->id;

//    lock_slabclass(id);

    DECR(slab_metrics, item_curr);
    INCR(slab_metrics, item_dealloc);
    PERSLAB_DECR(id, item_curr);

    slab_put_item(*it_p, id);
    cc_itt_free(slab_free, *it_p);
    *it_p = NULL;

//    unlock_slabclass(id);
}

/*
 * (Re)Link an item into the hash table
 */
static void
_item_link(struct item *it, bool relink)
{
    ASSERT(it->magic == ITEM_MAGIC);
    ASSERT(!(it->in_freeq));

    hashtable_put(it, hash_table);

    if (!relink) {
        ASSERT(!(it->is_linked));

        it->is_linked = 1;
//        slab_deref(item_to_slab(it)); /* slab ref'ed in _item_alloc */
        int refcnt = __atomic_fetch_sub(&it->refcount, 1, __ATOMIC_RELAXED);
        ASSERT(refcnt >= 0);
    }


    log_verb("link it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
            it->offset);

    __atomic_store_n(&it->locked, 0, __ATOMIC_RELEASE);

    INCR(slab_metrics, item_linked_curr);
    INCR(slab_metrics, item_link);
    /* TODO(yao): how do we track optional storage? Separate or treat as val? */
    INCR_N(slab_metrics, item_keyval_byte, it->klen + it->vlen);
    INCR_N(slab_metrics, item_val_byte, it->vlen);
    PERSLAB_INCR_N(it->id, item_keyval_byte, it->klen + it->vlen);
    PERSLAB_INCR_N(it->id, item_val_byte, it->vlen);
}

void
item_relink(struct item *it)
{
    _item_link(it, true);
}

void
item_insert(struct item *it, const struct bstring *key)
{
    ASSERT(it != NULL && key != NULL);

    ASSERT(it->is_linked == 0);
    ASSERT(it->in_freeq == 0);
    ASSERT(it->refcount == 1);

//    lock_bucket(key->data, key->len);

    item_delete(key);

    _item_link(it, false);

//    unlock_bucket(key->data, key->len);

    log_verb("insert it %p of id %"PRIu8" for key %.*s", it, it->id, key->len,
        key->data);

    cc_itt_alloc(slab_malloc, it, item_size(it));
}

/*
 * Unlinks an item from the hash table.
 */
static void
_item_unlink(struct item *it)
{

    ASSERT(it->magic == ITEM_MAGIC);

    log_verb("unlink it %p of id %"PRIu8" at offset %"PRIu32, it, it->id,
            it->offset);

    if (it->is_linked) {
        it->is_linked = 0;
        hashtable_delete(item_key(it), it->klen, hash_table);
    }


    DECR(slab_metrics, item_linked_curr);
    INCR(slab_metrics, item_unlink);
    DECR_N(slab_metrics, item_keyval_byte, it->klen + it->vlen);
    DECR_N(slab_metrics, item_val_byte, it->vlen);
    PERSLAB_DECR_N(it->id, item_keyval_byte, it->klen + it->vlen);
    PERSLAB_DECR_N(it->id, item_val_byte, it->vlen);
}

/**
 * Return an item if it hasn't been marked as expired, lazily expiring
 * item as-and-when needed
 */
struct item *
item_get(const struct bstring *key)
{
    struct item *it;
    struct slabclass *p;

    uint64_t local_n = __atomic_fetch_add(&n_req, 1, __ATOMIC_RELAXED);
    if (local_n && local_n % REBALANCE_INTVL == 0) {
        rebalance_slab();
    }

#ifdef USE_LHD
    if (local_n && local_n % RECOMPUTE_INTVL == 0) {
        LHD_num_reconfig += 1;
#ifdef USE_AGE_COARSENING
        age_coarsening();
#endif
        for (int i = SLABCLASS_MIN_ID; i <= profile_last_id; i++) {
            p = &slabclasses[i];
            uint64_t totalEvents = p->n_hit_age[MAX_AGE-1] + p->n_evict_age[MAX_AGE-1];
            uint64_t totalHits = p->n_hit_age[MAX_AGE-1];
            uint64_t lifetimeUnconditioned = totalEvents;

            for (int64_t a = MAX_AGE - 2; a >= 0; a--) {
                totalHits += p->n_hit_age[a];
                totalEvents += p->n_hit_age[a] + p->n_evict_age[a];
                lifetimeUnconditioned += totalEvents;

                if (totalEvents > 1e-5) {
                    p->LHD[a] = (double) totalHits / lifetimeUnconditioned;
                } else {
                    p->LHD[a] = 0.;
                }
            }
        }
    }
#endif


//    lock_bucket(key->data, key->len);

    it = hashtable_get(key->data, key->len, hash_table);
    if (it == NULL) {
        unlock_bucket(key->data, key->len);
        log_verb("get it '%.*s' not found", key->len, key->data);
        return NULL;
    }

    /* TODO(jason): it is possible that the thread is stuck here while other threads
     * have evicted the slab */
    __atomic_fetch_add(&it->refcount, 1, __ATOMIC_RELAXED);
    struct slab *slab = item_to_slab(it);
    p = &slabclasses[slab->id];

#ifdef USE_LHD
//    uint64_t age = local_n / AGE_GRANULARITY;
    uint64_t age = ((uint64_t) (local_n - it->v_create_time)) >> ageCoarseningShift;
    age = age > MAX_AGE ? MAX_AGE - 1 : age;
    __atomic_fetch_add(&p->n_hit_age[age], 1, __ATOMIC_RELAXED);
#elif defined(USE_HYPERBOLIC)
    __atomic_fetch_add(&it->freq, 1, __ATOMIC_RELAXED);
#ifdef USE_TAG
    tags[it->tag_idx].freq += 1;
#endif
#endif
    __atomic_fetch_add(&p->n_req, 1, __ATOMIC_RELAXED);

    log_verb("get it key %.*s val %.*s", key->len, key->data, it->vlen,
            item_data(it));

    if (_item_expired(it)) {
        log_verb("get it '%.*s' expired and nuked", key->len, key->data);

        __atomic_fetch_sub(&it->refcount, 1, __ATOMIC_RELAXED);

        _item_delete(&it);

        unlock_bucket(key->data, key->len);
        return NULL;
    }

//    unlock_bucket(key->data, key->len);

    log_verb("get it %p of id %"PRIu8, it, it->id);

    return it;
}

/* TODO(yao): move this to memcache-specific location */
static void
_item_define(struct item *it, const struct bstring *key, const struct bstring
        *val, uint8_t olen, proc_time_i expire_at)
{
    proc_time_i expire_cap = time_delta2proc_sec(max_ttl);

    it->create_at = time_proc_sec();
#ifdef USE_TAG
    tags[it->tag_idx].create_at = time_proc_sec();
#endif
#ifdef USE_LHD
    it->v_create_time = __atomic_load_n(&n_req, __ATOMIC_RELAXED);
#ifdef USE_TAG
    tags[it->tag_idx].v_create_time = it->v_create_time;
#endif
#endif
//    tags[it->tag_idx].expire_at = expire_at < expire_cap ? expire_at : expire_cap;
//    tags[it->tag_idx].klen = key->len;
//    tags[it->tag_idx].olen = olen;
//    tags[it->tag_idx].vlen = (val == NULL) ? 0 : val->len;

    it->expire_at = expire_at < expire_cap ? expire_at : expire_cap;
    item_set_cas(it);
    it->olen = olen;
    cc_memcpy(item_key(it), key->data, key->len);
    it->klen = key->len;
#ifdef REAL_COPY
    if (val != NULL) {
        cc_memcpy(item_data(it), val->data, val->len);
    }
#endif
    it->vlen = (val == NULL) ? 0 : val->len;


    ASSERT(it->is_linked == 0);
    ASSERT(it->in_freeq == 0);
    ASSERT(it->refcount == 1);
}

item_rstatus_e
item_reserve(struct item **it_p, const struct bstring *key, const struct bstring
        *val, uint32_t vlen, uint8_t olen, proc_time_i expire_at)
{
    item_rstatus_e status;
    struct item *it;

    if ((status = _item_alloc(it_p, key->len, vlen, olen)) != ITEM_OK) {
        log_debug("item reservation failed");
        return status;
    }

    it = *it_p;

    _item_define(it, key, val, olen, expire_at);

    log_verb("reserve it %p of id %"PRIu8" for key '%.*s' optional len %"PRIu8,
            it, it->id,key->len, key->data, olen);

    return ITEM_OK;
}

void
item_release(struct item **it_p)
{
//    slab_deref(item_to_slab(*it_p)); /* slab ref'ed in _item_alloc */
    _item_dealloc(it_p);
    ASSERT(0);
}

void
item_backfill(struct item *it, const struct bstring *val)
{
    ASSERT(it != NULL);

    cc_memcpy(item_data(it) + it->vlen, val->data, val->len);
    it->vlen += val->len;

    log_verb("backfill it %p with %"PRIu32" bytes, now %"PRIu32" bytes total",
            it, val->len, it->vlen);
}

//item_rstatus_e
//item_annex(struct item *oit, const struct bstring *key, const struct bstring
//        *val, bool append)
//{
//    item_rstatus_e status = ITEM_OK;
//    struct item *nit = NULL;
//    uint8_t id;
//    uint32_t ntotal = oit->vlen + val->len;
//
//    id = item_slabid(oit->klen, ntotal, oit->olen);
//    if (id == SLABCLASS_INVALID_ID) {
//        log_info("client error: annex operation results in oversized item with"
//                   "key size %"PRIu8" old value size %"PRIu32" and new value "
//                   "size %"PRIu32, oit->klen, oit->vlen, ntotal);
//
//        return ITEM_EOVERSIZED;
//    }
//
//    if (append) {
//        /* if it is large enough to hold the extra data and left-aligned,
//         * which is the default behavior, we copy the delta to the end of
//         * the existing data. Otherwise, allocate a new item and store the
//         * payload left-aligned.
//         */
//        if (id == oit->id && !(oit->is_raligned)) {
//            cc_memcpy(item_data(oit) + oit->vlen, val->data, val->len);
//            oit->vlen = ntotal;
//            INCR_N(slab_metrics, item_keyval_byte, val->len);
//            INCR_N(slab_metrics, item_val_byte, val->len);
//            item_set_cas(oit);
//        } else {
//            status = _item_alloc(&nit, oit->klen, ntotal, oit->olen);
//            if (status != ITEM_OK) {
//                log_debug("annex failed due to failure to allocate new item");
//                return status;
//            }
//            _copy_key_item(nit, oit);
//            nit->expire_at = oit->expire_at;
//            nit->create_at = time_proc_sec();
//            item_set_cas(nit);
//            /* value is left-aligned */
//            cc_memcpy(item_data(nit), item_data(oit), oit->vlen);
//            cc_memcpy(item_data(nit) + oit->vlen, val->data, val->len);
//            nit->vlen = ntotal;
//            item_insert(nit, key);
//        }
//    } else {
//        /* if oit is large enough to hold the extra data and is already
//         * right-aligned, we copy the delta to the front of the existing
//         * data. Otherwise, allocate a new item and store the payload
//         * right-aligned, assuming more prepends will happen in the future.
//         */
//        if (id == oit->id && oit->is_raligned) {
//            cc_memcpy(item_data(oit) - val->len, val->data, val->len);
//            oit->vlen = ntotal;
//            INCR_N(slab_metrics, item_keyval_byte, val->len);
//            INCR_N(slab_metrics, item_val_byte, val->len);
//            item_set_cas(oit);
//        } else {
//            status = _item_alloc(&nit, oit->klen, ntotal, oit->olen);
//            if (status != ITEM_OK) {
//                log_debug("annex failed due to failure to allocate new item");
//                return status;
//            }
//            _copy_key_item(nit, oit);
//            nit->expire_at = oit->expire_at;
//            nit->create_at = time_proc_sec();
//            item_set_cas(nit);
//            /* value is right-aligned */
//            nit->is_raligned = 1;
//            cc_memcpy(item_data(nit) - ntotal, val->data, val->len);
//            cc_memcpy(item_data(nit) - oit->vlen, item_data(oit), oit->vlen);
//            nit->vlen = ntotal;
//            item_insert(nit, key);
//        }
//    }
//
//    log_verb("annex to it %p of id %"PRIu8", new it at %p", oit, oit->id,
//            nit ? oit : nit);
//
//    return status;
//}

void
item_update(struct item *it, const struct bstring *val)
{
    ASSERT(item_slabid(it->klen, val->len, it->olen) == it->id);

    it->vlen = val->len;
    cc_memcpy(item_data(it), val->data, val->len);
    item_set_cas(it);

    log_verb("update it %p of id %"PRIu8, it, it->id);
}

static void
_item_delete(struct item **it)
{
//    uint32_t lock_idx = get_hv(item_key(*it), (*it)->klen) & 0x0000ffffu;
//    ASSERT(pthread_mutex_trylock(&item_lock[lock_idx]) != 0);

    log_verb("delete it %p of id %"PRIu8, *it, (*it)->id);

    int slabclass_id = (*it)->id;
    lock_slabclass(slabclass_id);

    _item_unlink(*it);
    _item_dealloc(it);

    unlock_slabclass(slabclass_id);
}

bool
item_delete(const struct bstring *key)
{
    struct item *it;

    lock_bucket(key->data, key->len);
    it = hashtable_get(key->data, key->len, hash_table);
    if (it != NULL) {
        _item_delete(&it);

        unlock_bucket(key->data, key->len);
        return true;
    } else {
        unlock_bucket(key->data, key->len);
        return false;
    }
}

void
item_flush(void)
{
    time_update();
    flush_at = time_proc_sec();
    log_info("all keys flushed at %"PRIu32, flush_at);
}



static double cal_item_score(struct item *it) {
#ifdef USE_LHD
//    if (n_req < RECOMPUTE_INTVL) {
//        return 0;
//    }

//    uint64_t age = (n_req - it->v_create_time) / AGE_GRANULARITY;
    uint64_t age = (n_req - it->v_create_time) >> ageCoarseningShift;
    age = age > MAX_AGE ? MAX_AGE - 1 : age;
    struct slabclass *p = &slabclasses[item_to_slab(it)->id];
    return -p->LHD[age]/item_size(it);
#elif defined(USE_HYPERBOLIC)
    return -(double) (it->freq)/(time_proc_sec() - it->create_at);
#else
#error unknown
#endif

}

static struct item * rand_evict(struct slabclass *p) {
    struct item *it;
    struct item *best_it = NULL;
    uint64_t best_it_ver = 0;
    double best_score = -1, score;

    uint64_t slab_idx, item_idx;
    uint8_t unlock = 0;
    int i = 0;
    static bool has_print = false;

start_selection:

    while (i < RAND_CHOOSE_N) {
        slab_idx = prand() % (p->nslabs);
        item_idx = prand() % (p->nitem);
        it = slab_to_item(p->slab_list[slab_idx], item_idx, p->size);
        if (it->in_freeq || it->is_linked == 0 ||
                __atomic_load_n(&it->locked, __ATOMIC_ACQUIRE) == 1 ||
                __atomic_load_n(&it->refcount, __ATOMIC_ACQUIRE) > 0) {
            continue;
        }
#if defined(USE_RANDOM_EXPIRE) && USE_RANDOM_EXPIRE == 1
        if (!has_print) {
            printf("random sampling for expiration turned on\n");
            has_print = true;
        }
        if (_item_expired(it)) {
#if defined(USE_LHD)
            uint64_t age = (n_req - it->v_create_time) >> ageCoarseningShift;
            age = age > MAX_AGE ? MAX_AGE - 1 : age;
            __atomic_fetch_add(&p->n_evict_age[age], 1, __ATOMIC_RELAXED);
#endif

            _item_delete(&it);
            i += 1;
            continue;
        }
#else
        if (!has_print) {
            printf("random sampling expiration off\n");
            has_print = true;
        }
#endif

        if (best_it == NULL) {
            best_it = it;
            best_score = cal_item_score(it);
            best_it_ver = item_get_cas(it);
        } else {
            score = cal_item_score(it);
//            printf("score %.2lf\n", score);
            if (score > best_score) {
                best_it = it;
                best_score = score;
                best_it_ver = item_get_cas(it);
            }
        }
        i ++;
    }

    if (best_it == NULL) {
        return NULL;
    }

    if (!__atomic_compare_exchange_n(&best_it->locked, &unlock, 1, false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
        goto start_selection;
    }

    if (item_get_cas(best_it) != best_it_ver) {
        __atomic_store_n(&best_it->locked, 0, __ATOMIC_RELEASE);
        goto start_selection;
    }

    log_debug("evict %.*s", best_it->klen, item_key(best_it));

#if defined(USE_LHD)
    uint64_t local_n = __atomic_load_n(&n_req, __ATOMIC_RELAXED);
//    uint64_t age = local_n / AGE_GRANULARITY;
    uint64_t age = (local_n - best_it->v_create_time) >> ageCoarseningShift;
    age = age > MAX_AGE ? MAX_AGE - 1 : age;
    __atomic_fetch_add(&p->n_evict_age[age], 1, __ATOMIC_RELAXED);
    p->ev_rank = p->ev_rank * 0.9 + (-best_score)*0.1;
#endif

//    int slabclass_id = p->slab_list[0]->id;
//    lock_slabclass(slabclass_id);

    _item_unlink(best_it);
    item_set_cas(best_it);

//    unlock_slabclass(slabclass_id);

    __atomic_fetch_add(&p->ev_age, time_proc_sec() - best_it->create_at, __ATOMIC_RELAXED);
    __atomic_fetch_add(&p->n_eviction, 1, __ATOMIC_RELAXED);


    return best_it;
}


