#include "slab.h"

#include "constant.h"

#include <cc_debug.h>

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#ifdef USE_LHD
#include "LHD.h"
struct LHD_class LHD_classes[NUM_LHD_CLASS];
uint64_t LHD_num_reconfig = 0;
uint64_t next_reconfigure_time = RECONFIGURE_INTVL;

unsigned int ageCoarseningShift = 10;
float ewmaNumObjects = 0;
float ewmaNumObjectsMass = 0;
bool LHD_init = false;



#elif defined(USE_HYPERBOLIC)
#include "hyperbolic.h"
#endif

uint64_t n_req = 0;
extern delta_time_i max_ttl;
proc_time_i flush_at = -1;

//extern pthread_mutex_t item_lock[1u<<16u];
//struct item     tags[100000000];
//uint64_t    tag_pos = 0;



static __thread __uint128_t g_lehmer64_state = 1;


static inline uint64_t prand(void) {
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return g_lehmer64_state >> 64u;
}

pthread_mutex_t temp_lock = PTHREAD_MUTEX_INITIALIZER;


static struct item * rand_evict(struct slabclass *p);

//void lock_item(struct item *it) {
//    uint32_t lock_idx = get_hv(item_key(it), it->klen) & 0x0000ffffu;
//    struct slabclass *p = &slabclasses[it->id];
//    pthread_mutex_lock(&item_lock[lock_idx]);
//    pthread_mutex_lock(&(p->lock));
//}
//
//void unlock_item(struct item *it) {
//    uint32_t lock_idx = get_hv(item_key(it), it->klen) & 0x0000ffffu;
//    struct slabclass *p = &slabclasses[it->id];
//    pthread_mutex_unlock(&item_lock[lock_idx]);
//    pthread_mutex_unlock(&(p->lock));
//}

//void lock_bucket(const char *key, uint32_t key_len) {
//    uint32_t lock_idx = get_hv(key, key_len) & 0x0000ffffu;
//    int status = pthread_mutex_lock(&item_lock[lock_idx]);
//    ASSERT(status == 0);
//}
//
//void unlock_bucket(const char *key, uint32_t key_len) {
//    uint32_t lock_idx = get_hv(key, key_len) & 0x0000ffffu;
//    int status = pthread_mutex_unlock(&item_lock[lock_idx]);
//    ASSERT(status == 0);
//}

//void lock_slabclass(int id) {
//    ASSERT(id >= SLABCLASS_MIN_ID && id <= profile_last_id);
////    log_info("lock slabclass id %d", id);
//    int status = pthread_mutex_lock(&(slabclasses[id].lock));
//    ASSERT(status == 0);
//    ASSERT(pthread_mutex_trylock(&slabclasses[id].lock) != 0);
//}
//
//void unlock_slabclass(int id) {
//    ASSERT(id >= SLABCLASS_MIN_ID && id <= profile_last_id);
////    log_info("unlock slabclass id %d", id);
//    struct slabclass *p = &slabclasses[id];
//    int status = pthread_mutex_unlock(&(p->lock));
//    ASSERT(status == 0);
//}


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
//    it->tag_idx = __atomic_fetch_add(&tag_pos, 1, __ATOMIC_RELAXED);
#ifdef USE_LHD
    it->access_time = 0;
    it->last_age = 0;
    it->last_last_age = MAX_AGE;
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
    it->access_time = 0;
#ifdef USE_TAG
    tags[it->tag_idx].access_time = 0;
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

    it = slab_get_item(id);

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
//            ASSERT(it->locked == 1);
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

    DECR(slab_metrics, item_curr);
    INCR(slab_metrics, item_dealloc);
    PERSLAB_DECR(id, item_curr);

    slab_put_item(*it_p, id);
    cc_itt_free(slab_free, *it_p);
    *it_p = NULL;
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

    item_delete(key);

    _item_link(it, false);

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

    if (!LHD_init) {
        init_lhd_class();
        LHD_init = true;
    }

    uint64_t local_n = __atomic_fetch_add(&n_req, 1, __ATOMIC_RELAXED);
    if (local_n && local_n % REBALANCE_INTVL == 0) {
        rebalance_slab();
    }


    it = hashtable_get(key->data, key->len, hash_table);
    if (it == NULL) {
        log_verb("get it '%.*s' not found", key->len, key->data);
        return NULL;
    }

    __atomic_fetch_add(&it->refcount, 1, __ATOMIC_RELAXED);
//    struct slab *slab = item_to_slab(it);
//    p = &slabclasses[slab->id];

#ifdef USE_LHD
    uint64_t age = lhd_age(it);
    int class_id = lhd_class_id(it);
    LHD_classes[class_id].hits[age] += 1;

    it->last_last_age = it->last_age;
    it->last_age = age;
    it->access_time = n_req;

    if (n_req > next_reconfigure_time) {
        lhd_reconfigure();
        next_reconfigure_time += RECONFIGURE_INTVL;
    }

#elif defined(USE_HYPERBOLIC)
    __atomic_fetch_add(&it->freq, 1, __ATOMIC_RELAXED);
#endif

    log_verb("get it key %.*s val %.*s", key->len, key->data, it->vlen,
            item_data(it));

    if (_item_expired(it)) {
        log_verb("get it '%.*s' expired and nuked", key->len, key->data);

        __atomic_fetch_sub(&it->refcount, 1, __ATOMIC_RELAXED);

        _item_delete(&it);

        return NULL;
    }


    log_verb("get it %p of id %"PRIu8, it, it->id);

    return it;
}

/* TODO(yao): move this to memcache-specific location */
static void
_item_define(struct item *it, const struct bstring *key, const struct bstring
        *val, uint8_t olen, proc_time_i expire_at, const uint8_t ns)
{
    proc_time_i expire_cap = time_delta2proc_sec(max_ttl);

    it->create_at = time_proc_sec();

#ifdef USE_LHD
    it->access_time = __atomic_fetch_add(&n_req, 1, __ATOMIC_RELAXED);
    it->last_age = 0;
    it->last_last_age = MAX_AGE;
#endif
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

    it->ns = ns;

    ASSERT(it->is_linked == 0);
    ASSERT(it->in_freeq == 0);
    ASSERT(it->refcount == 1);
}

item_rstatus_e
item_reserve(struct item **it_p, const struct bstring *key, const struct bstring
        *val, uint32_t vlen, uint8_t olen, proc_time_i expire_at, uint8_t ns)
{
    item_rstatus_e status;
    struct item *it;

    if ((status = _item_alloc(it_p, key->len, vlen, olen)) != ITEM_OK) {
        log_debug("item reservation failed");
        return status;
    }

    it = *it_p;

    _item_define(it, key, val, olen, expire_at, ns);

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

    _item_unlink(*it);
    _item_dealloc(it);

}

bool
item_delete(const struct bstring *key)
{
    struct item *it;

    it = hashtable_get(key->data, key->len, hash_table);
    if (it != NULL) {
        _item_delete(&it);

        return true;
    } else {
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



static float cal_item_score(struct item *it) {
#ifdef USE_LHD
    return lhd_get_hit_density(it);

#elif defined(USE_HYPERBOLIC)
    return cal_hyperbolic_score(it);
#else
#error unknown
#endif

}

static struct item * rand_evict(struct slabclass *p) {
    struct item *it;
    struct item *best_it = NULL;
    double lowest_score, score;

    uint64_t slab_idx, item_idx;
    int i = 0;
    static bool has_print = false;
    if (!has_print) {
#if defined(USE_RANDOM_EXPIRE) && USE_RANDOM_EXPIRE == 1
        printf("random sampling for expiration turned on\n");
#else
        printf("random sampling expiration off\n");
#endif
        has_print = true;
    }


    while (i < RAND_CHOOSE_N) {
        slab_idx = prand() % (p->nslabs);
        item_idx = prand() % (p->nitem);
        it = slab_to_item(p->slab_list[slab_idx], item_idx, p->size);
        if (it->in_freeq || it->is_linked == 0) {
            i += 1;
            continue;
        }
#if defined(USE_RANDOM_EXPIRE) && USE_RANDOM_EXPIRE == 1
        if (_item_expired(it)) {
#if defined(USE_LHD)
            uint64_t age = lhd_age(it);
            int class_id = lhd_class_id(it);
            LHD_classes[class_id].evictions[age] += 1;
#endif
            _item_delete(&it);
            i += 1;
            continue;
        }
#endif

        score = cal_item_score(it);
        if (best_it == NULL || score < lowest_score) {
            best_it = it;
            lowest_score = score;
        }
        i ++;
    }

    if (best_it == NULL) {
        return NULL;
    }

    log_debug("evict %.*s", best_it->klen, item_key(best_it));

#if defined(USE_LHD)
    uint64_t age = lhd_age(best_it);
    int class_id = lhd_class_id(best_it);
    LHD_classes[class_id].evictions[age] += 1;
#endif

    _item_unlink(best_it);
    item_set_cas(best_it);

    __atomic_fetch_add(&p->ev_age_sum, time_proc_sec() - best_it->create_at, __ATOMIC_RELAXED);
    __atomic_fetch_add(&p->n_eviction, 1, __ATOMIC_RELAXED);


    return best_it;
}


