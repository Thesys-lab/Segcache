
#define XXH_INLINE_ALL

#include "hashtable.h"
#include "xxhash.h"
#include "item.h"
#include "seg.h"

#include <cc_mm.h>
#include <hash/cc_murmur3.h>

#include <stdlib.h>
#include <sys/mman.h>
#include <sysexits.h>
#include <x86intrin.h>

#define CACHE_ALIGN_SIZE                    64

/* the number of slots in one bucket,
 * need to be multiple of 8 to make use of 64-byte cache line */
#define N_SLOT_PER_BUCKET                   8u
/* the number of slots in one bucket, expressed in terms of number of bits
 * for example 8 == 2^3 */
#define N_SLOT_PER_BUCKET_LOG2           3u

/* mask for item_info */
#define TAG_MASK                0xfff0000000000000ul
#define FREQ_MASK               0x000ff00000000000ul
#define SEG_ID_MASK             0x00000ffffff00000ul
#define OFFSET_MASK             0x00000000000ffffful

/* mast for bucket info */
#define LOCK_MASK               0xff00000000000000ul
#define BUCKET_CHAIN_LEN_MASK   0x00ff000000000000ul
#define CAS_MASK                0x00000000fffffffful
#define TS_MASK                 0x0000ffff00000000ul

/* a per-bucket spin lock */
#define LOCKED                  0x0100000000000000ul
#define UNLOCKED                0x0000000000000000ul

extern seg_metrics_st    *seg_metrics;
static struct hash_table hash_table;
static bool              hash_table_initialized = false;

//static uint32_t             murmur3_iv       = 0x3ac5d673;
static __thread __uint128_t g_lehmer64_state = 1;

///* keep a cache of time stamp here to avoid repeated fetch */
//static __thread int32_t last_sec_update = -1;
///* this is used when calculating whether we should increase frequency
// * counter at curr time, range 1-8, see _incr_freq */
//static __thread unsigned int cur_sec_freq_bit = 0;


#define HASHSIZE(_n)        (1ULL << (_n))
#define HASHMASK(_n)        (HASHSIZE(_n) - 1)

#define CAL_HV(key, klen)   _get_hv_xxhash(key, klen)
/* tags is calculated in two places,
 * one is calculated from hash value, the other is using item info,
 * we use the top 2 byte of hash value as tag and
 * store it in the top two bytes of item info,
 * note that we make tag to start from 1, so when we calculate true tag
 * we perform or with 0x0001000000000000ul */
#define GET_TAG(item_info)          ((item_info) & TAG_MASK)
#define GET_FREQ(item_info)         (((item_info) & FREQ_MASK) >> 44u)
#define GET_SEG_ID(item_info)       (((item_info) & SEG_ID_MASK) >> 20ul)
#define GET_OFFSET(item_info)       (((item_info) & OFFSET_MASK) << 3ul)
#define CLEAR_FREQ(item_info)       ((item_info) & (~FREQ_MASK))

#define CAL_TAG_FROM_HV(hv) (((hv) & TAG_MASK) | 0x0010000000000000ul)
#define GET_BUCKET(hv)      (&hash_table.table[((hv) & hash_table.hash_mask)])

#define GET_TS(bucket_ptr)          (((*(bucket_ptr)) & TS_MASK) >> 32u)
#define GET_CAS(bucket_ptr)         ((*(bucket_ptr)) & CAS_MASK)

/* calculate the number of buckets in the bucket chain */
#define GET_BUCKET_CHAIN_LEN(bucket_ptr)                                       \
                ((((*(bucket_ptr)) & BUCKET_CHAIN_LEN_MASK) >> 48ul) + 1)
#define INCR_BUCKET_CHAIN_LEN(bucket_ptr)                                      \
                ((*(bucket_ptr)) += 0x0001000000000000ul)

#define CAS_SLOT(slot_ptr, expect_ptr, new_val)                                \
    __atomic_compare_exchange_n(                                               \
        (slot_ptr), (expect_ptr), (new_val), false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)

#if defined HASHTABLE_DBG
#define SET_BUCKET_MAGIC(bucket_ptr)                                           \
    (*(bucket_ptr)) = ((*(bucket_ptr)) | 0x0000ffff00000000ul)
#define CHECK_BUCKET_MAGIC(bucket_ptr)                                         \
    ASSERT(((*(bucket_ptr)) & 0x0000ffff00000000ul) == 0x0000ffff00000000ul)

#undef GET_BUCKET
static uint64_t* GET_BUCKET(uint64_t hv)
{
    uint64_t *bucket_ptr = (&hash_table.table[((hv) & hash_table.hash_mask)]);
    CHECK_BUCKET_MAGIC(bucket_ptr);
    return bucket_ptr;
}

#else
#define SET_BUCKET_MAGIC(bucket_ptr)
#define CHECK_BUCKET_MAGIC(bucket_ptr)
#endif




/* TODO(jason): it is better to use id for extra_array instead of pointer
 * otherwise we have to assume each pointer is 64-byte
 * TODO(jason): add bucket array shrink
 * */
#define use_atomic_set

/* we assume little-endian here */
#define lock(bucket_ptr)                                                       \
    do {                                                                       \
        uint8_t locked = 0;                                                    \
        while (!CAS_SLOT(((uint8_t *)bucket_ptr + 7), &locked, 1)) {           \
            ASSERT(locked == 1);                                               \
            locked = 0;                                                        \
            ;                                                         \
        }                                                                      \
    } while (0)

#define unlock(bucket_ptr)                                                     \
    do {                                                                       \
        ASSERT(((*bucket_ptr) & LOCK_MASK) == LOCKED);                         \
        *bucket_ptr ^= LOCKED;                                                 \
    } while (0)

#define unlock_and_update_cas(bucket_ptr)                                      \
    do {                                                                       \
        ASSERT(((*bucket_ptr) & LOCK_MASK) != 0);                              \
        *bucket_ptr = (*bucket_ptr + 1) ^ LOCKED;                              \
    } while (0)

#ifdef use_atomic_set
#    undef lock
#    undef unlock
#    undef unlock_and_update_cas
#    define lock(bucket_ptr)                                                   \
        do {                                                                   \
            while (__atomic_test_and_set(((uint8_t *)(bucket_ptr) + 7), __ATOMIC_ACQUIRE)) {                                        \
                ;                                                     \
            }                                                                  \
        } while (0)

#    define unlock(bucket_ptr)                                                 \
        do {                                                                   \
            __atomic_clear(((uint8_t *)(bucket_ptr) + 7), __ATOMIC_RELEASE);   \
        } while (0)

#    define unlock_and_update_cas(bucket_ptr)                                  \
        do {                                                                   \
            *bucket_ptr += 1;                                                  \
            __atomic_clear(((uint8_t *)(bucket_ptr) + 7), __ATOMIC_RELEASE);   \
        } while (0)
#endif

#ifdef no_lock
#    undef lock
#    undef unlock
#    undef unlock_and_update_cas
#    define lock(bucket_ptr)
#    define unlock(bucket_ptr)
#    define unlock_and_update_cas(bucket_ptr) ((*(bucket_ptr)) += 1)
#endif

/**
 * this is placed here because it is called within bucket lock and it
 * needs to parse item_info
 *
 */

static inline struct item *
_info_to_item(uint64_t item_info)
{
    uint64_t seg_id = GET_SEG_ID(item_info);
    uint64_t offset = GET_OFFSET(item_info);
    ASSERT(seg_id < heap.max_nseg);
    ASSERT(offset < heap.seg_size);
//    ASSERT(offset < __atomic_load_n(&heap.segs[seg_id].write_offset, __ATOMIC_SEQ_CST));
    return (struct item *) (heap.base + heap.seg_size * seg_id + offset);
}

static inline void
_item_free(uint64_t item_info, bool mark_tombstone)
{
    struct item *it;
    uint64_t    seg_id = GET_SEG_ID(item_info);
    uint64_t    offset = GET_OFFSET(item_info);
    it = (struct item *) (heap.base + heap.seg_size * seg_id + offset);
    uint32_t sz = item_ntotal(it);

    __atomic_fetch_sub(&heap.segs[seg_id].occupied_size, sz, __ATOMIC_RELAXED);
    __atomic_fetch_sub(&heap.segs[seg_id].n_item, 1, __ATOMIC_RELAXED);

    ASSERT(__atomic_load_n(&heap.segs[seg_id].n_item, __ATOMIC_RELAXED) >= 0);
    ASSERT(__atomic_load_n(&heap.segs[seg_id].occupied_size, __ATOMIC_RELAXED)
        >= 0);

    if (mark_tombstone) {
        it->deleted = true;
    }
}

static inline bool
_same_item(const char *key, uint32_t klen, uint64_t item_info)
{
    struct item *oit = _info_to_item(item_info);
    return ((oit->klen == klen) && cc_memcmp(item_key(oit), key, klen) == 0);
}

static inline uint64_t
_build_item_info(uint64_t tag, uint64_t seg_id, uint64_t offset)
{
    ASSERT(offset % 8 == 0);
    uint64_t item_info = tag | (seg_id << 20u) | (offset >> 3u);
    return item_info;
}

#define SET_BIT(u64, pos) ((u64) | (1ul << (pos)))
#define GET_BIT(u64, pos) ((u64) & (1ul << (pos)))
#define CHECK_BIT(u64, pos) GET_BIT(u64, pos)
#define CLEAR_BIT(u64, pos) ((u64) & (~(1ul << (pos))))

static inline uint64_t
prand(void)
{
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return (uint64_t) g_lehmer64_state;
//    return g_lehmer64_state >> 64u;
}

/*
 * we use an approximate and probabilistic frequency counter
 * the freq counter has 8 bits, we set i-th bit if
 * 1. all bits before i has been set
 * 2. current time in sec mod (1 << i) == 0
 *
 * this avoids
 * 1. counting temporal bursts in seconds
 * 2. by counting only once per sec,
 *      the i-th bit is set with probability 1/(1<<i)
 *
 */
#ifdef COMMENT
static inline uint64_t
_incr_freq(uint64_t item_info)
{
#define FREQ_BIT_START 44u
    if (last_sec_update != time_proc_sec()) {
        last_sec_update = time_proc_sec();
        cur_sec_freq_bit = 0;
        while (((last_sec_update >> (cur_sec_freq_bit)) & 1u) == 0 && cur_sec_freq_bit < 8) {
            cur_sec_freq_bit += 1;
        }
        cur_sec_freq_bit += 1;
        ASSERT(FREQ_BIT_START + cur_sec_freq_bit <= 63);
    }


    for (unsigned int i = 0; i < cur_sec_freq_bit; i++) {
        if (GET_BIT(item_info, FREQ_BIT_START + i) == 0) {
            /* current bit is not set */
            item_info = SET_BIT(item_info, FREQ_BIT_START + i);
            /* set one bit at a time */
            break;
        }
    }

    return item_info;
}
#endif

//static inline uint64_t
//_incr_freq(uint64_t item_info)
//{
//#define FREQ_BIT_START 44u
//    uint64_t freq = GET_FREQ(item_info);
//    if (freq == 0 || prand() % freq == 0) {
//        freq = freq < 255 ? freq + 1 : 255;
//    }
//
//    item_info = (CLEAR_FREQ(item_info)) | (freq << FREQ_BIT_START);
//    return item_info;
//}

/*
 * Allocate table given size
 */
static inline uint64_t *
_hashtable_alloc(uint64_t n_slot)
{
    uint64_t *table =
                 aligned_alloc(CACHE_ALIGN_SIZE, sizeof(uint64_t) * n_slot);
    if (table == NULL) {
        log_crit("cannot create hash table");
        exit(EX_CONFIG);
    }
    cc_memset(table, 0, sizeof(uint64_t) * n_slot);

#ifdef MADV_HUGEPAGE
    /* USE_HUGEPAGE */
    madvise(table, sizeof(uint64_t) * n_slot, MADV_HUGEPAGE);
#endif

    return table;
}

void
hashtable_setup(uint32_t hash_power)
{
    uint64_t n_slot;

    ASSERT(hash_power > 0);

    if (hash_table_initialized) {
        log_warn("hash table has been initialized");
        hashtable_teardown();
    }

    /* init members */
    hash_table.hash_power = hash_power;
    n_slot = HASHSIZE(hash_power);
    hash_table.hash_mask =
        (n_slot - 1) & (0xfffffffffffffffful << N_SLOT_PER_BUCKET_LOG2);

    /* alloc table */
    hash_table.table = _hashtable_alloc(n_slot);

#if defined CC_ASSERT_PANIC || defined CC_ASSERT_LOG
    for (uint64_t i = 0; i < n_slot / N_SLOT_PER_BUCKET; i++) {
        SET_BUCKET_MAGIC(hash_table.table + i * N_SLOT_PER_BUCKET);
    }
#endif

    hash_table_initialized = true;

    log_info("create hash table of %" PRIu64 " entries %" PRIu64 " buckets",
        n_slot, n_slot >> N_SLOT_PER_BUCKET_LOG2);
}

void
hashtable_teardown(void)
{
    if (!hash_table_initialized) {
        log_warn("hash table is not initialized");
        return;
    }

    cc_free(hash_table.table);
    hash_table.table = NULL;

    hash_table_initialized = false;
}

//static inline uint64_t
//_get_hv_murmur3(const char *key, size_t klen)
//{
//    uint64_t hv[2];
//
//    hash_murmur3_128_x64(key, klen, murmur3_iv, hv);
//
//    return hv[0];
//}

static inline uint64_t
_get_hv_xxhash(const char *key, size_t klen)
{
    return XXH3_64bits(key, klen);

    /* maybe this API is preferred
     *   uint64_t hv = XXH3_64bits_dispatch(key, klen);
     */
}

/**
 * insert an item into hash table
 * insert has two steps, insert and delete (success or not found)
 * insert and delete must be completed in the same pass of scanning,
 * otherwise it cannot guarantee correctness
 *
 *
 * scan through all slots of the first bucket,
 * 1. if we found the item, replace with new item_info
 * 2. if we found an empty slot before the old item info, we place
 *      new item_info in empty slots and
 * 2-1. reset the old item info if the old item is in the first bucket,
 * 2-2. otherwise we simply return with the possibility that old item_info
 *      in a later bucket
 * 3. if oit item info is not found in the first bucket nor empty bucket,
 *      we continue to search
 */

void
hashtable_put(struct item *it, const uint64_t seg_id, const uint64_t offset)
{
    const char     *key = item_key(it);
    const uint32_t klen = item_nkey(it);

    uint64_t hv        = CAL_HV(key, klen);
    uint64_t tag       = CAL_TAG_FROM_HV(hv);
    uint64_t *head_bkt = GET_BUCKET(hv);

    uint64_t *bkt = head_bkt;
    INCR(seg_metrics, hash_insert);

    /* 12-bit tag, 8-bit counter,
     * 24-bit seg id, 20-bit offset (in the unit of 8-byte) */
    uint64_t item_info, insert_item_info;
    insert_item_info = _build_item_info(tag, seg_id, offset);

    lock(head_bkt);

    int bkt_chain_len = GET_BUCKET_CHAIN_LEN(head_bkt);
    int n_item_slot;
    do {
        /* the last slot will be a pointer to the next
         * bucket if there is next bucket */
        n_item_slot = bkt_chain_len > 1 ?
                      N_SLOT_PER_BUCKET - 1 :
                      N_SLOT_PER_BUCKET;

        for (int i = 0; i < n_item_slot; i++) {
            if (bkt == head_bkt && i == 0) {
                /* the first slot of the head bucket is bucket into */
                continue;
            }

            item_info = bkt[i];
            if (GET_TAG(item_info) != tag) {
                if (insert_item_info != 0 && item_info == 0) {
                    /* store item info in the first empty slot */
                    bkt[i] = insert_item_info;
                    insert_item_info = 0;
                }
                continue;
            }
            /* a potential hit */
            if (!_same_item(key, klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }

            /* found the item, now atomic update (or delete if already inserted)
             * read and write 8-byte on x86 is always atomic */
            bkt[i] = insert_item_info;
            insert_item_info = 0;

            /* now mark the old item as deleted, update stat */
            _item_free(item_info, false);

#ifdef OPTIMIZE_PMEM
            goto finish;
#endif
        }

//#ifdef OPTIMIZE_PMEM
        if (insert_item_info == 0) {
            /* item has been inserted, do not check next bucket to delete
             * old item, the info will be gc when item is evicted */
            goto finish;
        }
//#endif
        /* if there are overflown buckets, we continue to check */
        bkt_chain_len -= 1;
        if (bkt_chain_len > 0) {
            bkt = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
        }
    } while (bkt_chain_len > 0);

    /* we have searched every bucket, but have not found the old item
     * nor inserted new item - so we need to allocate a new array,
     * this is very rare */
    INCR(seg_metrics, hash_bucket_alloc);

    uint64_t *new_bkt = cc_zalloc(sizeof(uint64_t) * N_SLOT_PER_BUCKET);
    /* move the last item from last bucket to new bucket */
    new_bkt[0] = bkt[N_SLOT_PER_BUCKET - 1];
    new_bkt[1] = insert_item_info;
    insert_item_info = 0;

    /* Q(juncheng: what is the purpose of fence */
    __atomic_thread_fence(__ATOMIC_RELEASE);

    bkt[N_SLOT_PER_BUCKET - 1] = (uint64_t) new_bkt;

    INCR_BUCKET_CHAIN_LEN(head_bkt);
    /* this is for debugging, bucket chain length in production should not
     * be so high */
    log_verb("increase bucket chain to len %d", GET_BUCKET_CHAIN_LEN(head_bkt));
    ASSERT(GET_BUCKET_CHAIN_LEN(head_bkt) <= 16);

    finish:
    ASSERT(insert_item_info == 0);
    unlock_and_update_cas(head_bkt);
}

bool
hashtable_delete(const char *key, const uint32_t klen)
{
    INCR(seg_metrics, hash_remove);

    bool deleted = false;

    uint64_t hv         = CAL_HV(key, klen);
    uint64_t tag        = CAL_TAG_FROM_HV(hv);
    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *bkt       = first_bkt;

    uint64_t item_info;

    lock(first_bkt);

    int bkt_chain_len = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int n_item_slot;
    do {
        n_item_slot =
            bkt_chain_len > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < n_item_slot; i++) {
            if (bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = bkt[i];
            if (GET_TAG(item_info) != tag) {
                continue;
            }
            /* a potential hit */
            if (!_same_item(key, klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }
            /* found the item, now delete */
            /* if this is the first and most up-to-date hash table entry
             * for this key, we need to mark tombstone */
            _item_free(item_info, !deleted);
            bkt[i] = 0;

            deleted = true;
        }
        bkt_chain_len -= 1;
        bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
    } while (bkt_chain_len >= 0);

    unlock(first_bkt);
    return deleted;
}

/*
 * the difference between delete and evict is that
 * delete needs to mark tombstone on the most recent object,
 * evict  does not need to mark tombstone if the item being evicted is not
 *          up to date, otherwise, it needs to mark tombstone on the second
 *          most recent object
 *
 */
bool
hashtable_evict(const char *oit_key, const uint32_t oit_klen,
                const uint64_t seg_id, const uint64_t offset)
{
    INCR(seg_metrics, hash_remove);

    uint64_t hv         = CAL_HV(oit_key, oit_klen);
    uint64_t tag        = CAL_TAG_FROM_HV(hv);
    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *bkt       = first_bkt;

    uint64_t item_info;
    uint64_t oit_info   = _build_item_info(tag, seg_id, offset);

    bool first_match = true, item_outdated = true, fount_oit = false;

    lock(first_bkt);

    int bkt_chain_len = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int n_item_slot;
    do {
        n_item_slot =
            bkt_chain_len > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < n_item_slot; i++) {
            if (bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = CLEAR_FREQ(bkt[i]);
            if (GET_TAG(item_info) != tag) {
                continue;
            }
            /* a potential hit */
            if (!_same_item(oit_key, oit_klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }

            if (first_match) {
                if (oit_info == item_info) {
                    _item_free(item_info, false);
                    bkt[i] = 0;
                    item_outdated = false;
                    fount_oit     = true;
                }
                first_match = false;
                continue;
            }
            else {
                /* not first match, delete hash table entry,
                 * mark tombstone only when oit is the most up-to-date entry */
                if (!fount_oit && item_info == oit_info) {
                    fount_oit = true;
                }

                _item_free(bkt[i], !item_outdated);
                bkt[i] = 0;
            }
        }
        bkt_chain_len -= 1;
        bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
    } while (bkt_chain_len >= 0);

    unlock(first_bkt);

    return fount_oit;
}

bool
hashtable_delete_it(const char *oit_key, const uint32_t oit_klen,
                    const uint64_t seg_id, const uint64_t offset)
{
    INCR(seg_metrics, hash_remove);

    uint64_t hv         = CAL_HV(oit_key, oit_klen);
    uint64_t tag        = CAL_TAG_FROM_HV(hv);
    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *bkt       = first_bkt;

    uint64_t item_info;
    uint64_t oit_info   = _build_item_info(tag, seg_id, offset);
    bool found_oit = false;

    lock(first_bkt);

    int bkt_chain_len = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int n_item_slot;
    do {
        n_item_slot =
            bkt_chain_len > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < n_item_slot; i++) {
            item_info = CLEAR_FREQ(bkt[i]);
            if (oit_info == CLEAR_FREQ(bkt[i])) {
                _item_free(item_info, false);
                bkt[i] = 0;
                found_oit = true;
                break;
            }
        }
        bkt_chain_len -= 1;
        bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
    } while (bkt_chain_len >= 0);

    unlock(first_bkt);

    return found_oit;
}

struct item *
hashtable_get(const char *key, const uint32_t klen, int32_t *seg_id,
              uint64_t *cas)
{
    uint64_t    hv         = CAL_HV(key, klen);
    uint64_t    tag        = CAL_TAG_FROM_HV(hv);
    uint64_t    *first_bkt = GET_BUCKET(hv);
    uint64_t    *bkt       = first_bkt;
    uint64_t    offset;
    struct item *it;

    uint64_t item_info;

    int extra_array_cnt = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int array_size;

#ifdef USE_ASFC
    uint64_t cur_ts = (uint64_t) time_proc_sec() & 0xfffful;
    if (cur_ts != GET_TS(first_bkt)) {
        lock(first_bkt);

        if (cur_ts != GET_TS(first_bkt)) {
            *first_bkt = ((*first_bkt) & (~TS_MASK)) | (cur_ts << 32ul);
            do {
                array_size = extra_array_cnt > 0 ? N_SLOT_PER_BUCKET - 1 :
                             N_SLOT_PER_BUCKET;
                for (int i = 0; i < array_size; i++) {
                    if (bkt == first_bkt && i == 0) {
                        continue;
                    }
                    /* clear the bit */
                    bkt[i] = bkt[i] & 0xfff7fffffffffffful;

                    int old_freq = GET_FREQ(bkt[i]);
                    ASSERT(old_freq == GET_FREQ(bkt[i])
                        || old_freq - 128 == GET_FREQ(bkt[i]));
                }
                extra_array_cnt -= 1;
                bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
            } while (extra_array_cnt >= 0);
        }

        unlock(first_bkt);

        bkt             = first_bkt;
        extra_array_cnt = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    }
#endif

    do {
        array_size = extra_array_cnt > 0 ? N_SLOT_PER_BUCKET - 1 :
                     N_SLOT_PER_BUCKET;

        for (int i = 0; i < array_size; i++) {
            if (bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = bkt[i];
            if (GET_TAG(item_info) != tag) {
                continue;
            }
            /* a potential hit */
            if (!_same_item(key, klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }
            if (cas) {
                *cas = GET_CAS(first_bkt);
            }

            *seg_id = GET_SEG_ID(item_info);
            offset = GET_OFFSET(item_info);
            it     = (struct item *) (heap.base + heap.seg_size * (*seg_id)
                + offset);

#ifdef USE_ASFC
            uint64_t freq = GET_FREQ(item_info);

#if defined(SAMPLE_PER_SEC) && SAMPLE_PER_SEC == 1
//            if (it->freq < 16)
//                ASSERT(it->freq == freq || it->freq == freq - 128);

            if (freq < 127) {
                /* the highest bit of freq is not set
                 * and add one does not overflow */
                if (freq <= 16 || prand() % freq == 0) {
                    freq = ((freq + 1) | 0x80ul) << 44ul;
                }
                else {
                    freq = (freq | 0x80ul) << 44ul;
                }
                {
                    lock(first_bkt);
                    if (bkt[i] == item_info) {
                        bkt[i] = (item_info & (~FREQ_MASK)) | freq;
                    }
                    unlock(first_bkt);
                }
            }
#else
            if (freq < 127) {
                if (freq <= 8 || prand() % freq == 0) {
                    freq = ((freq + 1) | 0x80ul) << 44ul;
                    lock(first_bkt);
                    if (bkt[i] == item_info) {
                        bkt[i] = (item_info & (~FREQ_MASK)) | freq;
                    }
                    unlock(first_bkt);
                }
            }
#endif
#endif
            return it;
        }
        extra_array_cnt -= 1;
        bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
    } while (extra_array_cnt >= 0);

    return NULL;
}

struct item *
hashtable_get_no_incr(const char *key, const uint32_t klen, int32_t *seg_id,
                      uint64_t *cas)
{
    uint64_t    hv         = CAL_HV(key, klen);
    uint64_t    tag        = CAL_TAG_FROM_HV(hv);
    uint64_t    *first_bkt = GET_BUCKET(hv);
    uint64_t    *bkt       = first_bkt;
    uint64_t    offset;
    struct item *it;

    /* 16-bit tag, 28-bit seg id, 20-bit offset (in the unit of 8-byte) */
    uint64_t item_info;

    int extra_array_cnt = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int array_size;
    do {
        array_size =
            extra_array_cnt > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < array_size; i++) {
            if (bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = bkt[i];
            if (GET_TAG(item_info) != tag) {
                continue;
            }
            /* a potential hit */
            if (!_same_item(key, klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }
            if (cas) {
                *cas = GET_CAS(first_bkt);
            }

            *seg_id = GET_SEG_ID(item_info);
            offset = GET_OFFSET(item_info);
            it     = (struct item *) (heap.base + heap.seg_size * (*seg_id)
                + offset);

            return it;
        }
        extra_array_cnt -= 1;
        bkt        = (uint64_t *) (bkt[N_SLOT_PER_BUCKET - 1]);
    } while (extra_array_cnt >= 0);

    return NULL;
}

#ifdef USE_ASFC
int
hashtable_get_it_freq(const char *oit_key, const uint32_t oit_klen,
                      const uint64_t old_seg_id, const uint64_t old_offset)
{
    uint64_t hv  = CAL_HV(oit_key, oit_klen);
    uint64_t tag = CAL_TAG_FROM_HV(hv);

    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *curr_bkt  = first_bkt;
    uint64_t item_info;
    uint64_t oit_info   = _build_item_info(tag, old_seg_id, old_offset);
    int      freq       = 0;

    int extra_array_cnt = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int array_size;
    do {
        array_size =
            extra_array_cnt > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < array_size; i++) {
            if (curr_bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = CLEAR_FREQ(curr_bkt[i]);
            if (GET_TAG(item_info) != tag) {
                continue;
            }

            if (item_info == oit_info) {
                freq = GET_FREQ(curr_bkt[i]) & 0x7Ful;
                return freq;
            }

            /* a potential hit */
            if (!_same_item(oit_key, oit_klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }

            /* the oit is outdated */
            return 0;

        }
        extra_array_cnt -= 1;
        curr_bkt   = (uint64_t *) (curr_bkt[N_SLOT_PER_BUCKET - 1]);
    } while (extra_array_cnt >= 0);

    /* disable this because an item can be evicted by other threads */
//    ASSERT(0);
    return 0;
}
#endif


/*
 * relink is used when the item is moved from one segment to another
 *
 * a few caveats
 *  item being relinked could be outdated, in which case we should not relink
 *
 * TODO(jason): it might be better not clear those old entries?
 */
bool
hashtable_relink_it(const char *oit_key, const uint32_t oit_klen,
                    const uint64_t old_seg_id, const uint64_t old_offset,
                    const uint64_t new_seg_id, const uint64_t new_offset)
{
    INCR(seg_metrics, hash_remove);

    uint64_t hv         = CAL_HV(oit_key, oit_klen);
    uint64_t tag        = CAL_TAG_FROM_HV(hv);
    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *curr_bkt  = first_bkt;
    uint64_t item_info;
    bool item_outdated = true, first_match = true;

    uint64_t oit_info = _build_item_info(tag, old_seg_id, old_offset);
    uint64_t nit_info = _build_item_info(tag, new_seg_id, new_offset);

    lock(first_bkt);

    int bkt_chain_len = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int array_size;
    do {
        array_size =
            bkt_chain_len > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < array_size; i++) {
            if (curr_bkt == first_bkt && i == 0) {
                continue;
            }

            item_info = CLEAR_FREQ(curr_bkt[i]);
            if (GET_TAG(item_info) != tag) {
                continue;
            }

            /* a potential hit */
            if (!_same_item(oit_key, oit_klen, item_info)) {
                INCR(seg_metrics, hash_tag_collision);
                continue;
            }

            if (first_match) {
                if (oit_info == item_info) {
                    /* item is not outdated */
                    curr_bkt[i] = nit_info;
                    item_outdated = false;
                }
                first_match = false;
                continue;
            }

            /* not first match, delete */
            _item_free(curr_bkt[i], false);
            curr_bkt[i] = 0;
        }
        bkt_chain_len -= 1;
        curr_bkt   = (uint64_t *) (curr_bkt[N_SLOT_PER_BUCKET - 1]);
    } while (bkt_chain_len >= 0);

    unlock(first_bkt);

    return !item_outdated;
}

bool
hashtable_check_it(const char *oit_key, const uint32_t oit_klen,
                   const uint64_t seg_id, const uint64_t offset)
{
    INCR(seg_metrics, hash_remove);

    uint64_t hv         = CAL_HV(oit_key, oit_klen);
    uint64_t tag        = CAL_TAG_FROM_HV(hv);
    uint64_t *first_bkt = GET_BUCKET(hv);
    uint64_t *curr_bkt  = first_bkt;

    uint64_t oit_info = _build_item_info(tag, seg_id, offset);

    lock(first_bkt);

    int extra_array_cnt = GET_BUCKET_CHAIN_LEN(first_bkt) - 1;
    int array_size;
    do {
        array_size =
            extra_array_cnt > 0 ? N_SLOT_PER_BUCKET - 1 : N_SLOT_PER_BUCKET;

        for (int i = 0; i < array_size; i++) {
            if (oit_info == curr_bkt[i]) {
                unlock(first_bkt);
                return true;
            }
        }
        extra_array_cnt -= 1;
        curr_bkt   = (uint64_t *) (curr_bkt[N_SLOT_PER_BUCKET - 1]);
    } while (extra_array_cnt >= 0);

    unlock(first_bkt);

    return false;
}

void
hashtable_stat(int *item_cnt_ptr, int *bucket_cnt_ptr)
{
#define BUCKET_HEAD(idx) (&hash_table.table[(idx) * N_SLOT_PER_BUCKET])

    *item_cnt_ptr   = 0;
    *bucket_cnt_ptr = 0;

    int n_used_slots; /* the number of used slot in current bucket */
    int bkt_chain_len; /* the number of buckets in current bucket chain */

    uint64_t item_info, *head_bkt, *curr_bkt;

    for (uint64_t bucket_idx = 0;
         bucket_idx < HASHSIZE(hash_table.hash_power - N_SLOT_PER_BUCKET_LOG2);
         bucket_idx++) {

        head_bkt      = curr_bkt = BUCKET_HEAD(bucket_idx);
        bkt_chain_len = GET_BUCKET_CHAIN_LEN(head_bkt);
        *bucket_cnt_ptr += bkt_chain_len;
        do {
            n_used_slots = bkt_chain_len > 1 ?
                           N_SLOT_PER_BUCKET - 1 :
                           N_SLOT_PER_BUCKET;

            for (int i = 0; i < n_used_slots; i++) {
                /* this is bucket info */
                if (curr_bkt == head_bkt && i == 0) {
                    continue;
                }

                item_info = curr_bkt[i];
                if (item_info != 0) {
                    *item_cnt_ptr += 1;
                }
            }
            bkt_chain_len -= 1;
            if (bkt_chain_len > 0) {
                curr_bkt = (uint64_t *) (curr_bkt[N_SLOT_PER_BUCKET - 1]);
            }
        } while (bkt_chain_len > 0);
    }

    log_info("hashtable %d items, %d buckets", *item_cnt_ptr, *bucket_cnt_ptr);

#undef BUCKET_HEAD
}

void
scan_hashtable_find_seg(int32_t target_seg_id)
{
#define BUCKET_HEAD(idx) (&hash_table.table[(idx)*N_SLOT_PER_BUCKET])
    log_warn("expensive debug");

    int         extra_array_cnt;
    uint64_t    item_info;
    uint64_t    *bucket, *array;
    int         array_size;
    uint64_t    seg_id;
    uint64_t    offset;
    struct item *it;
    int         n_bucket =
                    HASHSIZE(hash_table.hash_power - N_SLOT_PER_BUCKET_LOG2);

    for (uint64_t bucket_idx = 0; bucket_idx < n_bucket; bucket_idx++) {
        bucket          = BUCKET_HEAD(bucket_idx);
        array           = bucket;
        extra_array_cnt = GET_BUCKET_CHAIN_LEN(bucket);
        int extra_array_cnt0 = extra_array_cnt;
        do {
            array_size = extra_array_cnt >= 1 ? N_SLOT_PER_BUCKET - 1 :
                         N_SLOT_PER_BUCKET;

            for (int i = 0; i < array_size; i++) {
                if (array == bucket && i == 0) {
                    continue;
                }

                item_info = array[i];

                if (item_info == 0) {
                    continue;
                }

                seg_id = ((item_info & SEG_ID_MASK) >> 20u);
                if (target_seg_id == seg_id) {
                    offset = (item_info & OFFSET_MASK) << 3u;
                    it =
                        (struct item *) (heap.base + heap.seg_size * seg_id +
                            offset);
                    log_warn(
                        "find item (%.*s) len %d on seg %d offset %d, item_info "
                        "%lu, i %d, extra %d %d",
                        it->klen, item_key(it), it->klen, seg_id, offset,
                        item_info, i,
                        extra_array_cnt0, extra_array_cnt);
                    ASSERT(0);
                }
            }
            extra_array_cnt -= 1;
            array      = (uint64_t *) (array[N_SLOT_PER_BUCKET - 1]);
        } while (extra_array_cnt >= 0);
    }

#undef BUCKET_HEAD
}