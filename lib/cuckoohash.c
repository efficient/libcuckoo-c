/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   cuckoohash.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Mon Feb 25 22:17:04 2013
 * 
 * @brief  implementation of single-writer/multi-reader cuckoo hash
 * 
 * 
 */

#include "cuckoohash.h"

/*
 * default hash table size
 */
#define HASHPOWER_DEFAULT 16

/*
 * The maximum number of cuckoo operations per insert, 
 */
#define MAX_CUCKOO_COUNT 500

/*
 * The number of cuckoo paths 
 */
#define NUM_CUCKOO_PATH 2

#define  keyver_count ((uint32_t)1 << (13))
#define  keyver_mask  (keyver_count - 1)


/*
 * the structure of a bucket
 */
#define bucketsize 4
typedef struct {
    KeyType keys[bucketsize];
    ValType vals[bucketsize];
}  __attribute__((__packed__))
Bucket;


/**
 *  @brief Atomic read the counter
 *
 */
#define read_keyver(h, idx)                                      \
    __sync_fetch_and_add(&((uint32_t*) h->keyver_array)[idx & keyver_mask], 0)

/**
 * @brief Atomic increase the counter
 *
 */
#define incr_keyver(h, idx)                                      \
    __sync_fetch_and_add(&((uint32_t*) h->keyver_array)[idx & keyver_mask], 1)

//#include "city.h"
static inline  uint32_t hash(const char* buf, size_t len, const uint32_t initval) {
    return CityHash32(buf, len);
}


/** 
 * @brief Compute the index of the first bucket
 * 
 * @param hv 32-bit hash value of the key
 * 
 * @return The first bucket
 */
static inline size_t _index_hash(cuckoo_hashtable_t* h, 
                                 const uint32_t hv) {
    return  (hv >> (32 - h->hashpower)); 
}


/** 
 * @brief Compute the index of the second bucket
 * 
 * @param hv 32-bit hash value of the key
 * @param index The index of the first bucket
 * 
 * @return  The second bucket
 */
static inline size_t _alt_index(cuckoo_hashtable_t* h, 
                                const uint32_t hv, 
                                const size_t index) {
    // 0x5bd1e995 is the hash constant from MurmurHash2
    uint32_t tag = hv & 0xFF;
    return (index ^ (tag * 0x5bd1e995)) & h->hashmask;
}

/** 
 * @brief Compute the index of the corresponding counter in keyver_array
 * 
 * @param hv 32-bit hash value of the key
 * 
 * @return The index of the counter
 */
static inline size_t _lock_index(const uint32_t hv) {
    return hv & keyver_mask;
}


#define TABLE_KEY(h, i, j) ((Bucket*) h->buckets)[i].keys[j]
#define TABLE_VAL(h, i, j) ((Bucket*) h->buckets)[i].vals[j]
#define IS_SLOT_EMPTY(h, i, j) (TABLE_KEY(h, i, j)==0)



typedef struct  {
    size_t buckets[NUM_CUCKOO_PATH];
    size_t slots[NUM_CUCKOO_PATH];
    KeyType keys[NUM_CUCKOO_PATH];
}  __attribute__((__packed__))
CuckooRecord;



/** 
 * @brief Make bucket from[idx] slot[whichslot] available to insert a new item
 * 
 * @param from:   the array of bucket index
 * @param whichslot: the slot available
 * @param  depth: the current cuckoo depth
 * 
 * @return depth on success, -1 otherwise
 */
static int _cuckoopath_search(cuckoo_hashtable_t* h,
                              size_t depth_start, 
                              size_t *cp_index) {

    int depth = depth_start;
    while ((h->kick_count < MAX_CUCKOO_COUNT) && 
           (depth >= 0) && 
           (depth < MAX_CUCKOO_COUNT - 1)) {

        CuckooRecord *curr = ((CuckooRecord*) h->cuckoo_path) + depth;
        CuckooRecord *next = ((CuckooRecord*) h->cuckoo_path) + depth + 1;
        
        /*
         * Check if any slot is already free
         */
        size_t idx;
        for (idx = 0; idx < NUM_CUCKOO_PATH; idx ++) {
            size_t i;
            size_t j;
            i = curr->buckets[idx];
            for (j = 0; j < bucketsize; j ++) {
                if (IS_SLOT_EMPTY(h, i, j)) {
                    curr->slots[idx] = j;
                    *cp_index   = idx;
                    return depth;
                }
            }

            /* pick the victim as the j-th item */
            j = rand() % bucketsize;
            curr->slots[idx] = j;
            curr->keys[idx]  = TABLE_KEY(h, i, j);
            uint32_t hv = hash((char*) &TABLE_KEY(h, i, j), sizeof(KeyType), 0);
            next->buckets[idx] = _alt_index(h, hv, i);
        }

        h->kick_count += NUM_CUCKOO_PATH;
        depth ++;
    }

    printf("%zu max cuckoo achieved, abort\n", h->kick_count);
    return -1;
}

static int _cuckoopath_move(cuckoo_hashtable_t* h, 
                            size_t depth_start, 
                            size_t idx) {
    
    int depth = depth_start;
    while (depth > 0) {

        /*
         * Move the key/value in  buckets[i1] slot[j1] to buckets[i2] slot[j2]
         * and make buckets[i1] slot[j1] available
         *
         */
        CuckooRecord *from = ((CuckooRecord*) h->cuckoo_path) + depth - 1;
        CuckooRecord *to   = ((CuckooRecord*) h->cuckoo_path) + depth;
        size_t i1 = from->buckets[idx];
        size_t j1 = from->slots[idx];
        size_t i2 = to->buckets[idx];
        size_t j2 = to->slots[idx];

        /*
         * We plan to kick out j1, but let's check if it is still there;
         * there's a small chance we've gotten scooped by a later cuckoo.
         * If that happened, just... try again.
         */
        if (!keycmp((char*) &TABLE_KEY(h, i1, j1), (char*) &(from->keys[idx]))) {
            /* try again */
            return depth;
        }

        assert(IS_SLOT_EMPTY(h, i2, j2));

        uint32_t hv = hash((char*) &TABLE_KEY(h, i1, j1), sizeof(KeyType), 0);
        size_t keylock   = _lock_index(hv);

        incr_keyver(h, keylock);

        TABLE_KEY(h, i2, j2) = TABLE_KEY(h, i1, j1);
        TABLE_VAL(h, i2, j2) = TABLE_VAL(h, i1, j1);
        TABLE_KEY(h, i1, j1) = 0;
        TABLE_VAL(h, i1, j1) = 0;

        incr_keyver(h, keylock);

        depth --;
    }

    return depth;

}

static int _run_cuckoo(cuckoo_hashtable_t* h,
                       size_t i1, 
                       size_t i2) {
    int cur;

    size_t idx;
    size_t depth = 0;
    for (idx = 0; idx < NUM_CUCKOO_PATH; idx ++) {
        if (idx< NUM_CUCKOO_PATH/2) 
            ((CuckooRecord*) h->cuckoo_path)[depth].buckets[idx] = i1;
        else
            ((CuckooRecord*) h->cuckoo_path)[depth].buckets[idx] = i2;
    }

    h->kick_count = 0;    
    while (1) {

        cur = _cuckoopath_search(h, depth, &idx);
        if (cur < 0)
            return -1;

        cur = _cuckoopath_move(h, cur, idx);
        if (cur == 0)
            return idx;

        depth = cur - 1;
    }

    return -1;
}


/** 
 * @brief Try to read bucket i and check if the given key is there
 * 
 * @param key The key to search
 * @param val The address to copy value to
 * @param i Index of bucket 
 * 
 * @return true if key is found, false otherwise
 */
static bool _try_read_from_bucket(cuckoo_hashtable_t* h,
                                  const char *key,  
                                  char *val, 
                                  size_t i) {
    size_t  j;
    for (j = 0; j < bucketsize; j ++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {

            memcpy(val, (char*) &TABLE_VAL(h, i, j), sizeof(ValType));
            return true;
        }
    }
    return false;
}

/** 
 * @brief Try to add key/val to bucket i, 
 * 
 * @param key Pointer to the key to store
 * @param val Pointer to the value to store
 * @param i Bucket index
 * @param keylock The index of key version counter
 * 
 * @return true on success and false on failure 
 */
static bool _try_add_to_bucket(cuckoo_hashtable_t* h,
                               const char* key, 
                               const char* val, 
                               size_t i, 
                               size_t keylock) {
    size_t j;
    for (j = 0; j < bucketsize; j ++) {
        if (IS_SLOT_EMPTY(h, i, j)) {

            incr_keyver(h, keylock);

            memcpy(&TABLE_KEY(h, i, j), key, sizeof(KeyType));
            memcpy(&TABLE_VAL(h, i, j), val, sizeof(ValType));
            
            h->hashitems ++;

            incr_keyver(h, keylock);
            return true;
        }
    }
    return false;
}




/** 
 * @brief Try to delete key and its corresponding value from bucket i, 
 * 
 * @param key Pointer to the key to store
 * @param i Bucket index
 * @param keylock The index of key version counter

 * @return true if key is found, false otherwise
 */
static bool _try_del_from_bucket(cuckoo_hashtable_t* h,
                                 const char*key, 
                                 size_t i, 
                                 size_t keylock) {
    size_t j;
    for (j = 0; j < bucketsize; j ++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {

            incr_keyver(h, keylock);

            TABLE_KEY(h, i, j) = 0;
            TABLE_VAL(h, i, j) = 0;
            /* buckets[i].keys[j] = 0; */
            /* buckets[i].vals[j] = 0; */

            h->hashitems --; 

            incr_keyver(h, keylock);
            return true;
        }
    }
    return false;
}


/** 
 * @brief internal of cuckoo_find
 * 
 * @param key 
 * @param val 
 * @param i1 
 * @param i2 
 * @param keylock 
 * 
 * @return 
 */
static cuckoo_status _cuckoo_find(cuckoo_hashtable_t* h,
                                  const char *key, 
                                  char *val, 
                                  size_t i1, 
                                  size_t i2, 
                                  size_t keylock) {
    bool result;

    uint32_t vs, ve;
TryRead:
    vs = read_keyver(h, keylock);

    result = _try_read_from_bucket(h, key, val, i1);
    if (!result) {
        result = _try_read_from_bucket(h, key, val, i2);
    }

    ve = read_keyver(h, keylock);

    if (vs & 1 || vs != ve)
        goto TryRead;

    if (result) 
        return ok;
    else
        return not_found;
}

static cuckoo_status _cuckoo_insert(cuckoo_hashtable_t* h,
                                    const char* key, 
                                    const char* val, 
                                    size_t i1, 
                                    size_t i2, 
                                    size_t keylock) {

    /*
     * try to add new key to bucket i1 first, then try bucket i2
     */
    if (_try_add_to_bucket(h, key, val, i1, keylock))
        return ok;
       
    if (_try_add_to_bucket(h, key, val, i2, keylock))
        return ok;


    /*
     * we are unlucky, so let's perform cuckoo hashing
     */
    int idx = _run_cuckoo(h, i1, i2);
    if (idx >= 0) {
        size_t i;
        i = ((CuckooRecord*) h->cuckoo_path)[0].buckets[idx];
        //j = cuckoo_path[0].slots[idx];
        if (_try_add_to_bucket(h, key, val, i, keylock)) {
            return ok;
        }
    }

    printf("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
           h->hashpower, h->hashitems, 1.0 * h->hashitems / bucketsize / h->hashsize);

    /*
     * todo , resize..
     */

    return not_enough_space;

}

static cuckoo_status _cuckoo_delete(cuckoo_hashtable_t* h,
                                    const char* key,
                                    size_t i1,
                                    size_t i2,
                                    size_t keylock) {
    if (_try_del_from_bucket(h, key, i1, keylock))
        return ok;

    if (_try_del_from_bucket(h, key, i2, keylock))
        return ok;

    return not_found;

}

/********************************************************************
 *               The interface of this hash table
 *********************************************************************/

cuckoo_hashtable_t* cuckoo_init(const int hashtable_init) {
    cuckoo_hashtable_t* h = (cuckoo_hashtable_t*) malloc(sizeof(cuckoo_hashtable_t));
    if (!h)
        goto Cleanup;
    memset(h, 0, sizeof(*h));

    h->hashpower  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
    h->hashsize   = (uint32_t) 1 << (h->hashpower);
    h->hashmask   = h->hashsize - 1;
    h->hashitems  = 0;
    h->kick_count = 0;

    h->buckets = malloc(h->hashsize * sizeof(Bucket));
    if (! h->buckets) {
        fprintf(stderr, "Failed to init hashtable.\n");
        goto Cleanup;
    }
    memset(h->buckets, 0, sizeof(Bucket) * h->hashsize);

    pthread_mutex_init(&h->lock, NULL);

    h->keyver_array = (uint32_t*) malloc(keyver_count * sizeof(uint32_t));
    if (! h->keyver_array) {
        fprintf(stderr, "Failed to init key version array.\n");
        goto Cleanup;
    }
    memset(h->keyver_array, 0, keyver_count * sizeof(uint32_t));

    h->cuckoo_path = malloc(MAX_CUCKOO_COUNT * sizeof(CuckooRecord));
    if (! h->cuckoo_path) {
        fprintf(stderr, "Failed to init cuckoo path.\n");
        goto Cleanup;
    }
    memset(h->cuckoo_path, 0, MAX_CUCKOO_COUNT * sizeof(CuckooRecord));

    return h;

Cleanup:
    if (h) {
        free(h->cuckoo_path);
        free(h->keyver_array);
        free(h->buckets);
    }
    free(h);
    return NULL;
    
}

cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h) {
    free(h->buckets);
    free(h->keyver_array);
    free(h);
    return ok;
}

cuckoo_status cuckoo_find(cuckoo_hashtable_t* h,
                          const char *key, 
                          char *val) {

    uint32_t hv    = hash(key, sizeof(KeyType), 0);
    size_t i1      = _index_hash(h, hv);
    size_t i2      = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st = _cuckoo_find(h, key, val, i1, i2, keylock);

    return st;
}

cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h,
                            const char *key, 
                            const char* val) {

    uint32_t hv = hash(key, sizeof(KeyType), 0);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    ValType oldval;
    cuckoo_status st;

    mutex_lock(&h->lock);

    st = _cuckoo_find(h, key, (char*) &oldval, i1, i2, keylock);
    if  (st == ok) {
        return duplicated_key_found;
    }

    st =  _cuckoo_insert(h, key, val, i1, i2, keylock);
    
    mutex_unlock(&h->lock);

    return st;
}    

cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h, 
                            const char *key) {

    uint32_t hv = hash(key, sizeof(KeyType), 0);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st;

    mutex_lock(&h->lock);

    st = _cuckoo_delete(h, key, i1, i2, keylock);

    mutex_unlock(&h->lock);

    return st;
}

void cuckoo_report(cuckoo_hashtable_t* h) {

    size_t sz;
    sz = sizeof(Bucket) * h->hashsize;
    printf("[report]\n");
    printf("total number of items %zu\n", h->hashitems);
    printf("total size %zu Bytes, or %.2f MB\n", sz, (float) sz / (1 <<20));
    printf("load factor %.4f\n", 1.0 * h->hashitems / bucketsize / h->hashsize);
}

