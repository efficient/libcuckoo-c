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



/* 
 * Number of items in the hash table. 
 */
static size_t hashsize;
static size_t hashitems;
static size_t hashpower;
static size_t hashmask;
static size_t hashitmes;



/*
 *  the mutex to serialize insert and delete
 *
 */
static pthread_mutex_t lock;

/*
 *  keyver_array is an array of version counters
 *  we keep keyver_count = 8192
 *
 */
#define  keyver_count ((uint32_t)1 << (13))
#define  keyver_mask  (keyver_count - 1)
static uint32_t keyver_array[keyver_count];

/**
 *  @brief Atomic read the counter
 *
 */
#define read_keyver(idx) \
    __sync_fetch_and_add(&keyver_array[idx & keyver_mask], 0)

/**
 * @brief Atomic increase the counter
 *
 */
#define incr_keyver(idx) \
    __sync_fetch_and_add(&keyver_array[idx & keyver_mask], 1)



/** 
 * @brief Compute the index of the first bucket
 * 
 * @param hv 32-bit hash value of the key
 * 
 * @return The first bucket
 */
static inline size_t _index_hash(const uint32_t hv) {
    return  (hv >> (32 - hashpower)); 
}


/** 
 * @brief Compute the index of the second bucket
 * 
 * @param hv 32-bit hash value of the key
 * @param index The index of the first bucket
 * 
 * @return  The second bucket
 */
static inline size_t _alt_index(const uint32_t hv, const size_t index) {
    // 0x5bd1e995 is the hash constant from MurmurHash2
    uint32_t tag = hv & 0xFF;
    return (index ^ (tag * 0x5bd1e995)) & hashmask;
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



/*
 * the structure of a bucket
 */
#define bucketsize 4
typedef struct {
    KeyType keys[bucketsize];
    ValType vals[bucketsize];
}  __attribute__((__packed__))
Bucket;

static  Bucket* buckets = NULL;


#define IS_SLOT_EMPTY(i,j) (buckets[i].keys[j] == 0)



typedef struct  {
    size_t buckets[NUM_CUCKOO_PATH];
    size_t slots[NUM_CUCKOO_PATH];
    KeyType keys[NUM_CUCKOO_PATH];
}  __attribute__((__packed__))
CuckooRecord;

static CuckooRecord cuckoo_path[MAX_CUCKOO_COUNT];
int       kick_count = 0;

/** 
 * @brief Make bucket from[idx] slot[whichslot] available to insert a new item
 * 
 * @param from:   the array of bucket index
 * @param whichslot: the slot available
 * @param  depth: the current cuckoo depth
 * 
 * @return idx on success, -1 otherwise
 */
static int _cuckoopath_search(size_t depth_start, size_t *cp_index) {

    int depth = depth_start;
    while ((kick_count < MAX_CUCKOO_COUNT) && 
           (depth >= 0) && 
           (depth < MAX_CUCKOO_COUNT - 1)) {

        CuckooRecord *curr = cuckoo_path + depth;
        CuckooRecord *next = cuckoo_path + depth + 1;
        
        /*
         * Check if any slot is already free
         */
        size_t idx;
        for (idx = 0; idx < NUM_CUCKOO_PATH; idx ++) {
            size_t i;
            size_t j;
            i = curr->buckets[idx];
            for (j = 0; j < bucketsize; j ++) {
                if (IS_SLOT_EMPTY(i, j)) {
                    curr->slots[idx] = j;
                    *cp_index   = idx;
                    return depth;
                }
            }

            /* pick the victim as the j-th item */
            j = rand() % bucketsize;
            curr->slots[idx] = j;
            curr->keys[idx]  = buckets[i].keys[j];
            uint32_t hv = hash((char*) &buckets[i].keys[j], sizeof(KeyType), 0);
            next->buckets[idx] = _alt_index(hv, i);
        }

        kick_count += NUM_CUCKOO_PATH;
        depth ++;
    }

    printf("%u max cuckoo achieved, abort\n", kick_count);
    return -1;
}

static int _cuckoopath_move(size_t depth_start, size_t idx) {
    
    int depth = depth_start;
    while (depth > 0) {

        /*
         * Move the key/value in  buckets[i1] slot[j1] to buckets[i2] slot[j2]
         * and make buckets[i1] slot[j1] available
         *
         */
        size_t i1 = cuckoo_path[depth - 1].buckets[idx];
        size_t j1 = cuckoo_path[depth - 1].slots[idx];
        size_t i2 = cuckoo_path[depth].buckets[idx];
        size_t j2 = cuckoo_path[depth].slots[idx];

        /*
         * We plan to kick out j1, but let's check if it is still there;
         * there's a small chance we've gotten scooped by a later cuckoo.
         * If that happened, just... try again.
         */
        if (!keycmp((char*) &buckets[i1].keys[j1], (char*) &cuckoo_path[depth - 1].keys[idx])) {
            /* try again */
            return depth;
        }

        assert(IS_SLOT_EMPTY(i2,j2));

        uint32_t hv = hash((char*) &buckets[i1].keys[j1], sizeof(KeyType), 0);
        size_t keylock   = _lock_index(hv);
        incr_keyver(keylock);

        buckets[i2].keys[j2] = buckets[i1].keys[j1];
        buckets[i2].vals[j2] = buckets[i1].vals[j1];
        buckets[i1].keys[j1] = 0;
        buckets[i1].vals[j1] = 0;

        incr_keyver(keylock);

        depth --;
    }

    return depth;

}

static int _run_cuckoo(size_t i1, size_t i2) {
    int cur;

    size_t idx;
    size_t depth = 0;
    for (idx = 0; idx < NUM_CUCKOO_PATH; idx ++) {
        if (idx< NUM_CUCKOO_PATH/2) 
            cuckoo_path[depth].buckets[idx] = i1;
        else
            cuckoo_path[depth].buckets[idx] = i2;
    }

    kick_count = 0;    
    while (1) {

        cur = _cuckoopath_search(depth, &idx);
        if (cur < 0)
            return -1;

        cur = _cuckoopath_move(cur, idx);
        if (cur == 0) {
            return idx;
        }

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
static bool _try_read_from_bucket(const char *key,  char *val, size_t i) {
    size_t  j;
    for (j = 0; j < bucketsize; j ++) {

        if (keycmp((char*) &buckets[i].keys[j], key)) {

            memcpy(val, (char*) &(buckets[i].vals[j]), sizeof(ValType));
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
static bool _try_add_to_bucket(const char* key, 
                                        const char* val, 
                                        size_t i, 
                                        size_t keylock) {
    size_t j;
    for (j = 0; j < bucketsize; j ++) {
        if (IS_SLOT_EMPTY(i, j)) {

            incr_keyver(keylock);

            memcpy(&buckets[i].keys[j], key, sizeof(KeyType));
            memcpy(&buckets[i].vals[j], val, sizeof(ValType));
            
            hashitems ++;

            incr_keyver(keylock);
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
static bool _try_del_from_bucket(const char*key, 
                                size_t i, 
                                size_t keylock) {
    size_t j;
    for (j = 0; j < bucketsize; j ++) {

        if (keycmp((char*) &buckets[i].keys[j], key)) {

            incr_keyver(keylock);

            buckets[i].keys[j] = 0;
            buckets[i].vals[j] = 0;

            hashitems --; 

            incr_keyver(keylock);
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
static cuckoo_status _cuckoo_find(const char *key, char *val, size_t i1, size_t i2, size_t keylock) {
    bool result;

    uint32_t vs, ve;
TryRead:
    vs = read_keyver(keylock);

    result = _try_read_from_bucket(key, val, i1);
    if (!result) {
       result = _try_read_from_bucket(key, val, i2);
    }

    ve = read_keyver(keylock);

    if (vs & 1 || vs != ve)
        goto TryRead;

    if (result) 
        return ok;
    else
        return not_found;
}

static cuckoo_status _cuckoo_insert(const char* key, const char* val, size_t i1, size_t i2, size_t keylock) {

    /*
     * try to add new key to bucket i1 first, then try bucket i2
     */
    if (_try_add_to_bucket(key, val, i1, keylock))
        return ok;
       
    if (_try_add_to_bucket(key, val, i2, keylock))
        return ok;


    /*
     * we are unlucky, so let's perform cuckoo hashing
     */
    int idx = _run_cuckoo(i1, i2);
    if (idx >= 0) {
        size_t i, j;
        i = cuckoo_path[0].buckets[idx];
        //j = cuckoo_path[0].slots[idx];
        if (_try_add_to_bucket(key, val, i, keylock)) {
            return ok;
        }
    }

    printf("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
           hashpower, hashitems, 1.0 * hashitems / bucketsize / hashsize);

    /*
     * todo , resize..
     */

    return not_enough_space;

}

static cuckoo_status _cuckoo_delete(const char* key,
                               size_t i1,
                               size_t i2,
                               size_t keylock) {
    if (_try_del_from_bucket(key, i1, keylock))
        return ok;

    if (_try_del_from_bucket(key, i2, keylock))
        return ok;

    return not_found;

}

/********************************************************************
 *               The interface of this hash table
 *********************************************************************/

cuckoo_status cuckoo_init(const int hashtable_init) {
    hashpower = HASHPOWER_DEFAULT;
    if (hashtable_init) {
        hashpower = hashtable_init;
    }

    hashsize = (uint32_t) 1 << (hashpower);
    hashmask = hashsize - 1;
    hashitems = 0;

    buckets = malloc(hashsize * sizeof(Bucket));
    if (! buckets) {
        fprintf(stderr, "Failed to init hashtable.\n");
        return not_enough_space;
    }

    memset(buckets, 0, sizeof(Bucket) * hashsize);

    pthread_mutex_init(&lock, NULL);

    memset(keyver_array, 0, sizeof(keyver_array));


    return ok;

}

cuckoo_status cuckoo_exit() {
    free(buckets);
    return ok;
}

cuckoo_status cuckoo_find(const char *key, char *val) {

    uint32_t hv    = hash(key, sizeof(KeyType), 0);
    size_t i1      = _index_hash(hv);
    size_t i2      = _alt_index(hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st = _cuckoo_find(key, val, i1, i2, keylock);

    return st;
}

cuckoo_status cuckoo_insert(const char *key, const char* val) {

    uint32_t hv = hash(key, sizeof(KeyType), 0);
    size_t i1   = _index_hash(hv);
    size_t i2   = _alt_index(hv, i1);
    size_t keylock = _lock_index(hv);

    ValType oldval;
    cuckoo_status st;

    mutex_lock(&lock);

    st = _cuckoo_find(key, (char*) &oldval, i1, i2, keylock);
    if  (st == ok) {
        return duplicated_key_found;
    }

    st =  _cuckoo_insert(key, val, i1, i2, keylock);
    
    mutex_unlock(&lock);

    return st;
}    

cuckoo_status cuckoo_delete(const char *key) {

    uint32_t hv = hash(key, sizeof(KeyType), 0);
    size_t i1   = _index_hash(hv);
    size_t i2   = _alt_index(hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st;

    mutex_lock(&lock);

    st = _cuckoo_delete(key, i1, i2, keylock);

    mutex_unlock(&lock);

    return st;
}

cuckoo_status cuckoo_report() {
    size_t sz = sizeof(Bucket) * hashsize;
    printf("[report]\n");
    printf("total number of items %zu\n", hashitems);
    printf("total size %zu Bytes, or %.2f MB\n", sz, (float) sz / (1 <<20));
    printf("load factor %.4f\n", (float) hashitems / bucketsize / hashsize);
}

