#ifndef _CUCKOOHASH_H
#define _CUCKOOHASH_H

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>



#include "config.h"
#include "cuckoohash_config.h"
#include "hash.h"
#include "util.h"


typedef enum {
    ok = 0,
    not_found = 1,
    not_enough_space = 2,
    duplicated_key_found = 3,
    not_supported = 4,
    null_hashtable = 5,
} cuckoo_status;





/*
 * the structure of a buckoo hash table
 */
typedef struct {

    size_t hashsize;
    size_t hashitems;
    size_t hashpower;
    size_t hashmask;
    size_t hashitmes;
    void*  buckets;

    /*
     *  keyver_array is an array of version counters
     *  we keep keyver_count = 8192
     *
     */
    void* keyver_array;

    /* the mutex to serialize insert and delete */
    pthread_mutex_t lock;

    /* record the path */
    void* cuckoo_path;

    size_t kick_count;

} cuckoo_hashtable_t;



/** 
 * @brief Initialize the hash table
 * 
 * @param hashtable_init The logarithm of the initial table size
 *
 * @return the hashtable structure on success, NULL on failure
 */
cuckoo_hashtable_t* cuckoo_init(const int hashpower_init);

/** 
 * @brief Cleanup routine
 * 
 */
cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h);


/** 
 * @brief Lookup key in the hash table
 * 
 * @param key The key to search 
 * @param val The value to return
 * 
 * @return ok if key is found, not_found otherwise
 */
cuckoo_status cuckoo_find(cuckoo_hashtable_t* h, const char *key, char *val);



/** 
 * @brief Insert key/value to cuckoo hash table,
 * 
 *  Inserting new key/value pair. 
 *  If the key is already inserted, the new value will not be inserted
 *
 *
 * @param key The key to be inserted
 * @param val The value to be inserted
 * 
 * @return ok if key/value are succesfully inserted
 */
cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h, const char *key, const char* val);


/** 
 * @brief Delete key/value from cuckoo hash table,
 * 
 * @param key The key to be deleted
 */
cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h, const char *key);


/** 
 * @brief Print stats of this hash table
 * 
 * 
 * @return Void
 */
void cuckoo_report(cuckoo_hashtable_t* h);

#endif
