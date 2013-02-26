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
    not_supported = 4
} cuckoo_status;


/** 
 * @brief Initialize the hash table
 * 
 * @param hashtable_init The logarithm of the initial table size
 */
cuckoo_status cuckoo_init(const int hashpower_init);

/** 
 * @brief Cleanup routine
 * 
 */
cuckoo_status cuckoo_exit(void);


/** 
 * @brief Lookup key in the hash table
 * 
 * @param key The key to search 
 * @param val The value to return
 * 
 * @return ok if key is found, not_found otherwise
 */
cuckoo_status cuckoo_find(const char *key, char *val);



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
cuckoo_status cuckoo_insert(const char *key, const char* val);


/** 
 * @brief Delete key/value from cuckoo hash table,
 * 
 * @param key The key to be deleted
 */
cuckoo_status cuckoo_delete(const char *key);


/** 
 * @brief Print stats of this hash table
 * 
 * 
 * @return Void
 */
cuckoo_status cuckoo_report(void);

#endif
