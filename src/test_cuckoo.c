#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <math.h>


#include "cuckoohash.h"

int main(int argc, char** argv) 
{

    int i;    
    size_t power = 19;
    size_t numkeys = (1 << power) * 4;
    cuckoo_hashtable_t* smalltable;
    cuckoo_hashtable_t* bigtable;

    printf("initializing two hash table\n");
    smalltable = cuckoo_init(power);
    bigtable   = cuckoo_init(power + 1);

    printf("inserting keys to the hash table\n");
    int failure = -1;
    for (i = 1; i < numkeys; i ++) {
        KeyType key;
        ValType val;
        cuckoo_status st;
        key = (KeyType) i;
        val = (ValType) i * 2 - 1;
        if (failure == -1) {
            st = cuckoo_insert(smalltable, (const char*) &key, (const char*) &val);
            if (st != ok) {
                printf("inserting key %d to smalltable fails \n", i);
                failure = i;
            }
        }
        st = cuckoo_insert(bigtable, (const char*) &key, (const char*) &val);
        if (st != ok) {
            printf("inserting key %d to bigtable fails \n", i);
            break;
        }
    }





    printf("looking up keys in the hash table\n");
    for (i = 1; i < numkeys; i ++) {
        KeyType key;
        ValType val1, val2;
        key = (KeyType) i;
        cuckoo_status st1 = cuckoo_find(smalltable, (const char*) &key, (char*) &val1);
        cuckoo_status st2 = cuckoo_find(bigtable, (const char*) &key, (char*) &val2);
        if (i < failure) {
            if (st1 != ok) {
                printf("failure to read key %d from smalltable\n", i);
                break;
            }
            if (st2 != ok) {
                printf("failure to read key %d from bigtable\n", i);
                break;
            }
            if (val1 != val2) {
                printf("smalltable and bigtable disagree on key %d\n", i);
                break;
            }
        }
        else {
            if (st1 != not_found) {
                printf(" key %d should not be in smalltable\n", i);
                break;
            }
        }

    }

    cuckoo_report(smalltable);
    cuckoo_exit(smalltable);

    cuckoo_report(bigtable);
    cuckoo_exit(bigtable);

    return 0;
}
