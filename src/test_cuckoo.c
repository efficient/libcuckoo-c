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
    size_t power = 20;
    size_t numkeys = (1 << power) * 4;

    printf("initializing a hash table\n");
    cuckoo_init(power);

    printf("inserting keys to the hash table\n");
    for (i = 1; i < numkeys; i ++) {
        KeyType key;
        ValType val;
        key = (KeyType) i;
        val = (ValType) i * 2 - 1;
        cuckoo_status st = cuckoo_insert((const char*) &key, (const char*) &val);

        if (st != ok) {
             printf("insert failure at key %d\n", i);
             break;
        }
    }

    int failure = i;

    printf("looking up keys in the hash table\n");
    for (i = 1; i < numkeys; i ++) {
        KeyType key;
        ValType val;
        key = (KeyType) i;
        cuckoo_status st = cuckoo_find((const char*) &key, (char*) &val);
        if (i < failure) {
            if (st != ok) {
                printf("failure to read key %d\n", i);
                break;
            }
            if (val != 2 * i - 1) {
                printf("wrong value for key %d\n", i);
                break;
            }
        }
        else {
            if (st != not_found) {
                printf("found key %d, which should not be in the table\n", i);
                break;
            }
        }

    }

    cuckoo_report();

    cuckoo_exit();

}
