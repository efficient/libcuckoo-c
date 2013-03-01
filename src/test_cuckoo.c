/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   test_cuckoo.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Thu Feb 28 15:54:47 2013
 * 
 * @brief  a simple example of using cuckoo hash table
 * 
 * 
 */

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
    bool passed = true;
    int i;    
    size_t power = 19;
    size_t numkeys = (1 << power) * 4;

    printf("initializing two hash tables\n");

    cuckoo_hashtable_t* smalltable = cuckoo_init(power);
    cuckoo_hashtable_t* bigtable = cuckoo_init(power + 1);

    printf("inserting keys to the hash table\n");
    int failure = -1;
    for (i = 1; i < numkeys; i ++) {
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;
        cuckoo_status st;

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
        ValType val1, val2;
        KeyType key = (KeyType) i;
        cuckoo_status st1 = cuckoo_find(smalltable, (const char*) &key, (char*) &val1);
        cuckoo_status st2 = cuckoo_find(bigtable, (const char*) &key, (char*) &val2);
        if (i < failure) {
            if (st1 != ok) {
                printf("failure to read key %d from smalltable\n", i);
                passed = false;
                break;
            }
            if (val1 != i * 2 -1 ) {
                printf("smalltable reads wrong value for key %d\n", i);
                passed = false;
                break;
            }
        }
        else {
            if (st1 != failure_key_not_found) {
                printf(" key %d should not be in smalltable\n", i);
                passed = false;
                break;
            }
        }


        if (st2 != ok) {
            printf("failure to read key %d from bigtable\n", i);
            passed = false;
            break;
        }
        if (val2 != i * 2 -1 ) {
            printf("bigtable reads wrong value for key %d\n", i);
            passed = false;
            break;
        }


    }

    cuckoo_report(smalltable);
    cuckoo_exit(smalltable);

    cuckoo_report(bigtable);
    cuckoo_exit(bigtable);

    printf("[%s]\n", passed ? "PASSED" : "FAILED");

    return 0;
}
