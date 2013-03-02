/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   test_cuckoo.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Thu Feb 28 15:54:47 2013
 * 
 * @brief  a simple example of using cuckoo hash table with multiple threads
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
#include <unistd.h>           /* for sleep */

#include "cuckoohash.h"

#define million 1000000
#define VALUE(key) (3*key-15)

static size_t power = 23;
static size_t numkeys = 0;
static size_t total =  100 * million;
static bool keep_reading = true;
static bool keep_writing = true;
static bool passed  = true;
static cuckoo_hashtable_t* table = NULL;

static void *lookup_thread(void *arg) {
    cuckoo_status st;
    char name[]="reader";
    KeyType key;
    ValType val;
    size_t ops = 0;
    size_t failures = 0;

    
    while (keep_reading) {
        if (numkeys == 0) {
            continue;
        }

        size_t i = (int) (((float )rand() / RAND_MAX) * numkeys);
        if (i == 0)
            i = 1;
        assert(i <= numkeys);
        key = (KeyType) i;
        st = cuckoo_find(table, (const char*) &key, (char*) &val);
        ops ++;
        
        if (st != ok) {
            printf("[%s] reading key %zu from table fails\n", name, i);
            failures ++;
            continue;
        }
        if (val != VALUE(key)) {
            printf("[%s] wrong value for key %zu from table\n", name, i);
            failures ++;
            continue;
        }

    }
    printf("[%s] %zu lookups, %zu failures\n", name, ops, failures);
    if (failures > 0)
        passed = false;

    pthread_exit(NULL);
}

static void *insert_thread(void *arg) {
    cuckoo_status st;
    char name[]="writer";
    KeyType key;
    ValType val;
    size_t ops = 0;
    size_t failures = 0;
    size_t expansion = 0;

    while (keep_writing) {
        ops ++;
        key = (KeyType) ops;
        val = (ValType) VALUE(ops);
        st = cuckoo_insert(table, (const char*) &key, (const char*) &val);

        if (st == ok) {

        }
        else if (st == failure_table_full) {

            printf("[%s] table is full when inserting key %zu\n", name, ops);
            printf("[%s] grow table\n", name);
            st = cuckoo_expand(table);
            if (st == ok) {
                printf("[%s] grow table returns\n", name);
                expansion ++;
                ops --;
            }
            else if (st == failure_under_expansion) {
                printf("[%s] grow table is already on-going\n", name);
                sleep(1);
            }
            else {
                printf("[%s] unknown error\n", name);
                failures ++;
            }
        }
        else {
            printf("[%s] unknown error\n", name);
            failures ++;
        }
        
        if ((ops % 10000) == 0) {
            numkeys = ops;
        }

        if (ops == total ) {
            break;
        }

    }
    keep_reading = false;
    keep_writing = false;

    printf("[%s] %zu inserts, %zu failures\n", name, ops, failures);
    printf("[%s] %zu expansion\n", name, expansion);
    if (failures > 0)
        passed = false;

    pthread_exit(NULL);
}

int main(int argc, char** argv) 
{


    printf("initializing hash table\n");

    table = cuckoo_init(power);
    pthread_t reader, writer;
    
    
    if (pthread_create(&writer, NULL, insert_thread, table) != 0) {
        fprintf(stderr, "Can't create thread\n");
    }

    if (pthread_create(&reader, NULL, lookup_thread, table) != 0) {
        fprintf(stderr, "Can't create thread\n");
    }

    pthread_join(writer, NULL);
    pthread_join(reader, NULL);

    cuckoo_report(table);
    cuckoo_exit(table);

    printf("[%s]\n", passed ? "PASSED" : "FAILED");
}
