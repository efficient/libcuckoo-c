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

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <unistd.h>           /* for sleep */
#include <sys/time.h>        /* for gettimeofday */
#ifdef HAVE_GETOPT_H
#  include <getopt.h>
#endif

#include "cuckoohash.h"

#define million 1000000
#define VALUE(key) (3*key-15)

static cuckoo_hashtable_t* table = NULL;
static size_t power = 20;
static size_t total =  30 * million;
static size_t total_inserted;
static volatile bool keep_reading = true;
static volatile bool keep_writing = true;
static bool passed  = true;

typedef struct {
    size_t num_read;
    size_t num_written;
    size_t ops;
    size_t failures;
    int id;
} thread_arg_t;

static size_t task_next;
static size_t task_done;
static size_t task_size = 1 * million;
static size_t task_num;
pthread_mutex_t task_mutex;
bool* task_complete_flag;

static void task_init(size_t total) {
    pthread_mutex_init(&task_mutex, NULL);
    task_num = total / task_size;
    task_complete_flag = calloc(sizeof(bool), task_num);
    memset(task_complete_flag, false, task_num);
    task_next = 0;
    task_done = 0;
}

static size_t task_assign() {
    size_t ret;
    pthread_mutex_lock(&task_mutex);
    ret = task_next;
    task_next ++;
    pthread_mutex_unlock(&task_mutex);
    return ret;
}


static void task_complete(size_t task) {
    size_t i;

    pthread_mutex_lock(&task_mutex);

    task_complete_flag[task] = true;

    for (i = task_done; i < task_num; i++) {
        if (task_complete_flag[i]) {
            task_done = i;
        }
        else
            break;
    }
    total_inserted = task_done * task_size;

    if (i >= task_num) {
        keep_reading = false;
        keep_writing = false;
    }

    pthread_mutex_unlock(&task_mutex);
}

static void *lookup_thread(void *arg) {
    cuckoo_status st;
    KeyType key;
    ValType val;

    thread_arg_t* th = (thread_arg_t*) arg;
    th->ops         = 0;
    th->failures    = 0;
    th->num_read    = 0;
    th->num_written = 0;
    
    while (keep_reading) {
        if (total_inserted == 0) {
            continue;
        }

        size_t i = (int) (((float )rand() / RAND_MAX) * total_inserted);
        if (i == 0)
            i = 1;
        assert(i <= total_inserted);
        th->ops ++;
        key = (KeyType) i;
        st = cuckoo_find(table, (const char*) &key, (char*) &val);
        
        if (st != ok) {
            printf("[reader%d] reading key %zu from table fails\n", th->id, i);
            th->failures ++;
            continue;
        }
        if (val != VALUE(key)) {
            printf("[reader%d] wrong value for key %zu from table\n", th->id, i);
            th->failures ++;
            continue;
        }
        th->num_read ++;
    }
    printf("[reader%d] %zu lookups, %zu failures\n", th->id, th->ops, th->failures);
    if (th->failures > 0)
        passed = false;

    pthread_exit(NULL);
}

static void *insert_thread(void *arg) {
    cuckoo_status st;
    KeyType key;
    ValType val;

    thread_arg_t* th = (thread_arg_t*) arg;
    th->ops         = 0;
    th->failures    = 0;
    th->num_read    = 0;
    th->num_written = 0;
    
    while (keep_writing) {
        size_t i, task;
        task = task_assign();
        printf("[writer%d] gets task [%zu, %zu)\n", th->id, task * task_size, (task + 1) * task_size);
        for (i = task * task_size; i < (task + 1) * task_size; i ++) {
            if (i == 0) continue;
            th->ops ++;
            key = (KeyType) i;
            val = (ValType) VALUE(i);
            st = cuckoo_insert(table, (const char*) &key, (const char*) &val);

            if (st == ok) {
                th->num_written ++;

            }
            else if (st == failure_table_full) {

                printf("[writer%d] table is full when inserting key %zu\n", th->id, th->ops);
                //printf("[%s] grow table\n", name);
                st = cuckoo_expand(table);
                if (st == ok) {
                    //printf("[%s] grow table returns\n", name);
                    //expansion ++;
                    th->ops --;
                }
                else if (st == failure_under_expansion) {
                    printf("[writer%d] grow table is already on-going\n", th->id);
                    sleep(1);
                }
                else {
                    printf("[writer%d] unknown error for key %zu (%d)\n", th->id, i, st);
                    th->failures ++;
                }
            }
            else {
                printf("[writer%d] unknown error for key %zu (%d)\n", th->id, i, st);
                th->failures ++;
            }
        }
        printf("[writer%d] completes task [%zu, %zu)\n", th->id, task * task_size, (task + 1) * task_size);
        task_complete(task);
        
    }

    printf("[writer%d] %zu inserts, %zu failures\n", th->id, th->ops, th->failures);
    //printf("[writer%s] %zu expansion\n", th->id, expansion);
    if (th->failures > 0)
        passed = false;

    pthread_exit(NULL);
}

static void usage() {
    printf("test_cuckoo_mt:\ttest cuckoo hash table with multiple threads\n");
    printf("\t-r #: the number of readers\n");
    printf("\t-w #: the number of writers\n");
    printf("\t-h  : show usage\n");
}

int main(int argc, char** argv) 
{

    int i;
    int num_writers = 1;
    int num_readers = 1;
    struct timeval tvs, tve; 
    double tvsd, tved, tdiff;

    char ch; 
    while ((ch = getopt(argc, argv, "r:w:h")) != -1) {
        switch (ch) {
        case 'w': num_writers = atoi(optarg); break;
        case 'r': num_readers = atof(optarg); break;
        case 'h': usage(argv[0]); exit(0); break;
        default:
            usage(argv[0]);
            exit(-1);
        }   
    }  


    task_init(total);

    printf("initializing hash table\n");
    table = cuckoo_init(power);

    pthread_t* readers = calloc(sizeof(pthread_t), num_readers);
    pthread_t* writers = calloc(sizeof(pthread_t), num_writers);

    thread_arg_t* reader_args = calloc(sizeof(thread_arg_t), num_readers);
    thread_arg_t* writer_args = calloc(sizeof(thread_arg_t), num_writers);
    // create threads as writers
    for (i = 0; i < num_writers; i ++) {
        writer_args[i].id = i;
        if (pthread_create(&writers[i], NULL, insert_thread, &writer_args[i]) != 0) {
            fprintf(stderr, "Can't create thread for writer%d\n", i);
            exit(-1);
        }
    }

    // create threads as readers
    for (i = 0; i < num_readers; i ++) {
        reader_args[i].id = i;
        if (pthread_create(&readers[i], NULL, lookup_thread, &reader_args[i]) != 0) {
            fprintf(stderr, "Can't create thread for reader%d\n", i);
            exit(-1);
        }
    }


    gettimeofday(&tvs, NULL); 
    tvsd = (double)tvs.tv_sec + (double)tvs.tv_usec/1000000;

    size_t* last_num_read = calloc(sizeof(size_t), num_readers);
    size_t* last_num_written = calloc(sizeof(size_t), num_writers);
    memset(last_num_read, 0, num_readers);
    memset(last_num_written, 0, num_writers);
    while (keep_reading && keep_writing) {
        sleep(1);
        gettimeofday(&tve, NULL); 
        tvsd = (double)tvs.tv_sec + (double)tvs.tv_usec/1000000;
        tved = (double)tve.tv_sec + (double)tve.tv_usec/1000000;
        tdiff = tved - tvsd;
        printf("[stats (MOPS)] ");
        for (i = 0; i < num_readers; i ++) {
            printf("reader%d %4.2f ", i, (reader_args[i].num_read - last_num_read[i])/ tdiff/ million );
            last_num_read[i] = reader_args[i].num_read;
        }
        for (i = 0; i < num_writers; i ++) {
            printf("writer%d %4.2f ", i, (writer_args[i].num_written - last_num_written[i])/ tdiff/ million );
            last_num_written[i] = writer_args[i].num_written;
        }
        printf("\n");
        tvs = tve;
    }

    for (i = 0; i < num_readers; i ++) {
        pthread_join(readers[i], NULL);
    }

    for (i = 0; i < num_writers; i ++) {
        pthread_join(writers[i], NULL);
    }


    cuckoo_report(table);
    cuckoo_exit(table);

    printf("[%s]\n", passed ? "PASSED" : "FAILED");
}
