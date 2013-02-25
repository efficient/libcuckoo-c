#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <math.h>

#include "bench_common.h"
#include "bench_config.h"
#include "bench_util.h"


#ifdef TEST_ORIGINAL
#define ASSOC_INIT       assoc_init
#define ASSOC_DESTROY    assoc_destroy
#define ASSOC_INSERT     assoc_insert
#define ASSOC_FIND       assoc_find
#define ASSOC_PRE_BENCH  assoc_pre_bench
#define ASSOC_POST_BENCH assoc_post_bench
#endif

#ifdef TEST_CUCKOO
#define ASSOC_INIT       assoc2_init
#define ASSOC_DESTROY    assoc2_destroy
#define ASSOC_INSERT     assoc2_insert
#define ASSOC_FIND       assoc2_find
#define ASSOC_PRE_BENCH  assoc2_pre_bench
#define ASSOC_POST_BENCH assoc2_post_bench
#endif



/* Lock for cache operations (item_*, assoc_*) */
pthread_mutex_t cache_lock;
static pthread_spinlock_t cache_lock1;

void STATS_LOCK() {}

void STATS_UNLOCK() {}

/** exported globals **/
struct stats stats;
struct settings settings;


pthread_mutex_t mymutex;

/* typedef struct { */
/*     char it[sizeof(item)]; */
/*     char key[NKEY]; */
/* } kv_unit; */


static uint32_t myhashpower = 0;
static int num_threads = 1;
static size_t num_queries = 100*MILLION; //100*MILLION;
static double duration = 10.0;
static double writeratio = 0.00;
static hash_query *queries = NULL;
static size_t nkeys_lookup = 100*MILLION;
static size_t nkeys_insert = 20*MILLION;
static size_t coverage;
static bool verbose = false;
static int number = 1;


static int ht_insert(item *it, const uint32_t hv) {
    // insert is protected by a global lock
#if defined(NO_LOCKING) || defined(ENABLE_OPT_LOCK)
    int ret = ASSOC_INSERT(it, hv);
#else
    //mutex_lock(&cache_lock);
    pthread_spin_lock(&cache_lock1);

    int ret = ASSOC_INSERT(it, hv);
    //mutex_unlock(&cache_lock);
    pthread_spin_unlock(&cache_lock1);
#endif
    return ret;
}

static item *ht_lookup(const char *key, const uint32_t hv) {
#if defined(NO_LOCKING) || defined(ENABLE_OPT_LOCK)
    item *it = ASSOC_FIND(key, NKEY, hv);
#else
    //mutex_lock(&cache_lock);
    pthread_spin_lock(&cache_lock1);
    item *it = ASSOC_FIND(key, NKEY, hv);
    //pthread_mutex_unlock(&cache_lock);
    pthread_spin_unlock(&cache_lock1);
#endif
    return it;
}


static void print_stats() {
    printf("stats:\n");
    printf("\thash_power_level=%d\n", stats.hash_power_level);
    printf("\thash_bytes=%" PRIu64"\n", stats.hash_bytes);
    printf("\tcurr_items=%d\n", stats.curr_items);
    printf("\n");
}

static void init(unsigned int hashpower) {
    srand( time(NULL) );
    //pthread_mutex_init(&cache_lock, NULL);
    pthread_spin_init(&cache_lock1, PTHREAD_PROCESS_PRIVATE);

    memset(&stats, 0, sizeof(struct stats));
    memset(&settings, 0, sizeof(struct settings));
    
    settings.verbose = 2;

/* #ifdef TEST_ORIGINAL */
/*     if (start_assoc_maintenance_thread() == -1) { */
/*         exit(-1); */
/*     } */
/* #endif */

    ASSOC_INIT(hashpower);
}


static hash_query *init_queries(size_t num, char* filename)
{
    static int batch = 0;
    batch ++;

    hash_query *queries = calloc(num, sizeof(hash_query));
    if (!queries) {
        perror("not enough memory to init queries\n");
        exit(-1);
    }
    item *items  = calloc(num, sizeof(item));
    if (!items) {
        perror("not enough memory for items\n");
        exit(-1);
    }

    uint32_t *hvs = calloc(num, sizeof(uint32_t));
    char     *keys = (char*) malloc(num * NKEY);
    FILE *fd;
    size_t count;
    fd = fopen(filename, "rb");
    if (fd) {
        printf("read trace file %s\n", filename);
        count = fread(keys, NKEY, num, fd);
        if (count < num) {
            perror("write err\n");
            exit(-1);
        }
        count = fread(hvs, sizeof(uint32_t), num, fd);
        if (count < num) {
            perror("write err\n");
            exit(-1);
        }
        fclose(fd);

    }
    else {
        printf("can not open file %s, let us make it\n", filename);
        for (size_t i = 0; i < num; i++) {
            char *key = keys+(i*NKEY);
            sprintf(key, "key-%d-%"PRIu64"0", batch, (uint64_t) i);
            hvs[i] = hash(key, NKEY, 0);
        }
        
        fd = fopen(filename, "wb");
        if (fd) {
            count = fwrite(keys, NKEY, num, fd);
            if (count < num) {
                perror("write err\n");
                exit(-1);
            }
            count = fwrite(hvs, sizeof(uint32_t), num, fd);
            if (count < num) {
                perror("write err\n");
                exit(-1);
            }
            fclose(fd);
        } else {
            perror("can not dump\n");
            exit(-1);
        }

    }
    


    memset(queries, 0, sizeof(hash_query) * num);
    memset(items, 0, sizeof(item) * num);

    for (size_t i = 0; i < num; i++) {
        char* key = keys + i * NKEY;

        memcpy(queries[i].hashed_key, key, NKEY);
        queries[i].hv = hvs[i];
        
        item* it =  &(items[i]);
        it->it_flags = settings.use_cas ? ITEM_CAS : 0;
        it->nkey = NKEY;
#ifdef TEST_LRU
        it->next = it->prev = 0;
#endif
#ifdef TEST_ORIGINAL
        it->h_next = 0;
#endif
        memcpy(ITEM_key(it), queries[i].hashed_key, NKEY);

        queries[i].ptr = (void*) it;
    }
        
    free(hvs);
    free(keys);
    return queries;
}

static void insert_items(size_t num, hash_query *queries)
{
    size_t total = 0;
    for (size_t i = 0; i < num; i ++) {
        int ret = ht_insert((item*) queries[i].ptr, queries[i].hv);
        if (ret == 0)
            break;
        total += 1;
    }
    printf("insert_items done: %zu items of %zu inserted\n", total, num);

    print_stats();

}

typedef struct {
    int    tid;
    double time;
    double tput;
    size_t gets;
    size_t puts;
    size_t hits;
    int cpu;
} thread_param;

static void* exec_queries(void *a) //size_t num, hash_query *queries)
{
    struct timeval tv_s, tv_e;

    thread_param *tp = (thread_param*) a;
    tp->time = 0; 
    tp->hits = 0;
    tp->gets = 0;
    tp->puts = 0;
    tp->cpu =  sched_getcpu();

    if (verbose) {
        pthread_mutex_lock (&mymutex);
        printf("thread%d (on CPU %d) started \n", tp->tid, sched_getcpu());
        pthread_mutex_unlock (&mymutex);
    }
    size_t nq = num_queries / num_threads;
    //printf("nq = %"PRIu64" , num_threads = %d\n", nq, num_threads);
    hash_query *q = queries + nq * tp->tid;

    size_t k = 0;
    size_t left = nq;
    bool done = false;
    while (!done && tp->time < duration && left > 0) {
        size_t step;
        if (left >= 1000000)
            step = 1000000;
        else
            step = left;
    
        gettimeofday(&tv_s, NULL);
        for (size_t j = 0; j < step; j++, k ++) {
            if (q[k].type == query_get) {
                tp->gets ++;
                item* it = ht_lookup(q[k].hashed_key,  q[k].hv);
                tp->hits += (it != NULL);
                
            }
            else if (q[k].type == query_put) {
                tp->puts ++;
                int ret = ht_insert((item*) q[k].ptr, q[k].hv);
                if (ret == 0) {
                    done = true;
                    break;
                }
            } else {
                perror("unknown query\n");
                exit(0);
            }
        }
        gettimeofday(&tv_e, NULL);
        tp->time += timeval_diff(&tv_s, &tv_e);
        left = left - step;
     }

    tp->tput = (float) k / tp->time;

    pthread_exit(NULL);
}


static void usage(char* binname) 
{
    printf("%s\n", binname);
    printf("\t-l  : benchmark lookup\n");
    printf("\t-i  : benchmark insert\n");
    printf("\t-p #: myhashpower, benchmark create (to measure size)\n");
    printf("\t-t #: number of threads\n");
    printf("\t-c #: coverage\n");
    printf("\t-w #: percent of writes\n");
    printf("\t-h  : show usage\n");

}

static int assign_core(int i) 
{
    //size_t n = get_cpunum();
    size_t n = 6;
    int c = 0;
    if (i < n)
        c = 2 * i;
    else if (i < 2 * n)
        c = 2 * i - 2 * n + 1;
    else if (i < 3 * n)
        c = 2 * i - 2 * n ;

    /* if (2 * i < 24) { */
    /*     c = 2 * i; */
    /* } else { */
    /*     c = 2 * i - 24 + 1; */
    /* } */
    return c;
    //return i;
}

int main(int argc, char** argv) 
{

    int bench_lookup = 0;
    int bench_insert = 0;

    //size_t num_runs = 1;
    coverage = nkeys_lookup;

    hash_query *pos_queries = NULL;
    hash_query *neg_queries = NULL;
    
    char ch;
    while ((ch = getopt(argc, argv, "ilp:c:t:d:w:hn:v")) != -1) {
        switch (ch) {
        case 'i': bench_insert = 1; break;
        case 'l': bench_lookup = 1; break;
        case 'p': myhashpower = atoi(optarg); break;
        case 't': num_threads = atoi(optarg); break;
        case 'd': duration = atof(optarg); break;
        case 'c': coverage = atoi(optarg); break;
        case 'w': writeratio = atof(optarg); break;
        case 'n': number = (int)atoi(optarg); break;
        case 'v': verbose = true; break;
        case 'h': usage(argv[0]); exit(0); break;
        default:
            usage(argv[0]);
            exit(-1);
        }
    }

    if (bench_insert + bench_lookup < 1)  {
        usage(argv[0]);
        exit(-1);
    }

    uint32_t tmp = nkeys_lookup;
    while (tmp > 0) {
        myhashpower ++;
        tmp = tmp >> 1;
    }
#ifdef TEST_CUCKOO
    myhashpower -= 2;
#endif

    print_bench_settings();
                
    if (bench_insert) {

         printf("[bench_insert]\n");

#ifdef TEST_ORIGINAL
        myhashpower = 26;
        num_queries = 1 << (myhashpower + 1);
#endif

#ifdef TEST_CUCKOO
        myhashpower = 25;
        num_queries = 1 << (myhashpower + 2);
#endif

        char insert_trace[16];
        sprintf(insert_trace,"insert_trace%d", number);
        queries = init_queries(num_queries, insert_trace);

        myhashpower = 25;
        num_queries = 1 << (myhashpower + 2);

        init(myhashpower);
        print_stats();

        /* for (size_t k = 0; k < num_queries; k ++) { */
        /*     queries[k].type = query_put; */
        /* } */

        /* duration = 1000; */

        printf("num_queries = %.2f M\n", num_queries * 1.0 / MILLION);
        printf("num_threads = %d\n", num_threads);
        printf("hashpower    = %u\n", myhashpower);

        ASSOC_PRE_BENCH();
        TIME("insert items", insert_items(num_queries, queries), num_queries);
        ASSOC_POST_BENCH();
        exit(0);
        
    } else if (bench_lookup) {
        printf("[bench_lookup]\n");

        printf("[exec %.2f M queries]\n", num_queries * 1.0 / MILLION);

        init(myhashpower);

        char pos_trace[16];
        char neg_trace[16];
        sprintf(pos_trace,"pos_trace%d", number);
        sprintf(neg_trace,"neg_trace%d", number);

        pos_queries = init_queries(nkeys_lookup, pos_trace);
        neg_queries = init_queries(nkeys_insert, neg_trace);

        print_stats();

        ASSOC_PRE_BENCH();
        TIME("insert items", insert_items(nkeys_lookup, pos_queries), nkeys_lookup);
        ASSOC_POST_BENCH();

        // create the queries with hitratio of 
        queries = malloc(sizeof(hash_query) * num_queries);
        if (!queries) {
            perror("not enough memory for queries\n");
            exit(-1);
        }
        size_t next_insert = 0;
        for (size_t k = 0; k < num_queries; k ++) {
            if (((float) rand() / (float) RAND_MAX) < writeratio) {
                queries[k] = neg_queries[next_insert++];
                queries[k].type = query_put;
            }
            else {
                // this is a get query
                queries[k] = pos_queries[rand() % (coverage)];
                queries[k].type = query_get;
            }
        }

        printf("writeratio = %.2f\n", writeratio);
        printf("coverage = %"PRIu64"\n", (uint64_t)coverage);
        printf("num_queries = %.2f M\n", num_queries * 1.0 / MILLION);
        printf("num_threads = %d\n", num_threads);
        printf("hashpower    = %u\n", myhashpower);
    }

    pthread_t threads[num_threads];
    thread_param tp[num_threads];
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    // start  benchmarking
    {
        for (int i = 0; i < num_threads; i++) {
            tp[i].tid = i;
#ifdef __linux__

            int c = assign_core(i);
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(c, &cpuset);
            pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
#endif
            int rc = pthread_create(&threads[i],  &attr, exec_queries, (void*) &(tp[i]));
            if (rc) {
                perror("error, pthread_create\n");
                exit(-1);
            }
        }
        double total_tput = 0.0;
        size_t total_puts = 0;
        for (int i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
            total_tput += tp[i].tput;
            total_puts += tp[i].puts;

            if (verbose) {
                printf("thread%d done on CPU %d: %.2f sec\n", 
                       tp[i].tid, tp[i].cpu, tp[i].time);
                printf("\thitratio = %.2f, tput = %.2f MOPS\n",
                       1.0 * tp[i].hits / tp[i].gets, (float) tp[i].tput / MILLION);
                printf("\tgets = %.2f M puts = %.2f M\n",
                       (float) tp[i].gets / MILLION, (float) tp[i].puts / MILLION);
            }
        }

        printf("tput = %.2f MOPS\n", total_tput / MILLION);
        printf("total_puts = %.2f M\n", (float) total_puts / MILLION);
        ASSOC_POST_BENCH();
        free(queries);
    }
}
