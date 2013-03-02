#ifndef _CUCKOOHASH_CONFIG_H
#define _CUCKOOHASH_CONFIG_H
#include <stdint.h>

typedef uint32_t KeyType;
typedef uint32_t ValType;

/* size of bulk move in background cleaning process */
#define DEFAULT_BULK_MOVE 128


/* set DEBUG to 1 to enable debug output */
#define DEBUG 1


#endif
