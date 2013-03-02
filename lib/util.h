#ifndef UTIL_H
#define UTIL_H


#define mutex_lock(mutex) while (pthread_mutex_trylock(mutex));

#define mutex_unlock(mutex) pthread_mutex_unlock(mutex)

#define keycmp(p1, p2) (memcmp(p1, p2, sizeof(KeyType)) == 0)

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"


#if DEBUG
#  define DBG(fmt, args...)  fprintf(stderr, ANSI_COLOR_GREEN"[libcuckoo:%s:%d] "fmt""ANSI_COLOR_RESET,__FILE__,__LINE__,args)
#else
#  define DBG(fmt, args...)  do {} while (0)
#endif


#endif
