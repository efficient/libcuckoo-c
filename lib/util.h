#ifndef UTIL_H
#define UTIL_H


#define mutex_lock(mutex) while (pthread_mutex_trylock(mutex));

#define  mutex_unlock(mutex) pthread_mutex_unlock(mutex)

#define keycmp(p1, p2) (memcmp(p1, p2, sizeof(KeyType)) == 0)


#endif
