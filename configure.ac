#           -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([libcuckoo], [0.1], [fawn-dev@mailman.srv.cs.cmu.edu])
AM_INIT_AUTOMAKE([foreign])
#AC_CONFIG_SRCDIR([src/])
AC_CONFIG_HEADERS([config.h])


# Checks for programs. 
AC_PROG_CXX
AC_PROG_CC


# Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_mutex_init], [], [echo "pthreads not found.  Please install pthread library before proceeding"; exit -1])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_PROG_GCC_TRADITIONAL
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_TYPE_SIGNAL
AC_CHECK_FUNCS([bzero ftruncate gethostbyname getpagesize gettimeofday inet_ntoa memchr memmove memset munmap select socket strdup strerror strtol strtoul strtoull])


AC_CONFIG_FILES([ Makefile
          src/Makefile
        ])  

AC_OUTPUT
