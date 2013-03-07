#           -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([libcuckoo], [0.1], [fawn-dev@mailman.srv.cs.cmu.edu])
AM_INIT_AUTOMAKE([foreign])
#AC_CONFIG_SRCDIR([src/])
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_MACRO_DIR([m4])

CFLAGS="-std=c99"
CPPFLAGS="-std=c99"

# Checks for programs. 
AC_PROG_CXX
AC_PROG_CC

#AC_DISABLE_SHARED
AC_PROG_LIBTOOL

# Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_mutex_init], [], [echo "pthreads not found.  Please install pthread library before proceeding"; exit -1])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_PROG_GCC_TRADITIONAL
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC

AC_CHECK_HEADERS([getopt.h])

dnl Check if we're a little-endian or a big-endian system, needed by hash code

AC_DEFUN([AC_C_ENDIAN],
[AC_CACHE_CHECK(for endianness, ac_cv_c_endian,
[
  AC_RUN_IFELSE(
    [AC_LANG_PROGRAM([], [dnl
        long val = 1;
        char *c = (char *) &val;
        exit(*c == 1); 
    ])  
  ],[ 
    ac_cv_c_endian=big
  ],[ 
    ac_cv_c_endian=little
  ])  
])
if test $ac_cv_c_endian = big; then
  AC_DEFINE(ENDIAN_BIG, 1, [machine is bigendian])
fi
if test $ac_cv_c_endian = little; then
  AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])
fi
])

AC_C_ENDIAN


dnl Check if we have stdbool 

#AC_HEADER_STDBOOL
AH_BOTTOM([#if HAVE_STDBOOL_H
#include <stdbool.h>
#else
#define bool char
#define false 0
#define true 1
#endif ])


AC_CONFIG_FILES([ Makefile
                  src/Makefile
                  lib/Makefile
		])  

AC_OUTPUT
