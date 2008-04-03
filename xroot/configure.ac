AC_INIT([xrootd Castor2], [20070802cvs_8_v2.1.7.1])
AC_PREREQ(2.53)
AC_CONFIG_SRCDIR([XrdCS2Xmi.hh])

AC_CANONICAL_HOST
AM_INIT_AUTOMAKE([1.6.3 subdir-objects])

AC_DEFINE(_LARGEFILE_SOURCE)
AC_DEFINE(_FILE_OFFSET_BITS,64)
AC_SYS_LARGEFILE

AC_ARG_WITH(xrootd-location,
	  [  --with-xrootd-location=installation of xrootd version to use],
	  [XROOTD_LOCATION=$withval],
	  [XROOTD_LOCATION=/usr/local/xroot])

AC_ARG_WITH(castor-inc,
	  [  --with-castor-inc=PATH   include files for castor],
          [CASTOR_INCDIR=$withval],
	  [CASTOR_INCDIR=/usr/local/xroot/CASTOR2])

AC_ARG_WITH(castor-lib,
	  [  --with-castor-lib=PATH   shared libraries for castor],
          [CASTOR_LIBDIR=$withval],
          [CASTOR_LIBDIR=/usr/lib/])

if ! test -e "$CASTOR_INCDIR/castor/client/BaseClient.hpp"  
then
   AC_MSG_ERROR([Castor header files could not be found under $CASTOR_INCDIR])
fi

if ! test -e "$CASTOR_LIBDIR/libshift.so"
then 
   AC_MSG_ERROR([Castor library libshift.so could not be found under $CASTOR_LIBDIR])
fi     

if ! test -e "${XROOTD_LOCATION}/include/xrootd/XrdOlb/XrdOlbXmi.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in prefix directory ${prefix}])
fi

AC_SUBST(CASTOR_INCDIR)
AC_SUBST(CASTOR_LIBDIR)
AC_SUBST(XROOTD_LOCATION)

# Checks for programs.
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_LIBTOOL

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

