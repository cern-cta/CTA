AC_PREREQ(2.57)
AC_INIT(xrootd-xcastor2fs, 1.0.8,[andreas.joachim.peters@cern.ch])
AC_CONFIG_SRCDIR([XrdxCastor2Fs.cc])

AM_INIT_AUTOMAKE(1.6 foreign)


AC_CANONICAL_HOST

case "$target_os" in
   tiger*)
     AC_DEFINE(__macos__)
     ;;
   darwin*)
     AC_DEFINE(__macos__)
     ;;
   apple*)
     AC_DEFINE(__macos__)
     ;;
   linux*)
     # TODO: check why two different macros are used for this
     AC_DEFINE(__linux__)
     AC_DEFINE(__linux)
     ;;
   solaris*)
     AC_DEFINE(__sun)
     CC_PREFERENCE="cc gcc"
     CXX_PREFERENCE="CC g++"
     DFLT_OPT="-O2"
     ;;
   *)
     AC_MSG_WARN([untested operating system])
   ;;
esac

echo $libdir | grep lib64 >& /dev/null
if test "$?" = "0" ; then 
   MARK64=64;
else
   MARK64="";
fi

AC_SUBST(MARK64)

AC_ARG_WITH(xrootd-location,
          [  --with-xrootd-location=installation of xrootd version to use],
          [XROOTD_LOCATION=$withval],
          [XROOTD_LOCATION=/usr/local/xroot])

AC_SUBST(XROOTD_LOCATION)

AC_ARG_WITH(castor-location,
          [  --with-castor-location=installation of castor version to use],
          [CASTOR_LOCATION=$withval],
          [CASTOR_LOCATION=/usr/])

AC_SUBST(CASTOR_LOCATION)


AC_ARG_WITH(castor-source-location,
          [  --with-castor-source-location=installation of castor source version to use],
          [CASTOR_SOURCE_LOCATION=$withval],
          [CASTOR_SOURCE_LOCATION=/usr/])

AC_SUBST(CASTOR_SOURCE_LOCATION)


AC_ARG_WITH(acl-incdir,
          [  --with-acl-incdir=installation of libacl.h file],
          [ACL_INCDIR=$withval],
          [ACL_INCDIR=/usr/include])

AC_SUBST(ACL_INCDIR)

AC_ARG_WITH(acl-libdir,
          [  --with-acl-libdir=installation of libacl.so library],
          [ACL_LIBDIR=$withval],
          [ACL_LIBDIR=/usr/lib])

AC_SUBST(ACL_LIBDIR)


AC_DEFINE(_LARGEFILE_SOURCE)
AC_DEFINE(_FILE_OFFSET_BITS,64)
AC_SYS_LARGEFILE

# Do all testing using C++
AC_LANG([C++])

# Checks for programs.
AC_PROG_CXX

dnl Disable libtool 1.5 support for languages we don't use
define([AC_LIBTOOL_LANG_F77_CONFIG], [:])dnl
define([AC_LIBTOOL_LANG_GCJ_CONFIG], [:])dnl
AC_PROG_LIBTOOL


# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([sys/time.h])

# Check for xrootd headers
if ! test -e "${XROOTD_LOCATION}/include/xrootd/XrdOlb/XrdOlbXmi.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in directory ${XROOTD_LOCATION}])
fi

# Check for castor source headers
if ! test -e "${CASTOR_SOURCE_LOCATION}/h/shift.h"
then
   AC_MSG_ERROR([castor source header files could not be found in directory ${CASTOR_SOURCE_LOCATION}])
fi 

# Check for castor headers
if ! test -e "${CASTOR_LOCATION}/include/shift.h"
then
   AC_MSG_ERROR([castor source header files could not be found in directory ${CASTOR_LOCATION}])
fi 


# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_HEADER_TIME

# Checks for library functions.
AC_FUNC_STAT
AC_CHECK_FUNCS([gettimeofday mkdir strdup])

AC_CONFIG_FILES([Makefile])
ln -s . XrdxCastor2Fs

echo "=================================================="
echo "Configuring for .../lib$MARK64 library directories"
echo "=================================================="
AC_OUTPUT
