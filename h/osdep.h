/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/IP
 * All rights reserved
 */

/*
 * @(#)$RCSfile: osdep.h,v $ $Revision: 1.4 $ $Date: 1999/10/13 14:10:33 $ CERN IT-PDP/IP Frederic Hemmer
 */

/* osdep.h      Operating system dependencies                           */

#ifndef _OSDEP_H_INCLUDED_
#define  _OSDEP_H_INCLUDED_

/*
 * Data representation
 */

#define BYTESIZE        1
#define WORDSIZE        2
#define LONGSIZE        4
#define QUADSIZE        8
#define HYPERSIZE       8
 
typedef unsigned char   U_BYTE;
typedef unsigned short  U_WORD;
typedef unsigned int    U_LONG;
typedef struct  {
        U_LONG      lslw;
        U_LONG      mslw;
} U_QUAD;
#if !defined(_WINDEF_)
typedef          char   BYTE;
typedef          short  WORD;
typedef          int    LONG;
#endif
typedef struct  {
        U_LONG    lslw;
        LONG      mslw;
} QUAD;
 
#define BYTEADDR(x)     (((char *)&(x))+sizeof(BYTE)-BYTESIZE)
#define WORDADDR(x)     (((char *)&(x))+sizeof(WORD)-WORDSIZE)
#define LONGADDR(x)     (((char *)&(x))+sizeof(LONG)-LONGSIZE)
#define QUADADDR(x)     (((char *)&(x))+sizeof(QUAD)-QUADSIZE)

#ifndef _WIN32
typedef long long		signed64;
typedef unsigned long long	u_signed64;
#else
typedef __int64			signed64;
typedef unsigned __int64	u_signed64;
typedef long gid_t;
typedef long uid_t;
typedef int mode_t;
#endif

typedef signed64 HYPER;
typedef u_signed64 U_HYPER;

#

/*
 * Byte swapping
 */

/*
 * Exit codes
 */

#ifdef unix
#define BADEXIT         1
#define GOODEXIT        0
#endif /* unix */

/*
 * Error reporting
 */

#define NETERROR  perror
#define OSERROR   perror
 
/* Macros for prototyping */
#ifdef _PROTO
#undef _PROTO
#endif
#ifdef __STDC__
#define _PROTO(a) a
#else
#define _PROTO(a) ()
#endif

/* Macros for externalization (UNIX) (J.-D.Durand) */
#ifdef EXTERN_C
#undef EXTERN_C
#endif
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else
#define EXTERN_C extern
#endif

/* Macros for externalization (WIN32) (O.Barring)     */
/* A correct exernalization of routines that          */
/* _always_ returns an interger, in your              */
/* <package>_api.h header file, is then:              */
/* EXTERN_C <type> DLL_DECL routine _PROTO((...));    */
/* [Please note the space before prototype itself]    */
/* [Please note the two level parenthesis]            */
/* If your externalized function do not return int    */
/* but another type, you must get inspired by the     */
/* following declaration of int and change       */
#ifdef DLL_DECL
#undef DLL_DECL
#endif
#if !(defined(_WIN32) && defined(_DLL))
#define DLL_DECL
#else
#ifdef _EXPORTING
#define DLL_DECL __declspec(dllexport) __cdecl
#else
#define DLL_DECL __declspec(dllimport) __cdecl
#endif
#endif

#endif /* _OSDEP_H_INCLUDED_ */
