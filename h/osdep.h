/*
 * $Id: osdep.h,v 1.13 2000/12/12 13:04:46 jdurand Exp $
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/IP
 * All rights reserved
 */

/*
 * @(#)$RCSfile: osdep.h,v $ $Revision: 1.13 $ $Date: 2000/12/12 13:04:46 $ CERN IT-PDP/IP Frederic Hemmer
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
#define TIME_TSIZE      HYPERSIZE

typedef unsigned char   U_BYTE;
/* typedef unsigned short  U_WORD; */
typedef unsigned short  U_SHORT;
typedef unsigned int    U_LONG;
typedef struct  {
        U_LONG      lslw;
        U_LONG      mslw;
} U_QUAD;
#ifndef _WINDEF_
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
typedef U_HYPER TIME_T;

#define ONE_KB 0x400
#define ONE_MB 0x100000
#define ONE_GB 0x40000000
#ifndef _WIN32
#define ONE_TB 0x10000000000LL
#define ONE_PB 0x4000000000000LL
#else
#define ONE_TB 0x10000000000
#define ONE_PB 0x4000000000000
#endif

/*
 * Error reporting
 */

#define NETERROR  perror
#define OSERROR   perror
 
/* Macros for prototyping */
#ifdef _PROTO
#undef _PROTO
#endif
#if (defined(__STDC__) || defined(_WIN32))
/* On Win32, compiler is STDC compliant but the */
/* __STDC__ definition itself is not a default. */
#define CONST const
#define _PROTO(a) a
#else
#define CONST
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
#if defined(DLL_DECL)
#undef DLL_DECL
#endif
#if !defined(_WIN32)
#define DLL_DECL
#else
#if defined(_EXPORTING) && defined(_DLL)
#define DLL_DECL __declspec(dllexport) 
#elif defined(_IMPORTING)
#define DLL_DECL __declspec(dllimport)
#else
#define DLL_DECL
#endif
#endif

#endif /* _OSDEP_H_INCLUDED_ */
