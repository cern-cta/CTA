/*
 * $Id: osdep.h,v 1.22 2009/05/13 10:06:27 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/IP
 * All rights reserved
 */

/*
 * @(#)$RCSfile: osdep.h,v $ $Revision: 1.22 $ $Date: 2009/05/13 10:06:27 $ CERN IT-PDP/IP Frederic Hemmer
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
typedef unsigned short  U_SHORT;
typedef unsigned int    U_LONG;
typedef          char   BYTE;
typedef          short  WORD;
typedef          int    LONG;

#define BYTEADDR(x)     (((char *)&(x))+sizeof(BYTE)-BYTESIZE)
#define WORDADDR(x)     (((char *)&(x))+sizeof(WORD)-WORDSIZE)
#define LONGADDR(x)     (((char *)&(x))+sizeof(LONG)-LONGSIZE)

typedef long long		signed64;
typedef unsigned long long	u_signed64;

typedef signed64 HYPER;
typedef u_signed64 U_HYPER;
typedef U_HYPER TIME_T;

#define ONE_KB 0x400
#define ONE_MB 0x100000
#define ONE_GB 0x40000000
#define ONE_TB 0x10000000000LL
#define ONE_PB 0x4000000000000LL

/*
 * Error reporting
 */

#define NETERROR  perror
#define OSERROR   perror
 
/* Macros for externalization (UNIX) (J.-D.Durand) */
#ifdef EXTERN_C
#undef EXTERN_C
#endif
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else
#define EXTERN_C extern
#endif

#if defined(__APPLE__)
#define off64_t off_t
#endif

#endif /* _OSDEP_H_INCLUDED_ */
