/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/IP
 * All rights reserved
 */

/*
 * %W% %G% CERN IT-PDP/IP Frederic Hemmer
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
#endif

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
 
#endif /* _OSDEP_H_INCLUDED_ */
