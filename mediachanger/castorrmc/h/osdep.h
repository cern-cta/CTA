/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

/* osdep.h      Operating system dependencies                           */

#pragma once

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

#define ONE_KIB 0x400
#define ONE_MIB 0x100000
#define ONE_GIB 0x40000000
#define ONE_TIB 0x10000000000LL
#define ONE_PIB 0x4000000000000LL
#define ONE_HIB 0x1000000000000000LL

#define ONE_KB 1000
#define ONE_MB 1000000
#define ONE_GB 1000000000
#define ONE_TB 1000000000000LL
#define ONE_PB 1000000000000000LL
#define ONE_HB 1000000000000000000LL

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
