/*
 * $Id: marshall.h,v 1.10 2000/08/14 14:46:50 baud Exp $
 */

/*
 * @(#)$RCSfile: marshall.h,v $ $Revision: 1.10 $ $Date: 2000/08/14 14:46:50 $ CERN IT-PDP/DM  Fabrizio Cane
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*  marshall.h   -   marshalling/unmarshalling definitions */

#ifndef _MARSHALL_H_INCLUDED_
#define _MARSHALL_H_INCLUDED_

#include <osdep.h>              /* Operating system dependencies        */
#include <memory.h>             /* memory operations definition         */

#define SHORT			WORD
/* #define U_SHORT			U_WORD */
#define SHORTSIZE		WORDSIZE
#define SHORTADDR		WORDADDR

#define marshall_WORD		marshall_SHORT
#define unmarshall_WORD		unmarshall_SHORT		

#define INC_PTR(ptr,n)		(ptr) = (char*)(ptr) + (n)
#define DIFF_PTR(ptr,base)      (char*)(ptr) - (char*)(base)

/*
 * BIT manipulation
 */

#define  BITSOFBYTE     8       /* number of bits in a byte             */

#define  bitsof(t)      sizeof(t)*BITSOFBYTE /* number of bits in a type*/

typedef  char*          bitvct; /* bit vector type definition           */

/*
 * Allocate enough memory for a 'bitvct' type variable containing
 * 'size' bits
 */

#define  bitalloc(size)		(bitvct)malloc(size/BITSOFBYTE + \
				((size%BITSOFBYTE) ? 1 : 0))

/*
 *  Set the bit 'bit-th' starting from the byte pointed to by 'ptr'
 */

#define  BIT_SET(ptr,bit)	{ char *p = (char*)(ptr) + (bit)/8; \
				  *p = *p | (1 << (7-(bit)%8)) ; \
				}

/*
 *  Clear the bit 'bit-th' starting from the byte pointed to by 'ptr'
 */

#define  BIT_CLR(ptr,bit)	{ char *p = (char*)(ptr) + (bit)/8; \
				  *p = *p & ~(1 << (7-(bit)%8)); \
				}

/*
 *  Test the bit 'bit-th' starting from the byte pointed to by 'ptr'
 */

#define  BIT_ISSET(ptr,bit)	(*(char*)((char*)(ptr)+(bit)/8) & (1 << (7-(bit)%8)))


/*
 *    B Y T E
 */

#define marshall_BYTE(ptr,n)	{ BYTE n_ = n; \
				  (void) memcpy((ptr),BYTEADDR(n_),BYTESIZE); \
				  INC_PTR(ptr,BYTESIZE); \
				} 

#define unmarshall_BYTE(ptr,n)  { BYTE n_; \
				  (void) memcpy(BYTEADDR(n_),(ptr),BYTESIZE); \
				  n = n_; \
				  INC_PTR(ptr,BYTESIZE); \
				} 

/*
 *    S H O R T
 */

#define marshall_SHORT(ptr,n)	{ SHORT n_ = htons((unsigned short)(n)); \
				  (void) memcpy((ptr),SHORTADDR(n_),SHORTSIZE); \
				  INC_PTR(ptr,SHORTSIZE); \
				}
                                                                 
#define unmarshall_SHORT(ptr,n)	{ SHORT n_ = 0;  \
				  (void) memcpy(SHORTADDR(n_),(ptr),SHORTSIZE); \
                                  n = ntohs((unsigned short)(n_)); \
				  if ( BIT_ISSET(ptr,0) && sizeof(SHORT)-SHORTSIZE > 0 ) \
					 (void) memset((char *)&n,255,sizeof(SHORT)-SHORTSIZE); \
				  INC_PTR(ptr,SHORTSIZE); \
				}

/*
 *    L O N G
 */

#define marshall_LONG(ptr,n)	{ LONG n_ = htonl((unsigned long)(n)); \
				  (void) memcpy((ptr),LONGADDR(n_),LONGSIZE); \
				  INC_PTR(ptr,LONGSIZE); \
				}
                                                                 
#define unmarshall_LONG(ptr,n)	{ LONG n_ = 0;  \
				  (void) memcpy(LONGADDR(n_),(ptr),LONGSIZE); \
                                  n = ntohl((unsigned long)(n_)); \
				  if ( BIT_ISSET(ptr,0) && sizeof(LONG)-LONGSIZE > 0 ) \
					 (void) memset((char *)&n,255,sizeof(LONG)-LONGSIZE); \
				  INC_PTR(ptr,LONGSIZE); \
				}

/*
 *    S T R I N G
 */

#define  marshall_STRING(ptr,str)       {  (void) strcpy((char*)(ptr),(char*)(str)); \
					   INC_PTR(ptr,strlen(str)+1); \
					}

#define  unmarshall_STRING(ptr,str)     { (void) strcpy((char*)(str),(char*)(ptr)); \
					  INC_PTR(ptr,strlen(str)+1); \
					}

/*
 *    H Y P E R   ( 6 4   B I T S )
 */

#if !defined(__alpha) && !defined(i386) && !defined(_WIN32) && !defined(__ia64__)
#define  marshall_HYPER(ptr,n)          { LONG n_ = htonl(*((unsigned long *)&(n))); \
					  (void) memcpy((ptr),LONGADDR(n_),LONGSIZE); \
					  INC_PTR(ptr,LONGSIZE); \
					  n_ = htonl(*((unsigned long *)((char *)&(n)+LONGSIZE))); \
					  (void) memcpy((ptr),LONGADDR(n_),LONGSIZE); \
					  INC_PTR(ptr,LONGSIZE); \
					}

#define  unmarshall_HYPER(ptr,n)        { LONG n_ = 0;  \
					  (void) memcpy(LONGADDR(n_),(ptr),LONGSIZE); \
					  *((LONG *)&(n)) = ntohl((unsigned long)(n_)); \
					  INC_PTR(ptr,LONGSIZE); \
					  n_ = 0;  \
					  (void) memcpy(LONGADDR(n_),(ptr),LONGSIZE); \
					  *((LONG *)((char *)&(n)+LONGSIZE)) = \
						ntohl((unsigned long)(n_)); \
					  INC_PTR(ptr,LONGSIZE); \
					}
#else
#define  marshall_HYPER(ptr,n)          { LONG n_ = htonl(*((U_LONG *)((char *)&(n)+LONGSIZE))); \
					  (void) memcpy((ptr),LONGADDR(n_),LONGSIZE); \
					  INC_PTR(ptr,LONGSIZE); \
					  n_ = htonl(*((U_LONG *)&(n))); \
					  (void) memcpy((ptr),LONGADDR(n_),LONGSIZE); \
					  INC_PTR(ptr,LONGSIZE); \
					}

#define  unmarshall_HYPER(ptr,n)        { LONG n_ = 0;  \
					  (void) memcpy(LONGADDR(n_),(ptr),LONGSIZE); \
					  *((LONG *)((char *)&(n)+LONGSIZE)) = \
						ntohl((U_LONG)(n_)); \
					  INC_PTR(ptr,LONGSIZE); \
					  n_ = 0;  \
					  (void) memcpy(LONGADDR(n_),(ptr),LONGSIZE); \
					  *((LONG *)&(n)) = ntohl((U_LONG)(n_)); \
					  INC_PTR(ptr,LONGSIZE); \
					}
#endif

/*
 *    O P A Q U E
 */

#define marshall_OPAQUE(ptr,raw,n)	{ (void) memcpy((ptr),(raw),(n)); \
					  INC_PTR(ptr,(n)); \
					}

#define unmarshall_OPAQUE(ptr,raw,n)	{ (void) memcpy((raw),(ptr),(n)); \
					  INC_PTR(ptr,(n)); \
					}

/*
 *    T I M E
 */

#define  marshall_TIME_T(ptr,n)       {  \
					   TIME_T _marshall_time_dummy = (TIME_T) n; \
					   marshall_HYPER(ptr,_marshall_time_dummy); \
					}

#define  unmarshall_TIME_T(ptr,n)     { \
					   TIME_T _unmarshall_time_dummy; \
					   unmarshall_HYPER(ptr,_unmarshall_time_dummy); \
					   n = (time_t) _unmarshall_time_dummy; \
					}

#endif /* _MARSHALL_H_INCLUDED_ */
