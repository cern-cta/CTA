/*
 * $Id: Cuuid.c,v 1.11 2007/02/13 07:52:24 waldron Exp $
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * Based on: http://www.webdav.org/specs/draft-leach-uuids-guids-01.txt
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cuuid.c,v $ $Revision: 1.11 $ $Date: 2007/02/13 07:52:24 $ CERN IT-ADC/CA Jean-Damien Durand";
#endif /* not lint */

/*
 * Cuuid.c - CASTOR MT-safe uuid routines.
 */ 

#include <errno.h>
#include <string.h>
#ifdef _WIN32
#include <time.h>
#else
#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#endif
#include "serrno.h"
#include "osdep.h"
#include "Cuuid.h"
#include "marshall.h"
#include "Cglobals.h"
#include "Csnprintf.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <sys/sysinfo.h>
#endif
#if (defined(aix) || defined(hpux))
#ifdef KMEM
#undef KMEM
#endif
#define KMEM "/dev/kmem"
#ifdef aix
#include <sys/nlist.h>
#else
#include <nlist.h>
static char *hp_unix = "/stand/vmunix";
#endif
#ifdef aix
struct nlist nl[] = {
	{"sysinfo"},
	{ "" } /* null name marks end of namelist */
};
#else
struct nlist nl[] = {
	{"sysinfo",0,0,0,0,0},
	{"",0,0,0,0,0}
};
#endif
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif

#define UUIDS_PER_TICK 1024
/* Set the following to a call to acquire a system wide global lock */
#define LOCK
#define UNLOCK

/**
 * Declaration of a usefull cuuid : the null one.
 * No need to do a memset, it will be set to 0 by default
 */
Cuuid_t nullCuuid;

/* Private MD5 */
/*
 ***********************************************************************
 ** md5.h -- header file for implementation of MD5                    **
 ** RSA Data Security, Inc. MD5 Message-Digest Algorithm              **
 ** Created: 2/17/90 RLR                                              **
 ** Revised: 12/27/90 SRD,AJ,BSK,JT Reference C version               **
 ** Revised (for MD5): RLR 4/27/91                                    **
 **   -- G modified to have y&~z instead of y&z                       **
 **   -- FF, GG, HH modified to add in last register done             **
 **   -- Access pattern: round 2 works mod 5, round 3 works mod 3     **
 **   -- distinct additive constant for each step                     **
 **   -- round 4 added, working mod 7                                 **
 ***********************************************************************
 */

/*
 ***********************************************************************
 ** Copyright (C) 1990, RSA Data Security, Inc. All rights reserved.  **
 **                                                                   **
 ** License to copy and use this software is granted provided that    **
 ** it is identified as the "RSA Data Security, Inc. MD5 Message-     **
 ** Digest Algorithm" in all material mentioning or referencing this  **
 ** software or this function.                                        **
 **                                                                   **
 ** License is also granted to make and use derivative works          **
 ** provided that such works are identified as "derived from the RSA  **
 ** Data Security, Inc. MD5 Message-Digest Algorithm" in all          **
 ** material mentioning or referencing the derived work.              **
 **                                                                   **
 ** RSA Data Security, Inc. makes no representations concerning       **
 ** either the merchantability of this software or the suitability    **
 ** of this software for any particular purpose.  It is provided "as  **
 ** is" without express or implied warranty of any kind.              **
 **                                                                   **
 ** These notices must be retained in any copies of any part of this  **
 ** documentation and/or software.                                    **
 ***********************************************************************
 */

/* typedef a 32-bit type */
typedef U_LONG UINT4;

/* Data structure for MD5 (Message-Digest) computation */
typedef struct {
  UINT4 i[2];                   /* number of _bits_ handled mod 2^64 */
  UINT4 buf[4];                                    /* scratch buffer */
  unsigned char in[64];                              /* input buffer */
  unsigned char digest[16];     /* actual digest after MD5Final call */
  char hex[33];       /* hexadecimal form + 1 for the null character */
} MD5_CTX;
/* forward declaration */

static unsigned char PADDING[64] = {
  0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

/* F, G, H and I are basic MD5 functions */
#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

/* ROTATE_LEFT rotates x left n bits */
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4 */
/* Rotation is separate from addition to prevent recomputation */
#define FF(a, b, c, d, x, s, ac) \
  {(a) += F ((b), (c), (d)) + (x) + (UINT4)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define GG(a, b, c, d, x, s, ac) \
  {(a) += G ((b), (c), (d)) + (x) + (UINT4)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define HH(a, b, c, d, x, s, ac) \
  {(a) += H ((b), (c), (d)) + (x) + (UINT4)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define II(a, b, c, d, x, s, ac) \
  {(a) += I ((b), (c), (d)) + (x) + (UINT4)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }

typedef u_signed64 Cuuid_time_t;
typedef struct {
    char nodeID[6];
} Cuuid_node_t;

/* data type for UUID generator persistent state */
typedef struct {
    Cuuid_time_t ts;       /* saved timestamp */
    Cuuid_node_t node;     /* saved node ID */
    U_SHORT cs;            /* saved clock sequence */
} Cuuid_state_t;


/* Static variables (Cglobals way of working) */
/* Thread-safe state */
static int _Cuuid_st_key = -1;
#define _Cuuid_st (*C_Cuuid_st())
static Cuuid_state_t _Cuuid_st_static;

#if (defined(hpux) || defined(sgi))
#ifdef _PROTO
#undef _PROTO
#endif
#define _PROTO(a) ()
#endif

/* Static MD5 prototypes */
static int     _Cuuid_MD5Init                  _PROTO((MD5_CTX *));
static int     _Cuuid_MD5Update                _PROTO((MD5_CTX *,
						       unsigned char *,
						       unsigned int));
static int     _Cuuid_MD5Final                 _PROTO((MD5_CTX *));
static void    _Cuuid_Transform                _PROTO((UINT4 *,
						       UINT4 *));

/* Static _Cuuid prototypes */
static void    _Cuuid_read_state               _PROTO((U_SHORT *,
						       Cuuid_time_t *,
						       Cuuid_node_t *));
static void    _Cuuid_write_state              _PROTO(());
static void    _Cuuid_format_uuid_v1           _PROTO(());
static void    _Cuuid_format_uuid_v3           _PROTO((Cuuid_t *,
						       unsigned char[16]));
static void    _Cuuid_get_current_time         _PROTO((Cuuid_time_t *));
static U_SHORT _Cuuid_true_random              _PROTO(());
static void    _Cuuid_get_ieee_node_identifier _PROTO((Cuuid_node_t *));
static void    _Cuuid_get_system_time          _PROTO((Cuuid_time_t *));
static void    _Cuuid_get_random_info          _PROTO((char[16]));
static Cuuid_state_t *C_Cuuid_st _PROTO(());

/* --------------------------- */
/* Thread-safe _Cuuid_state st */
/* --------------------------- */
static Cuuid_state_t *C_Cuuid_st()
{
	Cuuid_state_t *var;

	/* Call Cglobals_get */
	Cglobals_get(&_Cuuid_st_key,
				 (void **) &var,
				 sizeof(Cuuid_state_t)
		);
	/* If error, var will be NULL */
	if (var == NULL) {
		return(&_Cuuid_st_static);
	}
	return(var);
}

/* ------------------------------- */
/* Cuuid_create -- generate a UUID */
/* ------------------------------- */
void DLL_DECL Cuuid_create(uuid)
	Cuuid_t *uuid;
{
    Cuuid_time_t timestamp, last_time;
    U_SHORT clockseq;
    Cuuid_node_t node;
    Cuuid_node_t last_node;
	
    /* acquire system wide lock so we're alone */
    LOCK;
	
    /* get current time */
    _Cuuid_get_current_time(&timestamp);
	
    /* get node ID */
    _Cuuid_get_ieee_node_identifier(&node);
	
    /* get saved state from NV storage */
    _Cuuid_read_state(&clockseq, &last_time, &last_node);
	
    /* if no NV state, or if clock went backwards, or node ID changed
       (e.g., net card swap) change clockseq */
    if (memcmp(&node, &last_node, sizeof(Cuuid_node_t)))
        clockseq = _Cuuid_true_random();
    else if (timestamp < last_time)
        clockseq++;
	
    /* stuff fields into the UUID */
    _Cuuid_format_uuid_v1(uuid, clockseq, timestamp, node);
	
    /* save the state for next time */
    _Cuuid_write_state(clockseq, timestamp, node);
	
    UNLOCK;
}

/* ----------------------------------------------------- */
/* make a UUID from the timestamp, clockseq, and node ID */
/* ----------------------------------------------------- */
static void _Cuuid_format_uuid_v1(uuid,clock_seq,timestamp,node)
	Cuuid_t *uuid;
	U_SHORT clock_seq;
	Cuuid_time_t timestamp;
	Cuuid_node_t node;
{
	/* Construct a version 1 uuid with the information we've gathered
	 * plus a few constants. */
    uuid->time_low = (unsigned long)(timestamp & 0xFFFFFFFF);
	uuid->time_mid = (unsigned short)((timestamp >> 32) & 0xFFFF);
	uuid->time_hi_and_version = (unsigned short)((timestamp >> 48) &
												 0x0FFF);
	uuid->time_hi_and_version |= (1 << 12);
	uuid->clock_seq_low = clock_seq & 0xFF;
	uuid->clock_seq_hi_and_reserved = (clock_seq & 0x3F00) >> 8;
	uuid->clock_seq_hi_and_reserved |= 0x80;
	memcpy(uuid->node, &node, sizeof uuid->node);
}

/* ------------------------- */
/* read UUID generator state */
/* ------------------------- */
static void _Cuuid_read_state(clockseq,timestamp,node)
	U_SHORT *clockseq;
	Cuuid_time_t *timestamp;
	Cuuid_node_t *node;
{
	Cuuid_state_t st = _Cuuid_st;
	
    *clockseq = st.cs;
    *timestamp = st.ts;
    *node = st.node;
}

/* ------------------------- */
/* save UUID generator state */
/* ------------------------- */
static void _Cuuid_write_state(clockseq,timestamp,node)
	U_SHORT clockseq;
	Cuuid_time_t timestamp;
	Cuuid_node_t node;
{
	Cuuid_state_t st = _Cuuid_st;
	
    st.cs = clockseq;
    st.ts = timestamp;
    st.node = node;
}

/* ----------------------------------------------------- */
/* get time as 60 bit 100ns ticks since whenever.        */
/* Compensate for the fact that real clock resolution is */
/* less than 100ns.                                      */
/* ----------------------------------------------------- */
static void _Cuuid_get_current_time(timestamp)
	Cuuid_time_t *timestamp;
{
	*timestamp = (Cuuid_time_t) time(NULL);
}

/* ------------------------------------------------------ */
/* true_random -- generate a crypto-quality random number */
/* This sample doesn't do that.                           */
/* ------------------------------------------------------ */
static U_SHORT _Cuuid_true_random()
{
    Cuuid_time_t time_now;
	
	_Cuuid_get_system_time(&time_now);
	srand((unsigned int) (((time_now >> 32) ^ time_now)&0xffffffff));
	return ((U_SHORT) rand());
}

/* ------------------------------------------------------------------ */
/* Cuuid_create_from_name -- create a UUID using a name from a name   */
/* space                                                              */
/* ------------------------------------------------------------------ */
void DLL_DECL Cuuid_create_from_name(uuid,nsid,name)
	Cuuid_t *uuid;
	Cuuid_t  nsid;
	char *name;
{
    MD5_CTX c;
    Cuuid_t net_nsid;      /* context UUID in network byte order */
	unsigned int namelen = strlen(name);

    /* put name space ID in network byte order so it hashes the same
	   no matter what endian machine we're on */
    net_nsid = nsid;
    htonl(net_nsid.time_low);
    htons(net_nsid.time_mid);
    htons(net_nsid.time_hi_and_version);
	
    _Cuuid_MD5Init(&c);
    _Cuuid_MD5Update(&c, (unsigned char *) &net_nsid,
                     (unsigned int) sizeof(Cuuid_t));
    _Cuuid_MD5Update(&c, (unsigned char *) name, (unsigned int) namelen);
    _Cuuid_MD5Final(&c);
	
    /* the hash is in network byte order at this point */
    _Cuuid_format_uuid_v3(uuid, c.digest);
}

/* ------------------------------------------------------------------ */
/* format_uuid_v3 -- make a UUID from a (pseudo)random 128 bit number */
/* ------------------------------------------------------------------ */
static void _Cuuid_format_uuid_v3(uuid, hash)
	Cuuid_t *uuid;
	unsigned char hash[16];
{
	/* Construct a version 3 uuid with the (pseudo-)random number
	 * plus a few constants. */
	
	memcpy(uuid, hash, sizeof(Cuuid_t));
	
    /* convert UUID to local byte order */
    ntohl(uuid->time_low);
    ntohs(uuid->time_mid);
    ntohs(uuid->time_hi_and_version);
	
    /* put in the variant and version bits */
	uuid->time_hi_and_version &= 0x0FFF;
	uuid->time_hi_and_version |= (3 << 12);
	uuid->clock_seq_hi_and_reserved &= 0x3F;
	uuid->clock_seq_hi_and_reserved |= 0x80;
}

/* ---------------------------------------------------------- */
/* uuid_compare --  Compare two UUID's "lexically" and return */
/* -1   u1 is lexically before u2                             */
/*  0  u1 is equal to u2                                      */
/*  1   u1 is lexically after u2                              */
/* ---------------------------------------------------------- */
/*   
	 Note:   lexical ordering is not temporal ordering!
*/
int DLL_DECL Cuuid_compare(u1,u2)
	Cuuid_t *u1;
	Cuuid_t *u2;
{
    int i;
	
#define CHECK(f1, f2) if (f1 != f2) return f1 < f2 ? -1 : 1;
    CHECK(u1->time_low, u2->time_low);
    CHECK(u1->time_mid, u2->time_mid);
    CHECK(u1->time_hi_and_version, u2->time_hi_and_version);
    CHECK(u1->clock_seq_hi_and_reserved, u2->clock_seq_hi_and_reserved);
    CHECK(u1->clock_seq_low, u2->clock_seq_low)
		for (i = 0; i < 6; i++) {
			if (u1->node[i] < u2->node[i])
				return -1;
			if (u1->node[i] > u2->node[i])
				return 1;
		}
    return 0;
}

/* --------------- */
/* _marshall_UUID  */
/* --------------- */
void DLL_DECL _marshall_UUID (ptr, uuid)
     char **ptr;
     Cuuid_t *uuid;
{
  int i;
  marshall_LONG(*ptr, uuid->time_low);
  marshall_SHORT(*ptr, uuid->time_mid);
  marshall_SHORT(*ptr, uuid->time_hi_and_version);
  marshall_BYTE(*ptr, uuid->clock_seq_hi_and_reserved);
  marshall_BYTE(*ptr, uuid->clock_seq_low);
  for (i=0; i< 6; i++) marshall_BYTE(*ptr, uuid->node[i]);
}

/* ---------------- */
/* _unmarshall_UUID */
/* ---------------- */
void DLL_DECL _unmarshall_UUID (ptr, uuid)
     char **ptr;
     Cuuid_t *uuid;
{
  int i;
  unmarshall_LONG(*ptr, uuid->time_low);
  unmarshall_SHORT(*ptr, uuid->time_mid);
  unmarshall_SHORT(*ptr, uuid->time_hi_and_version);
  unmarshall_BYTE(*ptr, uuid->clock_seq_hi_and_reserved);
  unmarshall_BYTE(*ptr, uuid->clock_seq_low);
  for (i=0; i< 6; i++) unmarshall_BYTE(*ptr, uuid->node[i]);
}


/* The routine MD5Init initializes the message-digest context
   mdContext. All fields are set to zero.
 */
static int _Cuuid_MD5Init(mdContext)
	MD5_CTX *mdContext;
{
	mdContext->i[0] = mdContext->i[1] = (UINT4)0;
	
	/* Load magic initialization constants.
	 */
	mdContext->buf[0] = (UINT4)0x67452301;
	mdContext->buf[1] = (UINT4)0xefcdab89;
	mdContext->buf[2] = (UINT4)0x98badcfe;
	mdContext->buf[3] = (UINT4)0x10325476;
	
	return(0);
}

/* The routine MD5Update updates the message-digest context to
   account for the presence of each of the characters inBuf[0..inLen-1]
   in the message whose digest is being computed.
*/
static int _Cuuid_MD5Update(mdContext,inBuf,inLen)
	MD5_CTX *mdContext;
	unsigned char *inBuf;
	unsigned int inLen;
{
	UINT4 in[16];
	int mdi;
	unsigned int i, ii;
	
	/* compute number of bytes mod 64 */
	mdi = (int)((mdContext->i[0] >> 3) & 0x3F);
	
	/* update number of bits */
	if ((mdContext->i[0] + ((UINT4)inLen << 3)) < mdContext->i[0])
		mdContext->i[1]++;
	mdContext->i[0] += ((UINT4)inLen << 3);
	mdContext->i[1] += ((UINT4)inLen >> 29);
	
	while (inLen--) {
		/* add new character to buffer, increment mdi */
		mdContext->in[mdi++] = *inBuf++;
		
		/* transform if necessary */
		if (mdi == 0x40) {
			for (i = 0, ii = 0; i < 16; i++, ii += 4)
				in[i] = (((UINT4)mdContext->in[ii+3]) << 24) |
					(((UINT4)mdContext->in[ii+2]) << 16) |
					(((UINT4)mdContext->in[ii+1]) << 8) |
					((UINT4)mdContext->in[ii]);
			_Cuuid_Transform (mdContext->buf, in);
			mdi = 0;
		}
	}
	return(0);
}

/* The routine MD5Final terminates the message-digest computation and
   ends with the desired message digest in mdContext->digest[0...15].
*/
static int _Cuuid_MD5Final(mdContext)
	MD5_CTX *mdContext;
{
	UINT4 in[16];
	int mdi;
	unsigned int i, ii;
	unsigned int padLen;
	char *d;
	static char *hexdigits = "0123456789abcdef";
	
	/* save number of bits */
	in[14] = mdContext->i[0];
	in[15] = mdContext->i[1];
	
	/* compute number of bytes mod 64 */
	mdi = (int)((mdContext->i[0] >> 3) & 0x3F);
	
	/* pad out to 56 mod 64 */
	padLen = (mdi < 56) ? (56 - mdi) : (120 - mdi);
	_Cuuid_MD5Update (mdContext, PADDING, padLen);
	
	/* append length in bits and transform */
	for (i = 0, ii = 0; i < 14; i++, ii += 4)
		in[i] = (((UINT4)mdContext->in[ii+3]) << 24) |
			(((UINT4)mdContext->in[ii+2]) << 16) |
			(((UINT4)mdContext->in[ii+1]) << 8) |
			((UINT4)mdContext->in[ii]);
	_Cuuid_Transform (mdContext->buf, in);
	
	/* store buffer in digest */
	for (i = 0, ii = 0; i < 4; i++, ii += 4) {
		mdContext->digest[ii] = (unsigned char)(mdContext->buf[i] & 0xFF);
		mdContext->digest[ii+1] =
			(unsigned char)((mdContext->buf[i] >> 8) & 0xFF);
		mdContext->digest[ii+2] =
			(unsigned char)((mdContext->buf[i] >> 16) & 0xFF);
		mdContext->digest[ii+3] =
			(unsigned char)((mdContext->buf[i] >> 24) & 0xFF);
	}
	
	/* Kept from MD5.xs,v 1.24 1999/07/28 10:38:50 gisle Exp */
	/*           perl5 package.                              */
	d = &(mdContext->hex[0]);
	
	for (i = 0; i < 16; i++) {
		*d++ = hexdigits[(mdContext->digest[i] >> 4)];
		*d++ = hexdigits[(mdContext->digest[i] & 0x0F)];
	}
	*d = '\0';

	return(0);
}

/* Basic MD5 step. Transforms buf based on in.
 */
static void _Cuuid_Transform(buf,in)
	UINT4 *buf;
	UINT4 *in;
{
	UINT4 a = buf[0], b = buf[1], c = buf[2], d = buf[3];
	
	/* Round 1 */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
	FF ( a, b, c, d, in[ 0], S11, 3614090360UL); /* 1 */
	FF ( d, a, b, c, in[ 1], S12, 3905402710UL); /* 2 */
	FF ( c, d, a, b, in[ 2], S13,  606105819UL); /* 3 */
	FF ( b, c, d, a, in[ 3], S14, 3250441966UL); /* 4 */
	FF ( a, b, c, d, in[ 4], S11, 4118548399UL); /* 5 */
	FF ( d, a, b, c, in[ 5], S12, 1200080426UL); /* 6 */
	FF ( c, d, a, b, in[ 6], S13, 2821735955UL); /* 7 */
	FF ( b, c, d, a, in[ 7], S14, 4249261313UL); /* 8 */
	FF ( a, b, c, d, in[ 8], S11, 1770035416UL); /* 9 */
	FF ( d, a, b, c, in[ 9], S12, 2336552879UL); /* 10 */
	FF ( c, d, a, b, in[10], S13, 4294925233UL); /* 11 */
	FF ( b, c, d, a, in[11], S14, 2304563134UL); /* 12 */
	FF ( a, b, c, d, in[12], S11, 1804603682UL); /* 13 */
	FF ( d, a, b, c, in[13], S12, 4254626195UL); /* 14 */
	FF ( c, d, a, b, in[14], S13, 2792965006UL); /* 15 */
	FF ( b, c, d, a, in[15], S14, 1236535329UL); /* 16 */
	
	/* Round 2 */
#define S21 5
#define S22 9
#define S23 14
#define S24 20
	GG ( a, b, c, d, in[ 1], S21, 4129170786UL); /* 17 */
	GG ( d, a, b, c, in[ 6], S22, 3225465664UL); /* 18 */
	GG ( c, d, a, b, in[11], S23,  643717713UL); /* 19 */
	GG ( b, c, d, a, in[ 0], S24, 3921069994UL); /* 20 */
	GG ( a, b, c, d, in[ 5], S21, 3593408605UL); /* 21 */
	GG ( d, a, b, c, in[10], S22,   38016083UL); /* 22 */
	GG ( c, d, a, b, in[15], S23, 3634488961UL); /* 23 */
	GG ( b, c, d, a, in[ 4], S24, 3889429448UL); /* 24 */
	GG ( a, b, c, d, in[ 9], S21,  568446438UL); /* 25 */
	GG ( d, a, b, c, in[14], S22, 3275163606UL); /* 26 */
	GG ( c, d, a, b, in[ 3], S23, 4107603335UL); /* 27 */
	GG ( b, c, d, a, in[ 8], S24, 1163531501UL); /* 28 */
	GG ( a, b, c, d, in[13], S21, 2850285829UL); /* 29 */
	GG ( d, a, b, c, in[ 2], S22, 4243563512UL); /* 30 */
	GG ( c, d, a, b, in[ 7], S23, 1735328473UL); /* 31 */
	GG ( b, c, d, a, in[12], S24, 2368359562UL); /* 32 */
	
	/* Round 3 */
#define S31 4
#define S32 11
#define S33 16
#define S34 23
	HH ( a, b, c, d, in[ 5], S31, 4294588738UL); /* 33 */
	HH ( d, a, b, c, in[ 8], S32, 2272392833UL); /* 34 */
	HH ( c, d, a, b, in[11], S33, 1839030562UL); /* 35 */
	HH ( b, c, d, a, in[14], S34, 4259657740UL); /* 36 */
	HH ( a, b, c, d, in[ 1], S31, 2763975236UL); /* 37 */
	HH ( d, a, b, c, in[ 4], S32, 1272893353UL); /* 38 */
	HH ( c, d, a, b, in[ 7], S33, 4139469664UL); /* 39 */
	HH ( b, c, d, a, in[10], S34, 3200236656UL); /* 40 */
	HH ( a, b, c, d, in[13], S31,  681279174UL); /* 41 */
	HH ( d, a, b, c, in[ 0], S32, 3936430074UL); /* 42 */
	HH ( c, d, a, b, in[ 3], S33, 3572445317UL); /* 43 */
	HH ( b, c, d, a, in[ 6], S34,   76029189UL); /* 44 */
	HH ( a, b, c, d, in[ 9], S31, 3654602809UL); /* 45 */
	HH ( d, a, b, c, in[12], S32, 3873151461UL); /* 46 */
	HH ( c, d, a, b, in[15], S33,  530742520UL); /* 47 */
	HH ( b, c, d, a, in[ 2], S34, 3299628645UL); /* 48 */
	
	/* Round 4 */
#define S41 6
#define S42 10
#define S43 15
#define S44 21
	II ( a, b, c, d, in[ 0], S41, 4096336452UL); /* 49 */
	II ( d, a, b, c, in[ 7], S42, 1126891415UL); /* 50 */
	II ( c, d, a, b, in[14], S43, 2878612391UL); /* 51 */
	II ( b, c, d, a, in[ 5], S44, 4237533241UL); /* 52 */
	II ( a, b, c, d, in[12], S41, 1700485571UL); /* 53 */
	II ( d, a, b, c, in[ 3], S42, 2399980690UL); /* 54 */
	II ( c, d, a, b, in[10], S43, 4293915773UL); /* 55 */
	II ( b, c, d, a, in[ 1], S44, 2240044497UL); /* 56 */
	II ( a, b, c, d, in[ 8], S41, 1873313359UL); /* 57 */
	II ( d, a, b, c, in[15], S42, 4264355552UL); /* 58 */
	II ( c, d, a, b, in[ 6], S43, 2734768916UL); /* 59 */
	II ( b, c, d, a, in[13], S44, 1309151649UL); /* 60 */
	II ( a, b, c, d, in[ 4], S41, 4149444226UL); /* 61 */
	II ( d, a, b, c, in[11], S42, 3174756917UL); /* 62 */
	II ( c, d, a, b, in[ 2], S43,  718787259UL); /* 63 */
	II ( b, c, d, a, in[ 9], S44, 3951481745UL); /* 64 */
	
	buf[0] += a;
	buf[1] += b;
	buf[2] += c;
	buf[3] += d;
}

/* system dependent call to get IEEE node ID.
   This sample implementation generates a random node ID
*/
static void _Cuuid_get_ieee_node_identifier(node)
	Cuuid_node_t *node;
{
  char seed[16];
	Cuuid_node_t saved_node;

  memset(seed, 0, sizeof(seed));
	_Cuuid_get_random_info(seed);
	seed[0] |= 0x80;
	memcpy(&saved_node, seed, sizeof(Cuuid_node_t));
	*node = saved_node;
}

#ifdef _WIN32
static void _Cuuid_get_random_info(seed)
	char seed[16];
{
    MD5_CTX c;
    typedef struct {
        MEMORYSTATUS m;
        SYSTEM_INFO s;
        FILETIME t;
        LARGE_INTEGER pc;
        DWORD tc;
        DWORD l;
        char hostname[MAX_COMPUTERNAME_LENGTH + 1];
    } randomness;
    randomness r;
	
    memset(&r,'\0',sizeof(r));
    _Cuuid_MD5Init(&c);
    /* memory usage stats */
    GlobalMemoryStatus(&r.m);
    /* random system stats */
    GetSystemInfo(&r.s);
    /* 100ns resolution (nominally) time of day */
    GetSystemTimeAsFileTime(&r.t);
    /* high resolution performance counter */
    QueryPerformanceCounter(&r.pc);
    /* milliseconds since last boot */
    r.tc = GetTickCount();
    r.l = MAX_COMPUTERNAME_LENGTH + 1;
    GetComputerName(r.hostname, &r.l );
    _Cuuid_MD5Update(&c, (unsigned char *) &r, sizeof(randomness));
    _Cuuid_MD5Final(&c);
	memcpy(seed,c.digest,sizeof(seed));
}

static void _Cuuid_get_system_time(uuid_time)
	Cuuid_time_t *uuid_time;
{
    ULARGE_INTEGER time;
	
    GetSystemTimeAsFileTime((FILETIME *)&time);
	
    /* NT keeps time in FILETIME format which is 100ns ticks since
       Jan 1, 1601.  UUIDs use time in 100ns ticks since Oct 15, 1582.
       The difference is 17 Days in Oct + 30 (Nov) + 31 (Dec)
       + 18 years and 5 leap days.
    */
	
	time.QuadPart +=
		(unsigned __int64) (1000*1000*10)         /* seconds   */
		* (unsigned __int64) (60 * 60 * 24)       /* days      */
		* (unsigned __int64) (17+30+31+365*18+5); /* # of days */
	
    *uuid_time = time.QuadPart;
}

#else /* _WIN32 */

static void _Cuuid_get_random_info(seed)
	char seed[16];
{
    MD5_CTX c;
    typedef struct {
        struct sysinfo s;
        struct timeval t;
        char hostname[257];
    } randomness;
#if (defined(aix) || defined(hpux))
	long sysinfoOffset;
	int fdk;
#endif
    randomness r;
	
    memset(&r,'\0',sizeof(r));
    _Cuuid_MD5Init(&c);
#if (defined(aix) || defined(hpux))
#ifdef aix
	knlist(nl,1,sizeof(struct nlist));
#else
	if (nlist(hp_unix, nl) >= 0) {
#endif
		/* offset within /dev/kmem */
		sysinfoOffset = nl[0].n_value;
		/* read special file */
		if ((fdk = open(KMEM, O_RDONLY)) >= 0) {
			if (lseek(fdk, sysinfoOffset, SEEK_SET) == sysinfoOffset) {
				read(fdk, &r.s, sizeof(struct sysinfo));
			}
			close(fdk);
		}
#ifndef aix
	}
#endif
#else /* aix || hpux */
    sysinfo(&r.s);
#endif
    gettimeofday(&r.t, (struct timezone *)0);
    gethostname(r.hostname, 256);
    _Cuuid_MD5Update(&c, (unsigned char *) &r, sizeof(randomness));
    _Cuuid_MD5Final(&c);
	memcpy(seed,c.digest,sizeof(seed));
}

static void _Cuuid_get_system_time(uuid_time)
	Cuuid_time_t *uuid_time;
{
	struct timeval tp;
	
	gettimeofday(&tp, (struct timezone *)0);
	
	/* Offset between UUID formatted times and Unix formatted times.
	   UUID UTC base time is October 15, 1582.
	   Unix base time is January 1, 1970.
	*/
	*uuid_time = (tp.tv_sec * 10000000) + (tp.tv_usec * 10) +
        CONSTLL(0x01B21DD213814000);
}
#endif /* _WIN32 */

/* Conversion from/to Cuuid/string */
int DLL_DECL Cuuid2string(output,maxlen,uuid)
     char *output;
     size_t maxlen;
     Cuuid_t *uuid;
{
  if ((output == NULL) || (uuid == NULL)) {
    serrno = EFAULT;
    return(-1);
  }

  Csnprintf(output,
	    maxlen,
	    CUUID_STRING_FMT,
	    uuid->time_low,
	    uuid->time_mid,
	    uuid->time_hi_and_version,
	    uuid->clock_seq_hi_and_reserved,
	    uuid->clock_seq_low,
	    uuid->node[0],
	    uuid->node[1],
	    uuid->node[2],
	    uuid->node[3],
	    uuid->node[4],
	    uuid->node[5]
	    );

  return(0);
}

int string2Cuuid(uuid,input)
     Cuuid_t *uuid;
     char *input;
{
  int items;
  U_LONG dummy[11];

  if ((uuid == NULL) || (input == NULL)) {
    serrno = EFAULT;
    return(-1);
  }

  items = sscanf(input,
		 CUUID_STRING_FMT,
		 &(dummy[0]),
		 &(dummy[1]),
		 &(dummy[2]),
		 &(dummy[3]),
		 &(dummy[4]),
		 &(dummy[5]),
		 &(dummy[6]),
		 &(dummy[7]),
		 &(dummy[8]),
		 &(dummy[9]),
		 &(dummy[10]));
  if (items != 11) {
    /* We did not matched all the items !? */
    serrno = EINVAL;
    return(-1);
  }

  uuid->time_low                  = (U_LONG)  dummy[0];
  uuid->time_mid                  = (U_SHORT) dummy[1];
  uuid->time_hi_and_version       = (U_SHORT) dummy[2];
  uuid->clock_seq_hi_and_reserved = (U_BYTE)  dummy[3];
  uuid->clock_seq_low             = (U_BYTE)  dummy[4];
  uuid->node[0]                   = (U_BYTE)  dummy[5];
  uuid->node[1]                   = (U_BYTE)  dummy[6];
  uuid->node[2]                   = (U_BYTE)  dummy[7];
  uuid->node[3]                   = (U_BYTE)  dummy[8];
  uuid->node[4]                   = (U_BYTE)  dummy[9];
  uuid->node[5]                   = (U_BYTE)  dummy[10];

  return(0);
}
