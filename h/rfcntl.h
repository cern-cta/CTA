/*
 * @(#)rfcntl.h	1.10 06/04/98  CERN CN-SW/DC Antoine Trannoy, Jean-Philippe Baud
 */

/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

/* rfcntl.h		Macros to convert flags to a single set	*/

/*
 * The SHIFT set of flags is the one defined in Irix (SGI), Unicos (CRAY) and on Apollo machines.
 * These flags do not have the same value on SUN's machines, DEC stations or
 * AIXESA or Linux.
 *
 *		SHIFT		SUN, ULTRIX	LINUX		LynxOS
 *				AIXESA, OSF
 *
 * O_RDONLY	000000		000000		000000		000000
 * O_WRONLY	000001		000001		000001		000001
 * O_RDWR	000002		000002		000002		000002
 * O_NDELAY	000004		000004		004000  !	000100 !
 * 0_APPEND	000010		000010		002000  !	000004 !
 * O_CREAT	000400		001000  !	000100  !	000010 !
 * O_TRUNC	001000		002000	!	001000		000020 !
 * O_EXCL	002000		004000	!	000200  !	000040 !
 */

#if defined(sun) || defined(ultrix) || defined(sgi) || defined(CRAY) || defined(apollo) || defined(hpux) || defined(_AIX) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(_WIN32) || defined(__Lynx__)

#if defined(sgi) || defined(CRAY) || defined(apollo) || defined(hpux) || (defined(_AIX) && defined(_IBMR2))  || defined(SOLARIS) || defined(_WIN32) 
#define htonopnflg(A)		(A)
#define ntohopnflg(A)		(A)
#endif	/* sgi || CRAY || apollo || hpux || AIX */

#if (defined(sun) && !defined(SOLARIS)) || defined(ultrix) || (defined(_AIX) && defined(_IBMESA)) || ( defined(__osf__) && defined(__alpha) )
#define htonopnflg(A)		(((A) & ~(007000)) | (((A) & 007000) >> 1)) 
#define ntohopnflg(A)		(((A) & ~(003400)) | (((A) & 003400) << 1))
#endif	/* sun || ultrix || AIXESA || alpha-osf */

#if defined(linux)
#define htonopnflg(A)		(((A) & 004000) >> 9) | (((A) & 002000) >> 7) | \
				(((A) & 000100) << 2) | (((A) & 000200) << 3) | \
				((A) & 001003)
#define ntohopnflg(A)		(((A) & 000004) << 9) | (((A) & 000010) << 7) | \
				(((A) & 000400) >> 2) | (((A) & 002000) >> 3) | \
				((A) & 001003)
#endif	/* linux */

#if defined(__Lynx__)
#define htonopnflg(A)		(((A) & 000100) >> 4) | (((A) & 000004) << 1) | \
				(((A) & 000010) << 5) | (((A) & 000020) << 5) | \
				(((A) & 000040) << 5) | ((A) & 000003)
#define ntohopnflg(A)		(((A) & 000004) << 4) | (((A) & 000010) >> 1) | \
				(((A) & 000400) >> 5) | (((A) & 001000) >> 5) | \
				(((A) & 002000) >> 5) | ((A) & 000003)
#endif	/* __Lynx__ */

#endif	/* sun || ultrix || sgi || CRAY || apollo || hpux || AIX || alpha-osf || linux || _WIN32 || __Lynx__ */
