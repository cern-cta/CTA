/*
 * $Id: Cregexp_magic.h,v 1.1 2001/11/30 10:55:27 jdurand Exp $
 */

#ifndef __Cregexp_magic_h
#define __Cregexp_magic_h

/*
 * The first byte of the regexp internal "program" is actually this magic
 * number; the start node begins in the second byte.
 */

#ifdef CREGEXP_MAGIC
#undef CREGEXP_MAGIC
#endif
#define	CREGEXP_MAGIC 0234

#endif /* __Cregexp_magic_h */
