/*
 * $Id: Cregexp.h,v 1.1 2001/11/30 10:55:27 jdurand Exp $
 */

#ifndef __Cregexp_h
#define __Cregexp_h

/* ============= */
/* Local headers */
/* ============= */
#include <osdep.h>

/*
 * Definitions etc. for regexp(3) routines.
 *
 * Caveat:  this is V8 regexp(3) [actually, a reimplementation thereof],
 * not the System V one.
 */
#ifdef CREGEXP_NSUBEXP
#undef CREGEXP_NSUBEXP
#endif
#define CREGEXP_NSUBEXP  10

struct Cregexp {
  char *startp[CREGEXP_NSUBEXP];
  char *endp[CREGEXP_NSUBEXP];
  char regstart;         /* Internal use only. */
  char reganch;          /* Internal use only. */
  char *regmust;         /* Internal use only. */
  int regmlen;           /* Internal use only. */
  char program[1];       /* Unwarranted chumminess with compiler. */
};
typedef struct Cregexp Cregexp_t;

EXTERN_C Cregexp_t DLL_DECL *Cregexp_comp _PROTO((char *));
EXTERN_C int          DLL_DECL  Cregexp_exec _PROTO((Cregexp_t *, char *));
EXTERN_C int          DLL_DECL  Cregexp_sub _PROTO((Cregexp_t *, char *, char *, size_t));
EXTERN_C int          DLL_DECL  Cregexp_dump _PROTO((Cregexp_t *));

#endif /* __Cregexp_h */
