/*
 * $RCSfile: Cgetopt.h,v $ $Revision: 1.3 $ $Date: 2000/05/31 10:35:17 $ Jean-Damien Durand IT-PDP/DM
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Cgetopt.h    - CASTOR getopt definitions
 */

#ifndef __Cgetopt_h
#define __Cgetopt_h

#include <osdep.h>

struct Coptions {
  char *name;
  int has_arg;
  int *flag;
  int val;
};
typedef struct Coptions Coptions_t;

#define NO_ARGUMENT 0
#define REQUIRED_ARGUMENT 1
#define OPTIONAL_ARGUMENT 2

#if defined(_REENTRANT) || defined(_THREAD_SAFE) || \
   (defined(_WIN32) && (defined(_MT) || defined(_DLL)))
/*
 * Multi-thread (MT) environment
 */
EXTERN_C int  DLL_DECL  *C__Copterr _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptind _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptopt _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptreset _PROTO((void));
EXTERN_C char DLL_DECL **C__Coptarg _PROTO((void));

/*
 * Thread safe serrno. Note, C__serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define Copterr (*C__Copterr())
#define Coptind (*C__Coptind())
#define Coptopt (*C__Coptopt())
#define Coptreset (*C__Coptreset())
#define Coptarg (*C__Coptarg())

#else /* _REENTRANT || _THREAD_SAFE ... */
/*
 * non-MT environment
 */
EXTERN_C  int DLL_DECL Copterr;
EXTERN_C  int DLL_DECL Coptind;
EXTERN_C  int DLL_DECL Coptopt;
EXTERN_C  int DLL_DECL Coptreset;
EXTERN_C  char DLL_DECL *Coptarg;
#endif /* _REENTRANT || _TREAD_SAFE */

EXTERN_C int DLL_DECL Cgetopt _PROTO((int, char * CONST *, CONST char *));

#endif /* __Cgetopt_h */
