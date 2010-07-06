/*
 * $RCSfile: Cgetopt.h,v $ $Revision: 1.5 $ $Date: 2007/12/07 11:40:53 $ Jean-Damien Durand IT-PDP/DM
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
  const char *name;
  int has_arg;
  int *flag;
  int val;
};
typedef struct Coptions Coptions_t;

#define NO_ARGUMENT 0
#define REQUIRED_ARGUMENT 1
#define OPTIONAL_ARGUMENT 2

/*
 * Multi-thread (MT) environment
 */
EXTERN_C int   *C__Copterr (void);
EXTERN_C int   *C__Coptind (void);
EXTERN_C int   *C__Coptopt (void);
EXTERN_C int   *C__Coptreset (void);
EXTERN_C char **C__Coptarg (void);

/*
 * Thread safe serrno. Note, C__serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define Copterr (*C__Copterr())
#define Coptind (*C__Coptind())
#define Coptopt (*C__Coptopt())
#define Coptreset (*C__Coptreset())
#define Coptarg (*C__Coptarg())

EXTERN_C int Cgetopt (int, char * const *, const char *);
EXTERN_C int Cgetopt_long (int, char **, const char *, Coptions_t *, int *);

#endif /* __Cgetopt_h */
