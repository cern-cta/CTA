/*
 * $Id: stager_mapper.c,v 1.14 2009/01/14 17:33:32 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

/* ============= */
/* Local headers */
/* ============= */
#include "Castor_limits.h"
#include "stager_client_api.h"
#include "stager_mapper.h"
#include "getconfent.h"
#include "serrno.h"
#include "osdep.h"
#include "Csnprintf.h"


char stghostenv[CA_MAXLINELEN+1];
char stgpoolenv[CA_MAXLINELEN+1];
char svcclassenv[CA_MAXLINELEN+1];

EXTERN_C void stage_trace   (int, char *, ...);


/* ================= */
/* Internal routines */
/* ================= */

static void
free_list(char **p, int size) {
  int i;
  if (p == NULL) return;
  for (i=0; i<size; i++) {
    if (p[i] != NULL)
      free(p[i]);
  }
  free(p);
}

enum mapping_type { USERMAPPING, GROUPMAPPING };

static int
get_mapping(enum mapping_type mt,
	    const char *name, 
	    char **stager, 
	    char **svcclass) {
  char *category;
  char **vals = NULL;
  int nbvals = 0, rc;

  if (mt == GROUPMAPPING) {
    category = GROUP_MAPPING_CATEGORY;
  } else {
    category = USER_MAPPING_CATEGORY;
  }

  if (stager == NULL && svcclass == NULL) {
    serrno = EINVAL;
    return -1;
  }
  if (stager != NULL)
    *stager = NULL;
  if (svcclass != NULL)
    *svcclass = NULL;

  rc = getconfent_multi_fromfile("/etc/castor/stagemap.conf",
				 category,
				 (char *)name,
				 1,
				 &vals,
				 &nbvals); 
  if (rc == 0) {
    if (nbvals >=1) {
      if (stager != NULL) {
	*stager = (char *)strdup(vals[0]);
      }
    } 

    if (nbvals >= 2) {
      if (svcclass != NULL) {
	*svcclass = (char *)strdup(vals[1]);
      }
    }

    free_list(vals, nbvals); 
  } else {
    /* errno should have been set already */
    return -1;
  }
  return 0;
}


/* ================= */
/* External routines */
/* ================= */

int 
stage_mapper_setenv(const char *username, 
		    const char *groupname,
		    char **mstager,
		    char **msvcclass) {
  char *stager = NULL, *svcclass = NULL;

  if (username == NULL && groupname == NULL) {
    serrno = EINVAL;
    return -1;
  }


  if (username != NULL) {
    if (get_mapping(USERMAPPING,
		    username,
		    &stager,
		    &svcclass) != 0) {
      return -1;
    }
  }

  if (stager == NULL 
      && svcclass == NULL
      && groupname != NULL) {
    if (get_mapping(GROUPMAPPING,
		    groupname,
		    &stager,
		    &svcclass) != 0) {
      return -1;
    }
  }

  if (stager!= NULL) {
    Csnprintf(stghostenv,  CA_MAXLINELEN, "STAGE_HOST=%s", stager);
    putenv(stghostenv);
    stage_trace(3, stghostenv);
    if (mstager != NULL) {
      *mstager = stager;
    } else {
      free(stager);
    }
  }

  if (svcclass!= NULL) {
    Csnprintf(stgpoolenv,  CA_MAXLINELEN, "STAGE_POOL=%s", svcclass);
    putenv(stgpoolenv);
    stage_trace(3, stgpoolenv);
    Csnprintf(svcclassenv,  CA_MAXLINELEN, "STAGE_SVCCLASS=%s", svcclass);
    putenv(svcclassenv);
    stage_trace(3, svcclassenv);
    if (msvcclass != NULL) {
      *msvcclass = svcclass;
    } else {
      free(svcclass);
    }
  }
  
  return 0;
}


/* without setting Enviroment Variable*/

int 
just_stage_mapper(const char *username, 
		  const char *groupname,
		  char **mstager,
		  char **msvcclass) {

  char *stager = NULL, *svcclass = NULL;

  if (username == NULL && groupname == NULL) {
    serrno = EINVAL;
    return -1;
  }
 
  if (username != NULL) {
    if (get_mapping(USERMAPPING,
		    username,
		    &stager,
		    &svcclass) != 0) {
      return -1;
    } 
  }

  if (stager == NULL 
      && svcclass == NULL
      && groupname != NULL) {
    if (get_mapping(GROUPMAPPING,
		    groupname,
		    &stager,
		    &svcclass) != 0) {
      return -1;
    }
  }
 
  if (stager!= NULL) {
    if (mstager != NULL) {
      *mstager = stager;
    } else {
      free(stager);
    }
  }
 
  if (svcclass!= NULL) {
    if (msvcclass != NULL) {
      *msvcclass = svcclass;
    } else {
      free(svcclass);
    }
  }

  return 0;
}


