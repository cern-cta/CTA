/*
 * $Id: stager_mapper.c,v 1.1 2005/05/25 14:29:02 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_mapper.c,v $ $Revision: 1.1 $ $Date: 2005/05/25 14:29:02 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

/* ============= */
/* Local headers */
/* ============= */
#include "stager_client_api.h"
#include "stager_mapper.h"
#include "getconfent.h"
#include "serrno.h"


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

  char *func = "get_mapping";
  char *category;
  char **vals = NULL;
  int nbvals = 0, rc;

  if (mt == GROUPMAPPING) {
    category = GROUP_MAPPING_CATEGORY;
  } else {
    category = USER_MAPPING_CATEGORY;
  }

  if (stager == NULL && svcclass == NULL) {
    stage_errmsg(func, "Both parameters are NULL");
    serrno = EINVAL;
    return -1;
  }

  if (stager != NULL)
    *stager = NULL;
  if (svcclass != NULL)
    *svcclass = NULL;

  rc = getconfent_multi_fromfile(STAGEMAP,
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
    stage_errmsg(func, "Could not find mapping for %\n", name);
    return -1;
  }
  return 0;
}


enum stager_type { V1, V2 };


static enum stager_type
get_stager_type(const char *name) {

  char *func = "get_stager_type";
  char *val;
  enum stager_type ret = V1;

  if (name == NULL) {
    stage_errmsg(func, "parameter is NULL");
    serrno = EINVAL;
    return -1;
  }

  val = getconfent_fromfile(STAGETYPE,
			    STAGER_TYPE_CATEGORY,
			    (char *)name,
			    0);
  if (val != NULL) {
    if ((strcmp(val, STAGER_TYPE_V2) == 0)
	|| strcmp(val, STAGER_TYPE_V2_ALT)) {
      ret = V2;
    }
  }
    
  return ret;
}


/* ================= */
/* External routines */
/* ================= */

int 
stage_mapper_setenv(const char *username, const char *groupname) {
  char *func = "stage_mapper_setenv";
  char *stager = NULL, *svcclass = NULL;
  enum stager_type stgtype;

  if (stager == NULL && groupname == NULL) {
    stage_errmsg(func, "Both parameters are NULL");
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

  if (stager != NULL) {
    stgtype = get_stager_type(stager) ;
  }

  printf("Stager: %s (type:%d) Svcclass:%s\n", stager, stgtype, svcclass);

  if (stager != NULL) free(stager);
  if (svcclass != NULL) free(svcclass);

  return 0;
}





