/*
 * $Id: Csec_plugin_GSS_mapper.c,v 1.7 2005/03/15 22:52:37 bcouturi Exp $
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */
/** 
 * Csec_mapper.c - Provides functions for mapping a principal to a local user.
 */

#ifndef lint 
static char sccsid[] = "@(#)Csec_plugin_GSS_mapper.c,v 1.1 2004/01/12 10:31:40 CERN IT/ADC/CA Benjamin Couturier";
#endif
#include <stdio.h>
#include <errno.h>
#include "serrno.h"
#include <string.h>
#include <stdlib.h>

#include "Castor_limits.h"
#include "Cpwd.h"
#include "Csec_plugin.h"

#if defined KRB5
#if defined HEIMDAL
#include <gssapi.h>
#else /* HEIMDAL */
#if defined(linux)
#include <gssapi/gssapi_generic.h>
#define GSS_C_NT_USER_NAME (gss_OID)gss_nt_user_name
#else /* linux */
#include <gssapi/gssapi.h>
#endif /* linux */
#endif /* HEIMDAL */
#else /* KRB5 */
#if defined(GSI)
#include <globus_gss_assist.h>
#include <openssl/ssl.h>
#endif /* GSI */
#endif /* KRB5 */


#ifdef KRB5
#define MECH _KRB5
static char KRB5_service_prefix[][20] = { "host",
                                          "castor-central",
                                          "castor-disk",
                                          "castor-tape",
                                          "castor-stager",
                                          "" };

#endif
#ifdef GSI
#ifdef with_pthr_suffix
#define MECH _GSI_pthr
#else
#define MECH _GSI
#endif
static char GSI_service_prefix[][20] = { "host",
                                         "castor-central",
                                         "castor-disk",
                                         "castor-tape",
                                         "castor-stager",
                                         "" };
static char *GSI_DN_header = "";

#endif
#ifdef KRB5

#define SEP '@'
#endif

/**
 * Maps the credential to the corresponding name
 */
int (CSEC_METHOD_NAME(Csec_plugin_map2name, MECH))(Csec_id_t *user_id,
						   Csec_id_t **mapped_id) {
    char *p,*pri;
    char *func = "Csec_plugin_map2name";

    if (user_id == NULL || mapped_id == NULL) {
      serrno = EINVAL;
      _Csec_errmsg(func, "Userid of mapped id are NULL");
      return -1;
    }


#ifdef KRB5


    p = strchr(_Csec_id_name(user_id), SEP);
    if (p== NULL) {
      *mapped_id = _Csec_create_id(USERNAME_MECH, _Csec_id_name(user_id));
    } else {
      char *tmp;
      size_t pos = (p - _Csec_id_name(user_id));
      tmp = (char *)malloc(pos +1);
      memcpy(tmp, _Csec_id_name(user_id), pos);
      tmp[pos] = '\0';
      *mapped_id = _Csec_create_id(USERNAME_MECH, tmp);
    }

    return 0;

#endif
    /* ESEC_NO_PRINC */


#ifdef GSI

    _Csec_trace(func, "Looking for mapping for <%s>\n", _Csec_id_name(user_id));

    pri = strdup(_Csec_id_name(user_id));
    
    if (pri != NULL && !globus_gss_assist_gridmap(pri, &p)){
        /* We have a mapping */
        _Csec_trace(func, "We have a mapping to <%s>\n", p);
	*mapped_id = _Csec_create_id(USERNAME_MECH, p);
        free(p);
        free(pri);
    } else {
      if (pri!=NULL)
	free(pri);
      *mapped_id = NULL;
      return -1;
    }
    return 0;
#endif
}
    
int (CSEC_METHOD_NAME(Csec_plugin_servicetype2name, MECH))(enum  Csec_service_type  service_type,
							   char *host, 
							   char *domain,
							   char *service_name,
							   int service_namelen) {
    int rc;
    char *func = "Csec_plugin_servicetype2name";

/*     Csec_end_trim(host); */
    
    _Csec_trace(func, "Type: %d, host:<%s> domain:<%s> (%p,%d)\n",
		service_type,
		host,
		domain,
		service_name,
		service_namelen);
    
    if (service_type < 0 ||  service_type >= CSEC_SERVICE_TYPE_MAX
        || service_name == NULL || service_namelen <= 0) {
        serrno = EINVAL;
        return -1;
    }

#ifdef KRB5    
    if (domain[0] == '.') {
      rc = snprintf(service_name, service_namelen, "%s/%s%s",
		    KRB5_service_prefix[service_type],
		    host,
		    domain);

    } else {
      rc = snprintf(service_name, service_namelen, "%s/%s.%s",
		    KRB5_service_prefix[service_type],
		    host,
		    domain);
    }
#else
#ifdef GSI
    if (domain[0] == '.') {
      rc = snprintf(service_name, service_namelen, "%s/CN=%s/%s%s",
		    GSI_DN_header,
		    GSI_service_prefix[service_type],
		    host,
		    domain);
    } else {
      rc = snprintf(service_name, service_namelen, "%s/CN=%s/%s.%s",
		    GSI_DN_header,
		    GSI_service_prefix[service_type],
		    host,
		    domain);
    }
#endif
#endif

    _Csec_trace(func,"derived service name:<%s>\n", service_name);
    
    if (rc < 0) {
        serrno = E2BIG;
        return -1;
    }

    return 0;
}


int (CSEC_METHOD_NAME(Csec_plugin_isIdService, MECH))(Csec_id_t *id) {
  return -1;
}

