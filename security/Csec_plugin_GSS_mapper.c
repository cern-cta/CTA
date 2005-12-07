/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint 
static char sccsid[] = "@(#)$RCSfile: Csec_plugin_GSS_mapper.c,v $ $Revision: 1.8 $ $Date: 2005/12/07 10:19:21 $ CERN IT/ADC/CA Benjamin Couturier";
#endif

/** 
 * Csec_plugin_GSS_mapper.c - Provides functions for mapping a principal to a local user.
 */

#include <stdio.h>
#include <errno.h>
#include "serrno.h"
#include <string.h>
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
int (CSEC_METHOD_NAME(Csec_map2name, MECH))(FPARG, Csec_context_t *ctx, const char *principal, char *name, int maxnamelen) {
    char *p,*pri;
    char *func = "Csec_map2name";
#ifdef KRB5
    p = strchr(principal, SEP);
    if (p== NULL) {
        strncpy(name, principal, maxnamelen);
    } else {
        size_t pos = (p - principal);
        memcpy(name, principal, pos);
        name[pos] = '\0';
    }
        
    return 0;
    
}
#endif
    /* ESEC_NO_PRINC */


#ifdef GSI

    Csec_trace(func, "Looking for mapping for <%s>\n", principal);

    pri = strdup(principal);
    
    if (pri != NULL && !globus_gss_assist_gridmap(pri, &p)){
        /* We have a mapping */
        Csec_trace(func, "We have a mapping to <%s>\n", p);
        strncpy(name, p, maxnamelen);
        free(p);
        free(pri);
    } else {
        if (pri!=NULL)
          free(pri);

        name[0]=0;
        return -1;
    }
    return 0;
}
#endif

    
int (CSEC_METHOD_NAME(Csec_get_service_name, MECH))(FPARG, Csec_context_t *ctx, int service_type, char *host, char *domain,
                          char *service_name, int service_namelen) {

    int rc;
    char *func = "Csec_get_service_name";

/*     Csec_end_trim(host); */
    
    Csec_trace(func, "Type: %d, host:<%s> domain:<%s> (%p,%d)\n",
               service_type,
               host,
               domain,
               service_name,
               service_namelen);
    
    if (service_type < 0 ||  service_type >= CSEC_SERVICE_(MAX)
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

    Csec_trace(func,"derived service name:<%s>\n", service_name);
    
    if (rc < 0) {
        serrno = E2BIG;
        return -1;
    }

    return 0;
}
