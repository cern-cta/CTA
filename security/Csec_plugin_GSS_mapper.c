/*
 * $id$
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
#include <serrno.h>
#include <string.h>
#include <Castor_limits.h>
#include <Cpwd.h>

#include <Csec_plugin.h>


char KRB5_service_prefix[][20] = { "host",
                                   "host",
                                   "host",
                                   "" };

char GSI_service_prefix[][20] = { "host",
                                  "host",
                                  "host",
                                  "" };

char *GSI_DN_header = "";


#ifdef KRB5
int Csec_map2name_krb5(char *principal, char *name, int maxnamelen);
#else
#ifdef GSI
int Csec_map2name_GSI(char *principal, char *name, int maxnamelen);
#endif
#endif



/**
 * Maps the credential to the corresponding name
 */
int Csec_map2name_impl(Csec_context_t *ctx, char *principal, char *name, int maxnamelen) {

#ifdef KRB5
    return Csec_map2name_krb5(principal, name, maxnamelen);
#else
#ifdef GSI
    return Csec_map2name_GSI(principal, name, maxnamelen);
#endif
#endif
}

#ifdef KRB5

#define SEP '@'

/**
 * Maps the credential to the corresponding name
 */
int Csec_map2name_krb5(char *principal, char *name, int maxnamelen) {
    char *p;

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
/**
 * Maps the credential to the corresponding name
 */
int Csec_map2name_GSI(char *principal, char *name, int maxnamelen) {
    char *p;
    char *func = "Csec_map2name_GSI";

    Csec_trace(func, "Looking for mapping for <%s>\n", principal);
    
    if (!globus_gss_assist_gridmap(principal, &p)){
        /* We have a mapping */
        Csec_trace(func, "We have a mapping to <%s>\n", p);
        strncpy(name, p, maxnamelen);
        free(p);
    } else {
        name[0]=0;
        return -1;
    }
    return 0;
}
#endif

    

int Csec_get_service_name_impl(Csec_context_t *ctx, int service_type, char *host, char *domain,
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
    
    if (service_type < 0 ||  service_type > CSEC_SERVICE_TYPE_TAPE
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
    rc = snprintf(service_name, service_namelen, "%s/CN=%s/%s%s",
                  GSI_DN_header,
                  KRB5_service_prefix[service_type],
                  host,
                  domain);
#endif
#endif

    Csec_trace(func,"derived service name:<%s>\n", service_name);
    
    if (rc < 0) {
        serrno = E2BIG;
        return -1;
    }

    return 0;
}
