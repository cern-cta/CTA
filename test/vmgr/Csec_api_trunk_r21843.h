#ifndef _CSEC_API_H
#define _CSEC_API_H

#include <sys/types.h>
#include <Csec_common_trunk_r21843.h>

/* Init and cleanup */
int  Csec_client_initContext (Csec_context_t *,
			      int,
			      Csec_protocol *);

int  Csec_server_initContext (Csec_context_t *,
			      int,
			      Csec_protocol *);
int  Csec_server_reinitContext  (Csec_context_t *,
				 int,
				 Csec_protocol *);

int  Csec_clearContext (Csec_context_t *);

/* context establishment */

int  Csec_server_establishContext (Csec_context_t *,
				   int s);
int  Csec_client_establishContext (Csec_context_t *,
				   int s);

/* Error lookup */
char* Csec_getErrorMessage ();


/* security options, authorization id & delegated credential retrieval */

int Csec_client_setSecurityOpts (Csec_context_t *, int);

int Csec_server_setSecurityOpts (Csec_context_t *, int);

int Csec_server_getDelegatedCredential (Csec_context_t *, char **, void **, size_t *);

int Csec_client_setAuthorizationId (Csec_context_t *, const char *, const char *);

int Csec_server_getAuthorizationId (Csec_context_t *, char **, char **);

char *Csec_server_get_client_vo (Csec_context_t *);

char **Csec_server_get_client_fqans (Csec_context_t *, int *);

int Csec_client_setVOMS_data (Csec_context_t *, const char *, char **, int);

/* Service type & name handling */

char *Csec_client_get_service_name (Csec_context_t *);

int Csec_server_set_service_type (Csec_context_t *, 
				  int);
char *Csec_server_get_service_name (Csec_context_t *);

int Csec_server_getClientId (Csec_context_t *, char **, char **);

int Csec_client_get_service_type (Csec_context_t *);

int Csec_isIdAService (const char *, const char *);

int Csec_mapToLocalUser (const char *, const char *, char *, size_t, uid_t *, gid_t *);

/* Funtion providing a default per thread context */
Csec_context_t* Csec_get_default_context ();

#endif
