#ifndef _CSEC_API_H
#define _CSEC_API_H

#include <sys/types.h>
#include <Csec_common.h>

/* Init and cleanup */
int DLL_DECL  Csec_client_init_context _PROTO ((Csec_context_t *,
						int,
						Csec_protocol *));

int DLL_DECL  Csec_server_init_context _PROTO ((Csec_context_t *,
						int,
						Csec_protocol *));
int DLL_DECL  Csec_server_reinit_context  _PROTO ((Csec_context_t *,
						   int,
						   Csec_protocol *));

int DLL_DECL  Csec_clear_context _PROTO ((Csec_context_t *));

/* Credentials and context establishment */
int DLL_DECL  Csec_server_acquire_creds _PROTO ((Csec_context_t *,
                                                 char *));
int DLL_DECL  Csec_server_establish_context _PROTO ((Csec_context_t *,
                                                     int s));
int DLL_DECL  Csec_client_establish_context _PROTO ((Csec_context_t *,
                                                     int s));


/* Service type & name handling */

char DLL_DECL *Csec_client_get_service_name _PROTO((Csec_context_t *));

int DLL_DECL Csec_server_set_service_type _PROTO ((Csec_context_t *, 
						   int));
char DLL_DECL *Csec_server_get_service_name _PROTO((Csec_context_t *));

char DLL_DECL *Csec_server_get_client_username _PROTO ((Csec_context_t *,
							int *,
							int *));

char DLL_DECL *Csec_server_get_client_name _PROTO ((Csec_context_t *));

int DLL_DECL Csec_client_get_service_type _PROTO ((Csec_context_t *));

int DLL_DECL Csec_server_is_castor_service _PROTO ((Csec_context_t *));

/* Funtion providing a default per thread context */
Csec_context_t *DLL_DECL Csec_get_default_context _PROTO(());

#endif
