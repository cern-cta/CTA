#ifndef _CSEC_API_H
#define _CSEC_API_H

#include <sys/types.h>
#include <Csec_common.h>

/* Init and cleanup */
int DLL_DECL  Csec_client_init_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_server_init_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_server_reinit_context  _PROTO ((Csec_context *));
int DLL_DECL  Csec_clear_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_delete_connection_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_delete_creds _PROTO ((Csec_context *));

/* Credentials and context establishment */
int DLL_DECL  Csec_server_acquire_creds _PROTO ((Csec_context *,
                                                 char *));
int DLL_DECL  Csec_server_establish_context _PROTO ((Csec_context *,
                                                     int s));
int DLL_DECL  Csec_client_establish_context _PROTO ((Csec_context *,
                                                     int s));


/* Service type & name handling */
int DLL_DECL Csec_client_set_service_name _PROTO ((Csec_context *, 
						   int, 
						   int));
char DLL_DECL *Csec_client_get_service_name _PROTO((Csec_context *));

int DLL_DECL Csec_server_set_service_type _PROTO ((Csec_context *, 
						   int));
int DLL_DECL Csec_server_set_service_name _PROTO ((Csec_context *, 
						   int));
char DLL_DECL *Csec_server_get_service_name _PROTO((Csec_context *));

char DLL_DECL *Csec_server_get_client_username _PROTO ((Csec_context *,
							int *,
							int *));
#endif
