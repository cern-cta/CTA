#ifndef _CSEC_API_H
#define _CSEC_API_H

#include <sys/types.h>
#include <Csec_common.h>

int DLL_DECL  Csec_client_init_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_server_init_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_server_reinit_context  _PROTO ((Csec_context *));
int DLL_DECL  Csec_clear_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_delete_connection_context _PROTO ((Csec_context *));
int DLL_DECL  Csec_delete_creds _PROTO ((Csec_context *));


int DLL_DECL  Csec_server_acquire_creds _PROTO ((Csec_context *,
                                                 char *));
int DLL_DECL  Csec_server_establish_context _PROTO ((Csec_context *,
                                                     int s));
int DLL_DECL Csec_client_set_service_name _PROTO ((Csec_context *, 
						   int, 
						   int));
char DLL_DECL *Csec_client_get_service_name _PROTO((Csec_context *));

int DLL_DECL Csec_server_set_service_type _PROTO ((Csec_context *, 
						   int));
int DLL_DECL Csec_server_set_service_name _PROTO ((Csec_context *, 
						   int s));
char DLL_DECL *Csec_server_get_service_name _PROTO((Csec_context *));

int DLL_DECL  Csec_client_establish_context _PROTO ((Csec_context *,
                                                     int s));
char DLL_DECL *Csec_server_get_client_username _PROTO ((Csec_context *,
							int *,
							int *));

/* INTERNAL CALLS */

int DLL_DECL  Csec_server_establish_context_ext _PROTO ((Csec_context *ctx,
                                                         int s,
                                                         char *buf,
                                                         int len));

int DLL_DECL Csec_map2name _PROTO((Csec_context *ctx,
                                   char *principal,
                                   char *name,
                                   int maxnamelen));


int DLL_DECL Csec_get_service_name _PROTO ((Csec_context *ctx,
                                            int service_type,
                                            char *host,
                                            char *domain,
                                            char *service_name,
                                            int service_namelen));

int DLL_DECL Csec_map2id _PROTO((Csec_context *ctx, char *principal, uid_t *uid, gid_t *gid));
int DLL_DECL Csec_name2id _PROTO((char *name, uid_t *uid, gid_t *gid));
int DLL_DECL Csec_get_peer_service_name _PROTO ((Csec_context *ctx, int s, int service_type,
                                                 char *service_name, int service_namelen));
int DLL_DECL Csec_get_local_service_name _PROTO ((Csec_context *ctx, int s, int service_type,
                                                  char *service_name, int service_namelen));


#endif
