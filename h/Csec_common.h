#ifndef _CSEC_COMMON_H
#define _CSEC_COMMON_H

#include <osdep.h>
#include <Castor_limits.h>
#include <marshall.h>

#include <Csec_constants.h>

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
#else /* GSI */
/* In all cases, we use the gss_buffer_t as the structure to hold the tokens */
typedef struct gss_buffer_desc_struct
{
  size_t length;
  void *value;
} gss_buffer_desc, *gss_buffer_t;
#endif
#endif

int *C__Csec_errno();
#define Csec_errno (*C__Cns_errno())

/* Structure containing protocol information */
typedef struct Csec_protocol {
  char id[PROTID_SIZE+1];
} Csec_protocol;

/** Structure holding the context */ 
typedef struct Csec_context {
  int magic;
  U_LONG flags; /* Flags containing status of  context structure */
  void *shhandle; /* Handle to the shared library */
  void *credentials; /*     gss_cred_id_t credentials; */
  void *connection_context;     /*     gss_ctx_id_t context; */

  /* Variables containing ths status of the security protocol negociation
     between client and server */
  int protocol_negociation_status;
  Csec_protocol *protocols; /* Contains the list of protocols supported by this end of the connection */
  int nb_protocols; /* Number of entries in the previous list */
  int current_protocol; /* Index of the protocol in the list mentioned above */
  Csec_protocol *peer_protocols; /* List of protocols accepted by peer */
  int nb_peer_protocols; /* Number of entries in list above */

  /* Local and peer identity information */
  char local_name[CA_MAXCSECNAMELEN+1];
  char peer_name[CA_MAXCSECNAMELEN+1]; /*  Requested server name (in the Csec client) */
  char effective_peer_name[CA_MAXCSECNAMELEN+1]; /* Name returned by the GSI layer */
  unsigned long context_flags; /* Context flags from the GSS API */
  char peer_username[CA_MAXCSECNAMELEN+1];
  int peer_uid;
  int peer_gid;

  /* Used on the server only ! */
  int server_service_type;

  /* Pointers to the functions in the loaded shared libraries */
  int (*Csec_init_context)(struct Csec_context *);
  int (*Csec_reinit_context)(struct Csec_context *);
  int (*Csec_delete_connection_context)(struct Csec_context *);
  int (*Csec_delete_creds)(struct Csec_context *);
  int (*Csec_acquire_creds)(struct Csec_context *, char *, int);
  int (*Csec_server_establish_context)(struct Csec_context *, int);
  int (*Csec_server_establish_context_ext)(struct Csec_context *, int,char *, int);
  int (*Csec_client_establish_context)(struct Csec_context *, int);
  int (*Csec_map2name)(struct Csec_context *, char *, char *, int);
  int (*Csec_get_service_name)(struct Csec_context *, int, char *, char *, char *, int);

} Csec_context_t;



/** Structure with thread specific information */
struct Csec_api_thread_info {
  char  errbuf[ERRBUFSIZE+1];
  int	sec_errno;
  int   init_done;
  int   trace_mode;
  char  trace_file[CA_MAXNAMELEN+1];
  Csec_context_t default_context;
};


int DLL_DECL check_ctx _PROTO ((Csec_context_t *, char *));
void DLL_DECL Csec_unload_shlib _PROTO ((Csec_context_t *)); 
void DLL_DECL *Csec_get_shlib _PROTO ((Csec_context_t *)); 
/* int DLL_DECL Csec_init_context_ext _PROTO ((Csec_context_t *, int, int)) ; */
int DLL_DECL Csec_errmsg _PROTO((char *func, char *msg, ...));
int DLL_DECL Csec_apiinit _PROTO((struct Csec_api_thread_info **thip));
int DLL_DECL Csec_seterrbuf _PROTO((char *buffer, int buflen));
int DLL_DECL Csec_trace _PROTO((char *func, char *msg, ...));
int DLL_DECL Csec_setup_trace _PROTO(());
int DLL_DECL _Csec_recv_token _PROTO ((int s, gss_buffer_t tok, int timeout, int *token_type));
int DLL_DECL _Csec_send_token _PROTO ((int s, gss_buffer_t tok, int timeout, int token_type));
void DLL_DECL  _Csec_print_token _PROTO((gss_buffer_t tok));

/* Protocol functions */
int DLL_DECL Csec_client_negociate_protocol _PROTO((int, int, Csec_context_t *));
int DLL_DECL Csec_client_send_protocol _PROTO((int, int, Csec_context_t *));
int DLL_DECL Csec_client_receive_server_reponse _PROTO((int, int, Csec_context_t *));
int DLL_DECL Csec_server_receive_protocol _PROTO((int, int, Csec_context_t *, char *, int));
int DLL_DECL Csec_server_send_response _PROTO ((int, int, Csec_context_t *, int));   

/* Misc utils */
int DLL_DECL  Csec_server_establish_context_ext _PROTO ((Csec_context_t *ctx,
                                                         int s,
                                                         char *buf,
                                                         int len));
int DLL_DECL Csec_map2name _PROTO((Csec_context_t *ctx,
                                   char *principal,
                                   char *name,
                                   int maxnamelen));
int DLL_DECL Csec_get_service_name _PROTO ((Csec_context_t *ctx,
                                            int service_type,
                                            char *host,
                                            char *domain,
                                            char *service_name,
                                            int service_namelen));
int DLL_DECL Csec_map2id _PROTO((Csec_context_t *ctx, char *principal, uid_t *uid, gid_t *gid));
int DLL_DECL Csec_name2id _PROTO((char *name, uid_t *uid, gid_t *gid));
int DLL_DECL Csec_get_peer_service_name _PROTO ((Csec_context_t *ctx, int s, int service_type,
                                                 char *service_name, int service_namelen));
int DLL_DECL Csec_get_local_service_name _PROTO ((Csec_context_t *ctx, int service_type,
                                                  char *service_name, int service_namelen));
int DLL_DECL  Csec_delete_connection_context _PROTO ((Csec_context_t *));
int DLL_DECL  Csec_delete_creds _PROTO ((Csec_context_t *));
int DLL_DECL Csec_server_set_service_name _PROTO ((Csec_context_t *));
int DLL_DECL Csec_client_set_service_name _PROTO ((Csec_context_t *, 
						   int));
int DLL_DECL Csec_context_is_client _PROTO ((Csec_context_t *ctx));

#endif /* _CSEC_COMMON_H */
