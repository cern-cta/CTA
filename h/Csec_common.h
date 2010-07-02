#ifndef _CSEC_COMMON_H
#define _CSEC_COMMON_H

#include <osdep.h>
#include <Castor_limits.h>
#include <marshall.h>

#include <Csec_constants.h>

/* Structure used in Csec - similar to a gss_buffer */
typedef struct csec_buffer_desc_struct
{
  size_t length;
  void *value;
} csec_buffer_desc, *csec_buffer_t;

/* Structure containing protocol information */
typedef struct Csec_protocol {
  char id[CA_MAXCSECPROTOLEN+1];
} Csec_protocol;

/** Structure holding the context */ 
typedef struct Csec_context {
  int magic;
  U_LONG flags; /* Flags containing status of  context structure */
  void *shhandle; /* Handle to the shared library */
  void *credentials; /*     gss_cred_id_t credentials; */
  void *connection_context;     /*     gss_ctx_id_t context; */

  U_LONG peer_sec_flags;
  size_t deleg_credentials_len; /* size of exported deleg credentials */
  void *deleg_credentials; /* malloc buffer containing exported credentials from delegation */

  U_LONG peer_version; /* Peer Csec protocol version */
  U_LONG sec_flags; /* Security flags */

  /* Possible protocols avilable at this end of the connecrtion */
  Csec_protocol *total_protocols; /* Contains the list of protocols supported by this end of the connection */
  int nb_total_protocols; /* Number of entries in the previous list */

  /* Variables containing ths status of the security protocol negociation
     between client and server */
  U_LONG protocol_negociation_status;
  Csec_protocol *protocols; /* Protocols to offer/check-against the peer */
  int nb_protocols; /* Number of protocols above */
  int current_protocol; /* Index of the protocol in the list mentioned above */
  Csec_protocol *peer_protocols; /* List of protocols offered by peer */
  int nb_peer_protocols; /* Number of entries in list above */

  /* Local and peer identity information */
  char client_authorization_mech[CA_MAXCSECPROTOLEN+1]; /* Authorization mech associated to the name */
  char client_authorization_id[CA_MAXCSECNAMELEN+1]; /* Private authorization name the client may set */
  char local_name[CA_MAXCSECNAMELEN+1];
  char peer_name[CA_MAXCSECNAMELEN+1]; /*  Requested server name (in the Csec client) */
  char effective_peer_name[CA_MAXCSECNAMELEN+1]; /* Name returned by the GSI layer */
  U_LONG context_flags; /* Context flags from the GSS API */
  char peer_username[CA_MAXCSECNAMELEN+1];
  int peer_uid;
  int peer_gid;

  int thread_safe;
  int server_service_type;

  /* Pointers to the functions in the loaded shared libraries */
  int (*Csec_activate)(struct Csec_context *);
  int (*Csec_deactivate)(struct Csec_context *);
  int (*Csec_init_context)(struct Csec_context *);
  int (*Csec_reinit_context)(struct Csec_context *);
  int (*Csec_delete_connection_context)(struct Csec_context *);
  int (*Csec_delete_creds)(struct Csec_context *);
  int (*Csec_acquire_creds)(struct Csec_context *, char *, int);
  int (*Csec_server_establish_context)(struct Csec_context *, int);
  int (*Csec_server_establish_context_ext)(struct Csec_context *, int,char *, int);
  int (*Csec_client_establish_context)(struct Csec_context *, int);
  int (*Csec_map2name)(struct Csec_context *, const char *, char *, int);
  int (*Csec_get_service_name)(struct Csec_context *, int, char *, char *, char *, int);

  /* Pointers to VOMS data */
  char *voname;
  char **fqan;
  int nbfqan;

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


int check_ctx (Csec_context_t *, char *);
void Csec_unload_shlib (Csec_context_t *); 
void *Csec_get_shlib (Csec_context_t *); 
/* int Csec_init_context_ext (Csec_context_t *, int, int) ; */
int Csec_errmsg (char *func, char *msg, ...);
int Csec_apiinit (struct Csec_api_thread_info **thip);
int Csec_seterrbuf (char *buffer, int buflen);
int Csec_trace (char *func, char *msg, ...);
int Csec_setup_trace ();
int _Csec_recv_token (int s, csec_buffer_t tok, int timeout, int *token_type);
int _Csec_send_token (int s, csec_buffer_t tok, int timeout, int token_type);
void  _Csec_print_token (csec_buffer_t tok);

/* Protocol functions */
int Csec_client_negociate_protocol (int, int, Csec_context_t *);
int Csec_setup_protocols_to_offer (Csec_context_t *);
int Csec_server_negociate_protocol (int, int, Csec_context_t *, char *, int);

/* Misc utils */
int  Csec_server_establish_context_ext (Csec_context_t *ctx,
					int s,
					char *buf,
					int len);
int Csec_map2name (Csec_context_t *ctx,
		   const char *principal,
		   char *name,
		   int maxnamelen);
int Csec_get_service_name (Csec_context_t *ctx,
			   int service_type,
			   char *host,
			   char *domain,
			   char *service_name,
			   int service_namelen);
int Csec_map2id (Csec_context_t *ctx, char *principal, uid_t *uid, gid_t *gid);
int Csec_name2id (char *name, uid_t *uid, gid_t *gid);
int Csec_get_peer_service_name (Csec_context_t *ctx, int s, int service_type,
				char *service_name, int service_namelen);
int Csec_get_local_service_name (Csec_context_t *ctx, int service_type,
				 char *service_name, int service_namelen);
int Csec_server_set_service_name (Csec_context_t *, int);
int Csec_client_set_service_name (Csec_context_t *, int);
int Csec_context_is_client (Csec_context_t *ctx);

#endif /* _CSEC_COMMON_H */
