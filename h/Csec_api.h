/******************************************************************************
 *                      Csec_api.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: Csec_api.h,v $ $Revision: 1.12 $ $Release$ $Date: 2005/03/15 22:55:19 $ $Author: bcouturi $
 *
 * @author Ben Couturier / David Smith / Jean-Philippe Baud
 *****************************************************************************/

/** @file $RCSfile: Csec_api.h,v $
 * @version $Revision: 1.12 $
 * @date $Date: 2005/03/15 22:55:19 $
 */

/** @mainpage CASTOR Security API
 * $RCSfile: Csec_api.h,v $
 *
 * @section introduction Introduction
 * 
 * The CASTOR security API (Csec) allows to easily secure CASTOR servers and clients.
 * It offers the possibility to authenticate clients using either:
 * - The Globus Security Infrastructure (GSI)
 * - Kerberos version 4 and 5
 * 
 * There also is a non secure mode (ID) which doesn't require any special setup,
 * but its use is not recommended.
 *
 * @section overview Overview and principles
 *
 * The Csec API allows a server to authenticate a client over a connected socket.
 * A handshake first takes place between them to agree on the mechanism used for 
 * authentication, and on the options used (e.g. if they need to attempt delegation).
 * Once that is performed successfully, the mechanism dependent handshake takes place.
 *
 * To interface with the various libraries, Csec uses "plugins" built as shared
 * libraries which it opens using the programming interface to the dynamic linker 
 * (dlopen, dlsym...). The plugin are located by file name: e.g. the Csec plugin for 
 * KRB5 is called libCsec_plugin_KRB5.so.
 *
 * Nearly all functions use a security context (of type Csec_context_t), which
 * should be properly initialized before usage.
 *
 * When initializing a context, a client needs to specify the type of service it is 
 * going to connect to: that type of service is transformed into server credentials
 * that are checked by the client. Several service types have been defined:
 *
 * - CSEC_SERVICE_TYPE_HOST
 * - CSEC_SERVICE_TYPE_CENTRAL
 * - CSEC_SERVICE_TYPE_DISK
 * - CSEC_SERVICE_TYPE_TAPE
 * - CSEC_SERVICE_TYPE_STAGER
 *
 * for example CSEC_SERVICE_TYPE_HOST with GSI maps to [...]CN=host/machinename,
 * and with KRB5 maps to "host/machinename@DOMAIN"
 *
 * The protocol chosen for authentication by the Csec layer can be specified 
 * by the initialization calls. If this has not be done (NULL passed to the 
 * Csec_x_initContext function  as 3rd parameter) then the mechanisms are taken 
 * from the process environment,  or in the last resort the compiled default is taken.
 *
 * - The Client will try the mechanisms listed in the CSEC_MECH environment variables
 *   e.g. export CSEC_MECH="GSI KRB5" will have the client try first GSI then KRB5...
 * 
 * - The Server uses the mechanisms listed in the CSEC_AUTHMECH environment variable
 *   e.g. export CSEC_AUTHMECH=GSI will have the server only accept authentication with GSI
 *
 * @section Client_API Client API
 *
 * The following code shows a typical example of how a client show initiate a security 
 * context over a connected file descriptor:
 *
 * \code
 * // create a context 
 * Csec_context_t sec_ctx;
 *
 * // Initialize the context, indicating that the server will use a host key 
 * Csec_client_initContext(&sec_ctx,CSEC_SERVICE_TYPE_HOST, NULL);
 *
 * // Request that credentials delegation be performed
 * Csec_client_setSecurityOpts(&sec_ctx, CSEC_OPT_DELEG_FLAG);
 *
 * // Perform the context establishment exchange over the socket
 * Csec_client_establishContext(&sec_ctx, socket)
 * [...]
 * // Once the connection is done, clear the context
 * Csec_clearContext(&sec_ctx);
 * \endcode
 *
 * @section Server_API Server API
 *
 * \code 
 * // create a context 
 * Csec_context_t sec_ctx;
 *
 * // Initialize the context, indicating that the server will use a host key 
 * Csec_server_initContext(&sec_ctx,CSEC_SERVICE_TYPE_HOST, NULL);
 *
 * // Request that credentials delegation be performed
 * Csec_server_setSecurityOpts(&sec_ctx, CSEC_OPT_DELEG_FLAG);
 *
 * // Perform the context establishment exchange over the socket
 * Csec_server_establishContext(&sec_ctx, socket)
 * 
 * // Look up the identity of the client
 * char *mech, *name;
 * Csec_server_getClientId(&sec_ctx, &mech, &name);
 *
 * // Once the connection is done, clear the context
 * Csec_clearContext(&sec_ctx);
 * \endcode
 *
 * @section Common Common functions
 * 
 * The error number is stored in the traditional CASTOR serrno.
 * Csec_seterrrbuf allows to specify an error buffer where the error messages
 * are written. If none is specified, then the error messages are sent to stderr.
 *
 * the Csec_getErrorMessage function allows to retrieve the detailed error
 * message from that buffer.
 * 
 */

#ifndef _CSEC_API_H
#define _CSEC_API_H

#include <osdep.h>
#include <sys/types.h>
#include <Csec_struct.h>

/** \addtogroup functions */
/*\@{*/

/**
 * Csec_client_initContext
 * Initializes a Csec_context_t for a client.
 * \ingroup functions
 * @param ctx A pointer to the security context
 * @param service_type The server service type, used to determine which 
 *                     credentials the server should have.
 * @param protocols The list of protocols to be tried by the client. If NULL
 *                  the client will use the default or will take it from the \
 *		    environment.
 */
EXTERN_C int DLL_DECL  Csec_client_initContext _PROTO ((Csec_context_t *ctx,
					       int service_type,
					       Csec_protocol_t *protocols));
/**
 * Csec_server_initContext
 * Initializes a Csec_context_t for a server.
 * \ingroup functions
 * @param ctx A pointer to the security context
 * @param service_type The server service type, used to determine which 
 *                     credentials the server should have.
 * @param protocols The list of protocols to be tried by the client. If NULL
 *                  the client will use the default or will take it from the \
 *		    environment.
 */
EXTERN_C int DLL_DECL  Csec_server_initContext _PROTO ((Csec_context_t *ctx,
					       int service_type,
					       Csec_protocol_t *protocols));

/**
 * Csec_server_reinitContext
 * First clears the context, then nitializes a Csec_context_t for a server.
 * \ingroup functions
 * @param ctx A pointer to the security context
 * @param service_type The server service type, used to determine which 
 *                     credentials the server should have.
 * @param protocols The list of protocols to be tried by the client. If NULL
 *                  the client will use the default or will take it from the \
 *		    environment.
 */
EXTERN_C int DLL_DECL  Csec_server_reinitContext  _PROTO ((Csec_context_t *ctx,
						  int service_type,
						  Csec_protocol_t *protocols));

/**
 * Csec_clearContext
 * Clear a context structure.
 * \ingroup functions
 * @param ctx A pointer to the security context
 */
EXTERN_C int DLL_DECL  Csec_clearContext _PROTO ((Csec_context_t *ctx));


/**
 * Csec_server_establishContext
 * Establishes the connection context and performs the mutual authentication
 * between the client and the server..
 * \ingroup functions
 * @param ctx A pointer to the security context
 * @param socket The connected socket over which the messages should be sent
 */
EXTERN_C int DLL_DECL  Csec_server_establishContext _PROTO ((Csec_context_t *ctx,
							     int socket));

/**
 * Csec_client_establishContext
 * Establishes the connection context and performs the mutual authentication
 * between the client and the server..
 * \ingroup functions
 * @param ctx A pointer to the security context
 * @param socket The connected socket over which the messages should be sent
 */
EXTERN_C int DLL_DECL  Csec_client_establishContext _PROTO ((Csec_context_t *ctx,
						    int socket));


/**
 * Csec_getErrorMessage
 * Returns the detailed error message concerning the last Csec error
 * (per thread variable)
 * \ingroup functions
 */
char *DLL_DECL Csec_getErrorMessage _PROTO(());


/**
 * Csec_geterrmsg
 * Returns the detailed error message concerning the last Csec error
 * (per thread variable)
 * \ingroup functions
 */
char *DLL_DECL Csec_geterrmsg _PROTO(());


/**
 * Csec_geterrmsg
 * Returns the detailed error message concerning the last Csec error
 * (per thread variable)
 * \ingroup functions
 */
char *DLL_DECL Csec_geterrmsg _PROTO(());


/**
 * Csec_seterrbuff
 * Sets the per thread error buffer
 * @param buffer A pointer to the buffer
 * @param buflen The length of the allocated buffer
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_seterrbuf _PROTO((char *buffer, int buflen));



/**
 * Csec_client_setSecurityOpts
 * Sets the options on the Csec context (e.g. whether delegation should be enabled)
 * @param ctx A pointer to the connection context
 * @param option integer with the proper flags set (CSEC_OPT_DELEG_FLAG for example)
 * @note The options should be set BEFORE the context is established.
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_client_setSecurityOpts _PROTO((Csec_context_t *ctx, 
						 int option));

/**
 * Csec_server_setSecurityOpts
 * Sets the options on the Csec context (e.g. whether delegation should be enabled)
 * @param ctx A pointer to the connection context
 * @param option integer with the proper flags set (CSEC_OPT_DELEG_FLAG for example)
 * @note The options should be set BEFORE the context is established.
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_setSecurityOpts _PROTO((Csec_context_t *ctx, 
						 int option));


/**
 * Csec_client_setAuthorizationId
 * Sets the Authorization ID. This ID is then passed the server
 * which can then choose to trust that values according to the real identity
 * of the client. (this is useful for trusted services impersonating clients).
 * @param ctx A pointer to the connection context
 * @param mech The mechanism name (e.g.GSI/KRB5)
 * @param name The identity
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_client_setAuthorizationId _PROTO((Csec_context_t *ctx, 
							     const char *mech, 
							     const char *name));

/**
 * Csec_servers_getAuthorizationId
 * Gets the Authorization ID specified by the client
 * @param ctx A pointer to the connection context
 * @param mech The mechanism name (e.g.GSI/KRB5)
 * @param name The identity
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_getAuthorizationId _PROTO((Csec_context_t *ctx, 
							     char **mech, 
							     char **name));


/**
 * Csec_server_getDelegatedCredential
 * Exports the credentials delegated by the client to a buffer
 * @param ctx A pointer to the connection context
 * @param mech The mechanism name (GSI/KRB5)
 * @param buffer pointer to the buffer where the delegated credentials are exported
 * @param buffer_size pointer to an integer where to store the exported credentials size
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_getDelegatedCredential _PROTO((Csec_context_t *ctx, 
								 char **mech, 
								 void **buffer, 
								 size_t *buffer_size));

/**
 * Csec_server_getClientId
 * Returns the identity of the client
 * @param ctx The security context
 * @param mech The mechanism that was used for authenticating the client
 * @param name The client name, under the chosen mechanism
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_getClientId _PROTO ((Csec_context_t *ctx, char **mech, char **name));


/**
 * Csec_isIdAService
 * Checks whether an identity corresponds to a service
 * @param mech The mechanism that was used for authenticating the client
 * @param name The client name, under the chosen mechanism
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_isIdAService _PROTO ((const char *mech, const char *name));


/**
 * Csec_server_isClientAService
 * Checks whether the client with which the context has been established
 * is indeed a service.
 * @param context The established connection context
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_isClientAService _PROTO ((Csec_context_t *ctx));


/**
 * Csec_mapToLocalUser
 * Maps user credential to a local username/uid/gid
 * @param mech The mechanism that was used for authenticating the client
 * @param name The client name, under the chosen mechanism
 * @param username_buf A buffer where to copy the username
 * @param buffsize The length of the mentioned buffer
 * @param uid the uid to which the username is mapped
 * @param gid the gid to which the username is mapped
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_mapToLocalUser _PROTO ((const char *mech, 
						   const char *name, 
						   char *username_buf, 
						   size_t buffsize, 
						   uid_t *uid, 
						   gid_t *gid));


/**
 * Csec_server_mapClientToLocalUser
 * Maps the client credentials to a local username/uid/gid
 * @param ctx The established connection context
 * @param username_buf A buffer where to copy the username
 * @param buffsize The length of the mentioned buffer
 * @param uid the uid to which the username is mapped
 * @param gid the gid to which the username is mapped
 * \ingroup functions
 */
EXTERN_C int DLL_DECL Csec_server_mapClientToLocalUser _PROTO ((Csec_context_t *ctx, 
								char **username, 
								uid_t *uid, 
								gid_t *gid));

/*\@}*/

#endif /*  _CSEC_API_H */
