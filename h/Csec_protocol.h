#ifndef _CSEC_PROTOCOL_H
#define _CSEC_PROTOCOL_H

#include <marshall.h>
#include <Csec_plugin_loader.h>
#include <Csec_common.h>

/**
 * The Csec protocol is used to negociate the mechanism used
 * for authentication between the client and server. 
 * It is very simple and consists of one exchange of messages between 
 * the client and server: the client proposes a mechanism, and options
 * and the server chooses.
 */



/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_client_buildRequestMessage(Csec_context_t *ctx,
				     csec_buffer_t request);


/**
 * Parse the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_server_parseRequestMessage(Csec_context_t *ctx,
				     csec_buffer_t request);


/**
 * Checks the request from the client and 
 * XXX Add documentation concerning the algorithm
 * of the negociation on client side and server side !
 */
int _Csec_server_checkClientRequest(Csec_context_t *ctx,
				    int *negociationStatus,
				    Csec_protocol_t **chosenProtocol,
				    int *acceptedOptions);

/**
 * Builds the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_server_buildResponseMessage(Csec_context_t *ctx,
				      csec_buffer_t response,
				      int negociationStatus,
				      Csec_protocol_t *chosenProtocol,
				      int acceptedOptions);

/**
 * Parse the message containing the request from the client for the use
 * of a given protocol with given options.
 */
int _Csec_client_parseResponseMessage(Csec_context_t *ctx,
				      csec_buffer_t response);

/**
 * Checks the response from the server
 * XXX Add documentation concerning the algorithm
 * of the negociation on client side and server side !
 */
int _Csec_client_checkServerRequest(Csec_context_t *ctx,
				    int *negociationStatus,
				    Csec_protocol_t **chosenProtocol,
				    int *acceptedOptions);


/**
 * Negociate the protocol with the client !
 */
int   _Csec_server_negociateProtocol (Csec_context_t *ctx,
				      int socket);


/**
 * Negociate the protocol with the server !
 */
int   _Csec_client_negociateProtocol (Csec_context_t *ctx,
				     int socket);



#endif /* _CSEC_PROTOCOL_H */
