#ifndef CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H
#define CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StagePrepareToPutRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePrepareToPutRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePrepareToPutRequest_create(struct Cstager_StagePrepareToPutRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePrepareToPutRequest_delete(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePrepareToPutRequest_getRequest(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePrepareToPutRequest_t* Cstager_StagePrepareToPutRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePrepareToPutRequest_print(struct Cstager_StagePrepareToPutRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePrepareToPutRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StagePrepareToPutRequest_setId(struct Cstager_StagePrepareToPutRequest_t* instance,
                                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePrepareToPutRequest_id(struct Cstager_StagePrepareToPutRequest_t* instance,
                                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePrepareToPutRequest_type(struct Cstager_StagePrepareToPutRequest_t* instance,
                                          int* ret);

#endif // CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H
