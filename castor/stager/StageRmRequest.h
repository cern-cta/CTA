#ifndef CASTOR_STAGER_STAGERMREQUEST_H
#define CASTOR_STAGER_STAGERMREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageRmRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageRmRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageRmRequest_create(struct Cstager_StageRmRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageRmRequest_delete(struct Cstager_StageRmRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageRmRequest_getRequest(struct Cstager_StageRmRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageRmRequest_t* Cstager_StageRmRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageRmRequest_print(struct Cstager_StageRmRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageRmRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageRmRequest_setId(struct Cstager_StageRmRequest_t* instance,
                                 u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageRmRequest_id(struct Cstager_StageRmRequest_t* instance,
                              u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageRmRequest_type(struct Cstager_StageRmRequest_t* instance,
                                int* ret);

#endif // CASTOR_STAGER_STAGERMREQUEST_H
