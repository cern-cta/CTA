#ifndef CASTOR_STAGER_STAGEPUTREQUEST_H
#define CASTOR_STAGER_STAGEPUTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StagePutRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePutRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePutRequest_create(struct Cstager_StagePutRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePutRequest_delete(struct Cstager_StagePutRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePutRequest_getRequest(struct Cstager_StagePutRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePutRequest_t* Cstager_StagePutRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePutRequest_print(struct Cstager_StagePutRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePutRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StagePutRequest_setId(struct Cstager_StagePutRequest_t* instance,
                                  u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePutRequest_id(struct Cstager_StagePutRequest_t* instance,
                               u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePutRequest_type(struct Cstager_StagePutRequest_t* instance,
                                 int* ret);

#endif // CASTOR_STAGER_STAGEPUTREQUEST_H
