#ifndef CASTOR_STAGER_STAGEPUTDONEREQUEST_H
#define CASTOR_STAGER_STAGEPUTDONEREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StagePutDoneRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePutDoneRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePutDoneRequest_create(struct Cstager_StagePutDoneRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePutDoneRequest_delete(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePutDoneRequest_getRequest(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePutDoneRequest_t* Cstager_StagePutDoneRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePutDoneRequest_print(struct Cstager_StagePutDoneRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePutDoneRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StagePutDoneRequest_setId(struct Cstager_StagePutDoneRequest_t* instance,
                                      u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePutDoneRequest_id(struct Cstager_StagePutDoneRequest_t* instance,
                                   u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePutDoneRequest_type(struct Cstager_StagePutDoneRequest_t* instance,
                                     int* ret);

#endif // CASTOR_STAGER_STAGEPUTDONEREQUEST_H
