#ifndef CASTOR_STAGER_STAGEGETREQUEST_H
#define CASTOR_STAGER_STAGEGETREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageGetRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageGetRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageGetRequest_create(struct Cstager_StageGetRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageGetRequest_delete(struct Cstager_StageGetRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageGetRequest_getRequest(struct Cstager_StageGetRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageGetRequest_t* Cstager_StageGetRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageGetRequest_print(struct Cstager_StageGetRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageGetRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageGetRequest_setId(struct Cstager_StageGetRequest_t* instance,
                                  u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageGetRequest_id(struct Cstager_StageGetRequest_t* instance,
                               u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageGetRequest_type(struct Cstager_StageGetRequest_t* instance,
                                 int* ret);

#endif // CASTOR_STAGER_STAGEGETREQUEST_H
