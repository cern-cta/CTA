#ifndef CASTOR_STAGER_STAGEUPDATEREQUEST_H
#define CASTOR_STAGER_STAGEUPDATEREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageUpdateRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageUpdateRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageUpdateRequest_create(struct Cstager_StageUpdateRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageUpdateRequest_delete(struct Cstager_StageUpdateRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageUpdateRequest_getRequest(struct Cstager_StageUpdateRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageUpdateRequest_t* Cstager_StageUpdateRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageUpdateRequest_print(struct Cstager_StageUpdateRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageUpdateRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageUpdateRequest_setId(struct Cstager_StageUpdateRequest_t* instance,
                                     u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageUpdateRequest_id(struct Cstager_StageUpdateRequest_t* instance,
                                  u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageUpdateRequest_type(struct Cstager_StageUpdateRequest_t* instance,
                                    int* ret);

#endif // CASTOR_STAGER_STAGEUPDATEREQUEST_H
