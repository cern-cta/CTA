#ifndef CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H
#define CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageUpdateFileStatusRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageUpdateFileStatusRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageUpdateFileStatusRequest_create(struct Cstager_StageUpdateFileStatusRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageUpdateFileStatusRequest_delete(struct Cstager_StageUpdateFileStatusRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageUpdateFileStatusRequest_getRequest(struct Cstager_StageUpdateFileStatusRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageUpdateFileStatusRequest_t* Cstager_StageUpdateFileStatusRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageUpdateFileStatusRequest_print(struct Cstager_StageUpdateFileStatusRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageUpdateFileStatusRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageUpdateFileStatusRequest_setId(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                               u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageUpdateFileStatusRequest_id(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                            u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageUpdateFileStatusRequest_type(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                              int* ret);

#endif // CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H
