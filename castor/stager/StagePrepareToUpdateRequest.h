#ifndef CASTOR_STAGER_STAGEPREPARETOUPDATEREQUEST_H
#define CASTOR_STAGER_STAGEPREPARETOUPDATEREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StagePrepareToUpdateRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePrepareToUpdateRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePrepareToUpdateRequest_create(struct Cstager_StagePrepareToUpdateRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePrepareToUpdateRequest_delete(struct Cstager_StagePrepareToUpdateRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePrepareToUpdateRequest_getRequest(struct Cstager_StagePrepareToUpdateRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePrepareToUpdateRequest_t* Cstager_StagePrepareToUpdateRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePrepareToUpdateRequest_print(struct Cstager_StagePrepareToUpdateRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePrepareToUpdateRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StagePrepareToUpdateRequest_setId(struct Cstager_StagePrepareToUpdateRequest_t* instance,
                                              u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePrepareToUpdateRequest_id(struct Cstager_StagePrepareToUpdateRequest_t* instance,
                                           u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePrepareToUpdateRequest_type(struct Cstager_StagePrepareToUpdateRequest_t* instance,
                                             int* ret);

#endif // CASTOR_STAGER_STAGEPREPARETOUPDATEREQUEST_H
