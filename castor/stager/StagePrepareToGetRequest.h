#ifndef CASTOR_STAGER_STAGEPREPARETOGETREQUEST_H
#define CASTOR_STAGER_STAGEPREPARETOGETREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StagePrepareToGetRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePrepareToGetRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePrepareToGetRequest_create(struct Cstager_StagePrepareToGetRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePrepareToGetRequest_delete(struct Cstager_StagePrepareToGetRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePrepareToGetRequest_getRequest(struct Cstager_StagePrepareToGetRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePrepareToGetRequest_t* Cstager_StagePrepareToGetRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePrepareToGetRequest_print(struct Cstager_StagePrepareToGetRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePrepareToGetRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StagePrepareToGetRequest_setId(struct Cstager_StagePrepareToGetRequest_t* instance,
                                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePrepareToGetRequest_id(struct Cstager_StagePrepareToGetRequest_t* instance,
                                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePrepareToGetRequest_type(struct Cstager_StagePrepareToGetRequest_t* instance,
                                          int* ret);

#endif // CASTOR_STAGER_STAGEPREPARETOGETREQUEST_H
