#ifndef CASTOR_STAGER_STAGEFINDREQUESTREQUEST_H
#define CASTOR_STAGER_STAGEFINDREQUESTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageFindRequestRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageFindRequestRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageFindRequestRequest_create(struct Cstager_StageFindRequestRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageFindRequestRequest_delete(struct Cstager_StageFindRequestRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageFindRequestRequest_getRequest(struct Cstager_StageFindRequestRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageFindRequestRequest_t* Cstager_StageFindRequestRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageFindRequestRequest_print(struct Cstager_StageFindRequestRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageFindRequestRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageFindRequestRequest_setId(struct Cstager_StageFindRequestRequest_t* instance,
                                          u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageFindRequestRequest_id(struct Cstager_StageFindRequestRequest_t* instance,
                                       u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageFindRequestRequest_type(struct Cstager_StageFindRequestRequest_t* instance,
                                         int* ret);

#endif // CASTOR_STAGER_STAGEFINDREQUESTREQUEST_H
