#ifndef CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H
#define CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageRequestQueryRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageRequestQueryRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageRequestQueryRequest_create(struct Cstager_StageRequestQueryRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageRequestQueryRequest_delete(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageRequestQueryRequest_getRequest(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageRequestQueryRequest_t* Cstager_StageRequestQueryRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageRequestQueryRequest_print(struct Cstager_StageRequestQueryRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageRequestQueryRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageRequestQueryRequest_setId(struct Cstager_StageRequestQueryRequest_t* instance,
                                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageRequestQueryRequest_id(struct Cstager_StageRequestQueryRequest_t* instance,
                                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageRequestQueryRequest_type(struct Cstager_StageRequestQueryRequest_t* instance,
                                          int* ret);

#endif // CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H
