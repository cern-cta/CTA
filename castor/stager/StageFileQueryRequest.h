#ifndef CASTOR_STAGER_STAGEFILEQUERYREQUEST_H
#define CASTOR_STAGER_STAGEFILEQUERYREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageFileQueryRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageFileQueryRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageFileQueryRequest_create(struct Cstager_StageFileQueryRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageFileQueryRequest_delete(struct Cstager_StageFileQueryRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageFileQueryRequest_getRequest(struct Cstager_StageFileQueryRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageFileQueryRequest_t* Cstager_StageFileQueryRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageFileQueryRequest_print(struct Cstager_StageFileQueryRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageFileQueryRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageFileQueryRequest_setId(struct Cstager_StageFileQueryRequest_t* instance,
                                        u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageFileQueryRequest_id(struct Cstager_StageFileQueryRequest_t* instance,
                                     u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageFileQueryRequest_type(struct Cstager_StageFileQueryRequest_t* instance,
                                       int* ret);

#endif // CASTOR_STAGER_STAGEFILEQUERYREQUEST_H
