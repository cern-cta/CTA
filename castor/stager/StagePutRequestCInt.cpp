// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePutRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_create(castor::stager::StagePutRequest** obj) {
    *obj = new castor::stager::StagePutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_delete(castor::stager::StagePutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePutRequest_getRequest(castor::stager::StagePutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutRequest* Cstager_StagePutRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_print(castor::stager::StagePutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_setId(castor::stager::StagePutRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_id(castor::stager::StagePutRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutRequest_type(castor::stager::StagePutRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
