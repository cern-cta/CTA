// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_create(castor::stager::StagePutDoneRequest** obj) {
    *obj = new castor::stager::StagePutDoneRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_delete(castor::stager::StagePutDoneRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePutDoneRequest_getRequest(castor::stager::StagePutDoneRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePutDoneRequest* Cstager_StagePutDoneRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePutDoneRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_print(castor::stager::StagePutDoneRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePutDoneRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_setId(castor::stager::StagePutDoneRequest* instance,
                                        u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_id(castor::stager::StagePutDoneRequest* instance,
                                     u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePutDoneRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePutDoneRequest_type(castor::stager::StagePutDoneRequest* instance,
                                       int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
