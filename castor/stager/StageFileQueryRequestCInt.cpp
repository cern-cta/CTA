// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_create(castor::stager::StageFileQueryRequest** obj) {
    *obj = new castor::stager::StageFileQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_delete(castor::stager::StageFileQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageFileQueryRequest_getRequest(castor::stager::StageFileQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageFileQueryRequest* Cstager_StageFileQueryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageFileQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_print(castor::stager::StageFileQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageFileQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_setId(castor::stager::StageFileQueryRequest* instance,
                                          u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_id(castor::stager::StageFileQueryRequest* instance,
                                       u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageFileQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageFileQueryRequest_type(castor::stager::StageFileQueryRequest* instance,
                                         int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
