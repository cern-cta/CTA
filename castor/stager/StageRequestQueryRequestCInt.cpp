// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_create(castor::stager::StageRequestQueryRequest** obj) {
    *obj = new castor::stager::StageRequestQueryRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_delete(castor::stager::StageRequestQueryRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageRequestQueryRequest_getRequest(castor::stager::StageRequestQueryRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRequestQueryRequest* Cstager_StageRequestQueryRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageRequestQueryRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_print(castor::stager::StageRequestQueryRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_TYPE(int* ret) {
    *ret = castor::stager::StageRequestQueryRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_setId(castor::stager::StageRequestQueryRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_id(castor::stager::StageRequestQueryRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRequestQueryRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageRequestQueryRequest_type(castor::stager::StageRequestQueryRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
