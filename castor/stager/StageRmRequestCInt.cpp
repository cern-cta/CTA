// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageRmRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_create(castor::stager::StageRmRequest** obj) {
    *obj = new castor::stager::StageRmRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_delete(castor::stager::StageRmRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageRmRequest_getRequest(castor::stager::StageRmRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageRmRequest* Cstager_StageRmRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageRmRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_print(castor::stager::StageRmRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_TYPE(int* ret) {
    *ret = castor::stager::StageRmRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_setId(castor::stager::StageRmRequest* instance,
                                   u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_id(castor::stager::StageRmRequest* instance,
                                u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageRmRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageRmRequest_type(castor::stager::StageRmRequest* instance,
                                  int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
