// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageUpdateRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_create(castor::stager::StageUpdateRequest** obj) {
    *obj = new castor::stager::StageUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_delete(castor::stager::StageUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateRequest_getRequest(castor::stager::StageUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateRequest* Cstager_StageUpdateRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_print(castor::stager::StageUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_setId(castor::stager::StageUpdateRequest* instance,
                                       u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_id(castor::stager::StageUpdateRequest* instance,
                                    u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateRequest_type(castor::stager::StageUpdateRequest* instance,
                                      int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
