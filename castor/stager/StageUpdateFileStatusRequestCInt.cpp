// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageUpdateFileStatusRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_create(castor::stager::StageUpdateFileStatusRequest** obj) {
    *obj = new castor::stager::StageUpdateFileStatusRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_delete(castor::stager::StageUpdateFileStatusRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageUpdateFileStatusRequest_getRequest(castor::stager::StageUpdateFileStatusRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageUpdateFileStatusRequest* Cstager_StageUpdateFileStatusRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageUpdateFileStatusRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_print(castor::stager::StageUpdateFileStatusRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_TYPE(int* ret) {
    *ret = castor::stager::StageUpdateFileStatusRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_setId(castor::stager::StageUpdateFileStatusRequest* instance,
                                                 u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_id(castor::stager::StageUpdateFileStatusRequest* instance,
                                              u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageUpdateFileStatusRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageUpdateFileStatusRequest_type(castor::stager::StageUpdateFileStatusRequest* instance,
                                                int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
