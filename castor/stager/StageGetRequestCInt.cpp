// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_create(castor::stager::StageGetRequest** obj) {
    *obj = new castor::stager::StageGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_delete(castor::stager::StageGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StageGetRequest_getRequest(castor::stager::StageGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StageGetRequest* Cstager_StageGetRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StageGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_print(castor::stager::StageGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StageGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_setId(castor::stager::StageGetRequest* instance,
                                    u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_id(castor::stager::StageGetRequest* instance,
                                 u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StageGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StageGetRequest_type(castor::stager::StageGetRequest* instance,
                                   int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
