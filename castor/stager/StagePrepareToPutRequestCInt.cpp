// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_create(castor::stager::StagePrepareToPutRequest** obj) {
    *obj = new castor::stager::StagePrepareToPutRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_delete(castor::stager::StagePrepareToPutRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToPutRequest_getRequest(castor::stager::StagePrepareToPutRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToPutRequest* Cstager_StagePrepareToPutRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToPutRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_print(castor::stager::StagePrepareToPutRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToPutRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_setId(castor::stager::StagePrepareToPutRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_id(castor::stager::StagePrepareToPutRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToPutRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToPutRequest_type(castor::stager::StagePrepareToPutRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
