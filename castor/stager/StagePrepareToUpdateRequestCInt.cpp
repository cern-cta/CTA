// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_create(castor::stager::StagePrepareToUpdateRequest** obj) {
    *obj = new castor::stager::StagePrepareToUpdateRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_delete(castor::stager::StagePrepareToUpdateRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToUpdateRequest_getRequest(castor::stager::StagePrepareToUpdateRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToUpdateRequest* Cstager_StagePrepareToUpdateRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToUpdateRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_print(castor::stager::StagePrepareToUpdateRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToUpdateRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_setId(castor::stager::StagePrepareToUpdateRequest* instance,
                                                u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_id(castor::stager::StagePrepareToUpdateRequest* instance,
                                             u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToUpdateRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToUpdateRequest_type(castor::stager::StagePrepareToUpdateRequest* instance,
                                               int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
