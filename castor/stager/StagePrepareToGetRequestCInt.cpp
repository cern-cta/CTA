// Include Files
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_create
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_create(castor::stager::StagePrepareToGetRequest** obj) {
    *obj = new castor::stager::StagePrepareToGetRequest();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_delete
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_delete(castor::stager::StagePrepareToGetRequest* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_getRequest
  //----------------------------------------------------------------------------
  castor::stager::Request* Cstager_StagePrepareToGetRequest_getRequest(castor::stager::StagePrepareToGetRequest* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_fromRequest
  //----------------------------------------------------------------------------
  castor::stager::StagePrepareToGetRequest* Cstager_StagePrepareToGetRequest_fromRequest(castor::stager::Request* obj) {
    return dynamic_cast<castor::stager::StagePrepareToGetRequest*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_print
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_print(castor::stager::StagePrepareToGetRequest* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_TYPE
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_TYPE(int* ret) {
    *ret = castor::stager::StagePrepareToGetRequest::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_setId
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_setId(castor::stager::StagePrepareToGetRequest* instance,
                                             u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_id
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_id(castor::stager::StagePrepareToGetRequest* instance,
                                          u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_StagePrepareToGetRequest_type
  //----------------------------------------------------------------------------
  int Cstager_StagePrepareToGetRequest_type(castor::stager::StagePrepareToGetRequest* instance,
                                            int* ret) {
    *ret = instance->type();
    return 0;
  }

} // End of extern "C"
