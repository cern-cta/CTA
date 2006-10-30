#ifndef REPACKTOOLS_HPP
#define REPACKTOOLS_HPP 1

#include <string>
#include "Cuuid.h"
#include "stager_client_api.h"
#include "castor/repack/RepackCommonHeader.hpp"

namespace castor {
	namespace repack {
    std::string Cuuidtostring(_Cuuid_t *cuuid);
    _Cuuid_t stringtoCuuid(std::string strcuuid);
    void free_stager_response(struct  stage_filequery_resp* resp);
    void freeRepackObj(castor::IObject* obj);
    void getStageOpts(struct stage_options* opts, RepackSubRequest* sreq)
                                       throw (castor::exception::Internal);
  }
}
#endif
