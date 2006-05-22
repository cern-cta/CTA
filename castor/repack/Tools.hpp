#ifndef REPACKTOOLS_HPP
#define REPACKTOOLS_HPP 1

#include <string>
#include "Cuuid.h"
#include "stager_client_api.h"
namespace castor {
	namespace repack {
		std::string Cuuidtostring(_Cuuid_t *cuuid);
		_Cuuid_t stringtoCuuid(std::string strcuuid);
    void free_stager_response(struct  stage_filequery_resp* resp);
	}
}
#endif
