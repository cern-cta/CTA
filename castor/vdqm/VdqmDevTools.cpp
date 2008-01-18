#include "castor/vdqm/VdqmDevTools.hpp"
#include "h/vdqm_constants.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmDevTools::VdqmDevTools() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// printVdqmRequestType
//------------------------------------------------------------------------------
void castor::vdqm::VdqmDevTools::printVdqmRequestType(std::ostream &os,
  const int type) throw() {

  switch(type) {
  case VDQM_VOL_REQ     : os << "VDQM_VOL_REQ";      break;
  case VDQM_DRV_REQ     : os << "VDQM_DRV_REQ";      break;
  case VDQM_PING        : os << "VDQM_PING";         break;
  case VDQM_CLIENTINFO  : os << "VDQM_CLIENTINFO";   break;
  case VDQM_SHUTDOWN    : os << "VDQM_SHUTDOWN";     break;
  case VDQM_HOLD        : os << "VDQM_HOLD";         break;
  case VDQM_RELEASE     : os << "VDQM_RELEASE";      break;
  case VDQM_REPLICA     : os << "VDQM_REPLICA";      break;
  case VDQM_RESCHEDULE  : os << "VDQM_RESCHEDULE";   break;
  case VDQM_ROLLBACK    : os << "VDQM_ROLLBACK";     break;
  case VDQM_COMMIT      : os << "VDQM_COMMIT";       break;
  case VDQM_DEL_VOLREQ  : os << "VDQM_DEL_VOLREQ";   break;
  case VDQM_DEL_DRVREQ  : os << "VDQM_DEL_DRVREQ";   break;
  case VDQM_GET_VOLQUEUE: os << "VDQM_GET_VOLQUEUE"; break;
  case VDQM_GET_DRVQUEUE: os << "VDQM_GET_DRVQUEUE"; break;
  case VDQM_SET_PRIORITY: os << "VDQM_SET_PRIORITY"; break;
  case VDQM_DEDICATE_DRV: os << "VDQM_DEDICATE_DRV"; break;
  case VDQM_HANGUP      : os << "VDQM_HANGUP";       break;
  default               : os << "UNKNOWN";
  }
}
