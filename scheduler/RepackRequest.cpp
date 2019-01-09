#include "RepackRequest.hpp"

cta::RepackRequest::RepackRequest(){}

const cta::common::dataStructures::RepackInfo cta::RepackRequest::getRepackInfo() const
{
  return m_dbReq.get()->repackInfo;
}
