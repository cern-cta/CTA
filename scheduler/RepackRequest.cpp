/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "RepackRequest.hpp"

namespace cta {

const cta::common::dataStructures::RepackInfo cta::RepackRequest::getRepackInfo() const
{
  return m_dbReq->repackInfo;
}

void RepackRequest::fail() {
  m_dbReq->fail();
}


} // namespace cta

