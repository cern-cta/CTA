/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "EnterpriseRAOAlgorithm.hpp"

namespace castor::tape::tapeserver::rao {

std::unique_ptr<RAOAlgorithm> EnterpriseRAOAlgorithmFactory::createRAOAlgorithm() {
  //For now we only have the EnterpriseRAOAlgorithm, but we could maybe have another one,
  //this method should be modified consequently
  return std::unique_ptr<RAOAlgorithm>(new EnterpriseRAOAlgorithm(m_drive,m_maxFilesSupported));
}

} // namespace castor::tape::tapeserver::rao
