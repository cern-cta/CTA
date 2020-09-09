/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "EnterpriseRAOAlgorithm.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
EnterpriseRAOAlgorithmFactory::EnterpriseRAOAlgorithmFactory(castor::tape::tapeserver::drive::DriveInterface * drive, const uint64_t maxFilesSupported):
m_drive(drive), m_maxFilesSupported(maxFilesSupported) {
}

EnterpriseRAOAlgorithmFactory::~EnterpriseRAOAlgorithmFactory() {
}

std::unique_ptr<RAOAlgorithm> EnterpriseRAOAlgorithmFactory::createRAOAlgorithm() {
  //For now we only have the EnterpriseRAOAlgorithm, but we could maybe have another one,
  //this method should be modified consequently
  return std::unique_ptr<RAOAlgorithm>(new EnterpriseRAOAlgorithm(m_drive,m_maxFilesSupported));
}


}}}}