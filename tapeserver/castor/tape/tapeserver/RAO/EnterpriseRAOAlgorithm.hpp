/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "RAOAlgorithm.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "EnterpriseRAOAlgorithmFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class EnterpriseRAOAlgorithmFactory;
  
/**
 * This class represents an EnterpriseRAOAlgorithm. 
 */
class EnterpriseRAOAlgorithm : public RAOAlgorithm{
public:
  friend EnterpriseRAOAlgorithmFactory;
  
  virtual ~EnterpriseRAOAlgorithm();

  /**
   * Asks the Enteprise drive to perform a RAO query in order to get the RAO of the 
   * files represented by the jobs passed in parameter
   * @param jobs the jobs representing the files we want to perform the RAO on
   * @return the vector of the indexes of the jobs passed in parameters rearranged by the RAO query
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs) override;
  
  std::string getName() const override;
  
private:
  /**
   * Constructs an EnterpriseRAOAlgorithm
   * @param drive the drive in order to call its RAO via its DriveInterface
   * @param maxFilesSupported the maximum number of files supported by the drive to perform the RAO
   */
  EnterpriseRAOAlgorithm(castor::tape::tapeserver::drive::DriveInterface * drive, const uint64_t maxFilesSupported);
  
  //Interface to the drive
  castor::tape::tapeserver::drive::DriveInterface * m_drive;
  //Maximum number of files supported by the drive to perform the RAO
  uint64_t m_maxFilesSupported;
  
  const uint32_t c_blockSize = 262144;
};

}}}}
