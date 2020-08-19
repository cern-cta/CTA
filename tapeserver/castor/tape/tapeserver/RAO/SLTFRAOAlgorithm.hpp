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

#pragma once

#include "RAOAlgorithm.hpp"
#include "RAOConfigurationData.hpp"
#include "CostHeuristic.hpp"
#include "FilePositionEstimator.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
class SLTFRAOAlgorithm : public RAOAlgorithm {
public:
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;
  virtual ~SLTFRAOAlgorithm();
  
  class Builder {
  public:
    Builder(const RAOConfigurationData & data, drive::DriveInterface * drive, cta::catalogue::Catalogue * catalogue);
    std::unique_ptr<SLTFRAOAlgorithm> build();
  private:
    void initializeFilePositionEstimator();
    void initializeCostHeuristic();
    std::unique_ptr<SLTFRAOAlgorithm> m_algorithm;
    RAOConfigurationData m_data;
    drive::DriveInterface * m_drive;
    cta::catalogue::Catalogue * m_catalogue;
  };
  
private:
  SLTFRAOAlgorithm();
  std::unique_ptr<FilePositionEstimator> m_filePositionEstimator;
  std::unique_ptr<CostHeuristic> m_costHeuristic;
};

}}}}

