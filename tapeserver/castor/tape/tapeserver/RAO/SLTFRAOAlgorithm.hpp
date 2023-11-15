/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "RAOAlgorithm.hpp"
#include "RAOParams.hpp"
#include "CostHeuristic.hpp"
#include "FilePositionEstimator.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "RAOFile.hpp"

#include <map>

namespace castor::tape::tapeserver::rao {

class SLTFRAOAlgorithm : public RAOAlgorithm {
public:
  /**
   * Constructor of a ShortLocateTimeFirst RAO Algorithm
   * @param filePositionEstimator the file position estimator to determine the position of all files given to the performRAO() method
   * @param costHeuristic the cost heuristic to use to determine the cost between two files
   */
  SLTFRAOAlgorithm(std::unique_ptr<FilePositionEstimator> & filePositionEstimator, std::unique_ptr<CostHeuristic> & costHeuristic);
  /**
   * Perform the SLTF RAO algorithm on the Retrieve jobs passed in parameter
   * @param jobs the jobs to perform the SLTF RAO algorithm
   * @return the vector of the indexes of the jobs rearranged with the SLTF method
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;
  std::string getName() const override;
  virtual ~SLTFRAOAlgorithm();
  
  /**
   * This builder helps to build the SLTF RAO algorithm. It initializes the file position estimator and the cost heuristic
   * according to what are the parameters of the RAO but also by asking to the drive the drive'
   * @param data
   * @param drive
   * @param catalogue
   */
  class Builder {
  public:
    Builder(const RAOParams & data);
    void setCatalogue(cta::catalogue::Catalogue * catalogue);
    void setDrive(drive::DriveInterface * drive);
    std::unique_ptr<SLTFRAOAlgorithm> build();
  private:
    void initializeFilePositionEstimator();
    void initializeCostHeuristic();
    std::unique_ptr<SLTFRAOAlgorithm> m_algorithm;
    RAOParams m_raoParams;
    drive::DriveInterface * m_drive = nullptr;
    cta::catalogue::Catalogue * m_catalogue = nullptr;
  };
  
private:
  SLTFRAOAlgorithm();
  std::unique_ptr<FilePositionEstimator> m_filePositionEstimator;
  std::unique_ptr<CostHeuristic> m_costHeuristic;
    
  typedef std::map<uint64_t,RAOFile> RAOFilesContainer;
  
  RAOFilesContainer computeAllFilesPosition(const std::vector<std::unique_ptr<cta::RetrieveJob> > & jobs) const;
  void computeCostBetweenFileAndOthers(RAOFile & file, const RAOFilesContainer & files) const;
  std::vector<uint64_t> performSLTF(RAOFilesContainer & files) const;
  std::unique_ptr<cta::RetrieveJob> createFakeRetrieveJobForFileAtBeginningOfTape() const;
};

} // namespace castor::tape::tapeserver::rao

