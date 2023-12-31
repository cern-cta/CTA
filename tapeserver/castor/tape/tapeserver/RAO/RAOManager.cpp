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


#include "RAOManager.hpp"
#include "EnterpriseRAOAlgorithm.hpp"
#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"
#include "RAOAlgorithmFactoryFactory.hpp"
#include "catalogue/Catalogue.hpp"
#include "LinearRAOAlgorithm.hpp"
#include "common/Timer.hpp"

namespace castor::tape::tapeserver::rao {

bool RAOManager::useRAO() const{
  return m_raoParams.useRAO();
}

void RAOManager::disableRAO() {
  m_raoParams.disableRAO();
}

void RAOManager::setEnterpriseRAOUdsLimits(const SCSI::Structures::RAO::udsLimits& raoLimits) {
  m_enterpriseRaoLimits = raoLimits;
  m_maxFilesSupported = raoLimits.maxSupported;
  m_hasUDS = true;
  m_isDriveEnterpriseEnabled = true;
}

std::vector<uint64_t> RAOManager::queryRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs, cta::log::LogContext & lc){
  cta::utils::Timer totalTimer;
  RAOAlgorithmFactoryFactory raoAlgoFactoryFactory(*this,lc);
  std::unique_ptr<RAOAlgorithmFactory> raoAlgoFactory = raoAlgoFactoryFactory.createAlgorithmFactory();
  std::unique_ptr<RAOAlgorithm> raoAlgo;
  std::vector<uint64_t> ret;
  try {
    raoAlgo = raoAlgoFactory->createRAOAlgorithm();
  } catch(const cta::exception::Exception & ex){
    this->logWarningAfterRAOOperationFailed("In RAOManager::queryRAO(), failed to instanciate the RAO algorithm, will perform a linear RAO.",ex.getMessageValue(),lc);
    raoAlgo = raoAlgoFactory->createDefaultLinearAlgorithm();
  }
  try {
    ret = raoAlgo->performRAO(jobs);
  } catch (const cta::exception::Exception & ex) {
    this->logWarningAfterRAOOperationFailed("In RAOManager::queryRAO(), failed to perform the RAO algorithm, will perform a linear RAO.",ex.getMessageValue(),lc);
    raoAlgo = raoAlgoFactory->createDefaultLinearAlgorithm();
    ret = raoAlgo->performRAO(jobs);
  } catch(const std::exception &ex2){
    this->logWarningAfterRAOOperationFailed("In RAOManager::queryRAO(), failed to perform the RAO algorithm after a standard exception, will perform a linear RAO.",std::string(ex2.what()),lc);
    raoAlgo = raoAlgoFactory->createDefaultLinearAlgorithm();
    ret = raoAlgo->performRAO(jobs);
  }
  cta::log::ScopedParamContainer spc(lc);
  spc.add("executedRAOAlgorithm",raoAlgo->getName());
  cta::log::TimingList raoTimingList = raoAlgo->getRAOTimings();
  raoTimingList.insertAndReset("totalTime",totalTimer);
  raoTimingList.addToLog(spc);
  lc.log(cta::log::INFO, "In RAOManager::queryRAO(), successfully performed RAO.");
  return ret;
}

void RAOManager::logWarningAfterRAOOperationFailed(const std::string & warningMsg, const std::string & exceptionMsg, cta::log::LogContext & lc) const{
  cta::log::ScopedParamContainer spc(lc);
  spc.add("errorMsg",exceptionMsg)
     .add("raoAlgorithmName",m_raoParams.getRAOAlgorithmName())
     .add("raoAlgorithmOptions",m_raoParams.getRAOAlgorithmOptions().getOptionsString())
     .add("useRAO",m_raoParams.useRAO())
     .add("vid",m_raoParams.getMountedVid());
  lc.log(cta::log::WARNING,warningMsg);
}

} // namespace castor::tape::tapeserver::rao
