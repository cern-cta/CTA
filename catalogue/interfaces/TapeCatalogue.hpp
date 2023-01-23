/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <map>
#include <optional>
#include <set>
#include <string>

#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/VidToTapeMap.hpp"
#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct SecurityIdentity;
struct Tape;
}
}

namespace log {
class LogContext;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVendor);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVid);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyTape);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTape);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTapeState);

class CreateTapeAttributes;
class TapeForWriting;

class TapeCatalogue {
public:
  virtual ~TapeCatalogue() = default;

  /**
   * Creates a tape which is assumed to have isFromCastor disabled.
   *
   * @param admin The administrator.
   * @param tape The attributes of the tape to be created.
   */
  virtual void createTape(const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes &tape) = 0;

  virtual void deleteTape(const std::string &vid) = 0;

  /**
   * Returns the list of tapes that meet the specified search criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The list of tapes.
   * @throw UserSpecifiedANonExistentTapePool if the user specified a
   * non-existent tape pool.
   */
  virtual std::list<common::dataStructures::Tape> getTapes(
    const TapeSearchCriteria &searchCriteria = TapeSearchCriteria()) const = 0;

  /**
   * Returns the tape with the specified volume identifier.
   *
   * This method will throw an exception if it cannot find the specified tape.
   *
   * @param vids The tape volume identifier (VID).
   * @return Map from tape volume identifier to tape.
   */
  virtual common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const = 0;

  /**
   * Returns the tapes with the specified volume identifiers.
   *
   * This method will throw an exception if it cannot find ALL of the specified
   * tapes.
   *
   * @param vids The tape volume identifiers (VIDs).
   * @return Map from tape volume identifier to tape.
   */
  virtual common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const = 0;

  /**
   * Returns map from VID to logical library name for specified set of VIDs.
   *
   * @param vids The tape volume identifiers (VIDs).
   * @return map from VID to logical library name.
   */
  virtual std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const = 0;

  /**
   * Reclaims the specified tape.
   *
   * This method will throw an exception if the specified tape does not exist.
   *
   * This method will throw an exception if the specified tape is not FULL.
   *
   * This method will throw an exception if there is still at least one tape
   * file recorded in the catalogue as being on the specified tape.
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be reclaimed.
   * @param lc the logContext
   */
  virtual void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    cta::log::LogContext & lc) = 0;

  /**
   * Checks the specified tape for the tape label command.
   *
   * This method checks if the tape is safe to be labeled and will throw an
   * exception if the specified tape does not ready to be labeled.
   *
   * @param vid The volume identifier of the tape to be checked.
   */
  virtual void checkTapeForLabel(const std::string &vid) = 0;

  /**
   * Notifies the catalogue that the specified tape was labelled.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of tape drive that was used to label the tape.
   */
  virtual void tapeLabelled(const std::string &vid, const std::string &drive) = 0;

  /**
   * Returns the number of any files contained in the tape identified by its vid
   * @param vid the vid in which we will the number of files
   * @return the number of files on the tape
   */
  virtual uint64_t getNbFilesOnTape(const std::string &vid) const = 0;

  virtual void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &mediaType) = 0;

  virtual void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &vendor) = 0;

  virtual void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &vid, const std::string &logicalLibraryName) = 0;

  virtual void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string &tapePoolName) = 0;

  virtual void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &vid, const std::string &encryptionKeyName) = 0;

  virtual void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin,
    const std::string &vid, const std::string &verificationStatus) = 0;

  /**
   * Returns true if the specified tape exists.
   *
   * @param vid The volume identifier of the tape.
   * @return True if the tape exists.
   */
  virtual bool tapeExists(const std::string &vid) const = 0;

  /**
   * Modify the state of the specified tape
   * @param admin, the person or the system who modified the state of the tape
   * @param vid the VID of the tape to change the state
   * @param state the new state
   * @param stateReason the reason why the state changes, if the state is ACTIVE and the stateReason is std::nullopt, the state will be reset to null
   */
  virtual void modifyTapeState(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const common::dataStructures::Tape::State & state,
    const std::optional<common::dataStructures::Tape::State> & prev_state,
    const std::optional<std::string> & stateReason) = 0;
  /**
   * Sets the full status of the specified tape.
   *
   * Please note that this method is to be called by the CTA front-end in
   * response to a command from the CTA command-line interface (CLI).
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be marked as full.
   * @param fullValue Set to true if the tape is full.
   */
  virtual void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const bool fullValue) = 0;

  /**
   * Sets the dirty status of the specified tape.
   *
   * Please note that this method is to be called by the CTA front-end in
   * response to a command from the CTA command-line interface (CLI).
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be marked as full.
   * @param dirty Set to true if the tape is dirty.
   */
  virtual void setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const bool dirty) = 0;

  /**
   * This method notifies the CTA catalogue to set the specified tape is from CASTOR.
   * This method only for unitTests and MUST never be called in CTA!!!
   *
   * @param vid The volume identifier of the tape.
   */
  virtual void setTapeIsFromCastorInUnitTests(const std::string &vid) = 0;

  virtual void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string & reason) = 0;

  virtual void setTapeRepackingDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::string & reason) = 0;

  virtual void setTapeDirty(const std::string & vid) = 0;

  /**
   * Modifies the tape comment
   * If the comment == std::nullopt, it will delete the comment from the tape table
   * @param admin the admin who removes the comment
   * @param vid the vid of the tape to remove the comment
   * @param comment the new comment. If comment == std::nullopt, the comment will be deleted.
   */
  virtual void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
    const std::optional<std::string> &comment) = 0;

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to archive files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for archiving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  virtual void tapeMountedForArchive(const std::string &vid, const std::string &drive) = 0;  // internal function (noCLI)

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to retrieve files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for retrieving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  virtual void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) = 0;

  /**
   * This method notifies the CTA catalogue that there is no more free space on
   * the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  virtual void noSpaceLeftOnTape(const std::string &vid) = 0;

  /**
   * Returns the list of tapes that can be written to by a tape drive in the
   * specified logical library, in other words tapes that are labelled, not
   * disabled, not full, not read-only and are in the specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   * @return The list of tapes for writing.
   */
  virtual std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const = 0;

  virtual common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const = 0;
};

} // namespace catalogue
} // namespace cta