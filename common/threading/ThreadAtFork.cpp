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

#include <pthread.h>
#include "ThreadAtFork.hpp"


namespace cta::threading {

void ThreadAtFork::addHandlerBeforeFork(void (*prepare)(void)) {
  if(pthread_atfork(prepare, NULL, NULL)) {
    throw cta::threading::TreadAtForkException("Failed to add handler before fork.");
  }
}

void ThreadAtFork::addHandlerAfterParentFork(void (*parent)(void)) {
  if(pthread_atfork(NULL, parent, NULL)) {
    throw cta::threading::TreadAtForkException("Failed to add handler after fork for parent.");
  }
}

void ThreadAtFork::addHandlerAfterChildFork(void (*child)(void)) {
  if(pthread_atfork(NULL, NULL, child)) {
    throw cta::threading::TreadAtForkException("Failed to add handler after fork for child.");
  }
}

} // namespace cta::threading
