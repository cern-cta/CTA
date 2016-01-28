/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "nameserver/CastorNameServer.hpp"
#include "common/exception/Exception.hpp"
#include <iostream>
#include <string>
#include <stdlib.h>

int main() {
  try {
    cta::CastorNameServer ns;
    const cta::SecurityIdentity requester;
    cta::common::archiveNS::ArchiveDirIterator it = ns.getDirContents(requester, "/");
    while(it.hasMore()) {
      cta::common::archiveNS::ArchiveDirEntry entry = it.next();
      std::cout << entry.name << " " << entry.entryTypeToStr(entry.type) <<
        std::endl;
    }
    ns.createDir(requester, "/cta", 0777);
    ns.deleteDir(requester, "/cta");
  } catch(cta::exception::Exception &ex) {
    std::cout << "CTA exception: " << ex.getMessageValue() << std::endl;
  } catch(std::exception &ex) {
    std::cout << "std exception: " << ex.what() << std::endl;
  } catch(...) {
    std::cout << "unknown exception!" << std::endl;
  }
  return 0;
}
