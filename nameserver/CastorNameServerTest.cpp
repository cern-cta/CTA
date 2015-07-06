#include "nameserver/CastorNameServer.hpp"
#include "common/exception/Exception.hpp"
#include <iostream>
#include <string>
#include <stdlib.h>

int main() {
  try {
    cta::CastorNameServer ns;
    const cta::SecurityIdentity requester;
    cta::ArchiveDirIterator it = ns.getDirContents(requester, "/");
    while(it.hasMore()) {
      cta::ArchiveDirEntry entry = it.next();
      std::cout << entry.getName() << " " << entry.entryTypeToStr(entry.getType()) << std::endl;
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