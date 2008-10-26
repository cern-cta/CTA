#include "castor/vdqm/Utils.hpp"

bool castor::vdqm::Utils::isAValidUInt(char *str) {
  // An empty string is not a valid unsigned integer
  if(*str == '\0') {
    return false;
  }

  // For each character in the string
  for(;*str != '\0'; str++) {
    // If the current character is not a valid numerical digit
    if(*str < '0' || *str > '9') {
      return false;
    }
  }

  return true;
}
