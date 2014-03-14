#include "Exception.hpp"


void errnoExeptLauncher(int ret, void * obj) {
  if(ret) {
    char s[1000];
    char t[1000];
#ifdef _GNU_SOURCE
    char * errorStr = ::strerror_r(ret, s, sizeof(s));
    if (!errorStr) {
      snprintf(t, sizeof(t), "Failed to get error string for ret=%d obj=0x%p", ret, obj);
      throw MemException(t);
    } else {
      snprintf(t, sizeof(t), "%s (ret=%d) obj=0x%p",errorStr, ret, obj);
      throw MemException(t);
    }
#else /* This should work for CLANG on MAC */
    int r2 = ::strerror_r(ret, s, sizeof(s));
    if (!r2) {
      snprintf(t, sizeof(t), "Failed to get error string for ret=%d obj=0x%p", ret, obj);
      throw MemException(t);
    } else {
      snprintf(t, sizeof(t), "%s (ret=%d) obj=0x%p",s, ret, obj);
      throw MemException(t);
    }
#endif
  }
}
