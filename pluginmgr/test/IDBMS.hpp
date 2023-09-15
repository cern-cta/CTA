#include <iostream>
#include <string>

class IDBMS {
public:
  virtual const char* Name() const = 0;
  virtual int Query(std::string str) const = 0;
  virtual ~IDBMS() = default;
};
