#include "BackendVFS.hpp"
#include <iostream>
#include <memory>

int main() {
  cta::objectstore::BackendVFS bs;
  std::auto_ptr<cta::objectstore::Backend::Parameters> params(bs.getParams());
  std::cout << "Created a new backend store: " << params->toStr() << std::endl; 
}