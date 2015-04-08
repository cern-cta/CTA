#pragma once

#include <string>

namespace cta {

/**
 * Abstract class representing a tape mount.
 */
class Mount {
public:

  /**
   * Constructor.
   *
   * @param id The identifier of the mount.
   * @param vid The volume idenitifier of the tape to be mounted.
   */
  Mount(const std::string &id, const std::string &vid);

  /**
   * Destructor.
   */
  virtual ~Mount() throw() = 0;

  /**
   * Returns the identiifer of the mount.
   */
  const std::string &getId() const throw();

  /**
   * Returns the volume identiifer of the mount.
   */
  const std::string &getVid() const throw();

private:

  /**
   * The identiifer of the mount.
   */
  std::string m_id;

  /**
   * The volume identiifer of the tape to be mounted.
   */
  std::string m_vid;

}; // class Mount

} // namespace cta
