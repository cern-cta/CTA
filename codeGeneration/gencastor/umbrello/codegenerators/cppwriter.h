/***************************************************************************
                          cppwriter.h  -  description
                             -------------------
    copyright            : (C) 2003 Brian Thomas
***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

#ifndef CPPWRITER_H
#define CPPWRITER_H

// Local includes
#include "cppcastorwriter.h"
#include "cppbasewriter.h"

class UMLOperation;

/**
 * class CppWriter is a code generator for UMLClassifier objects.
 * Create an instance of this class, and feed it a UMLClassifier when
 * calling writeClass and it will generate both a header (.h) and
 * source (.cpp) file for that classifier.
 */
class CppWriter : public CppCastorWriter {
 public:

  /**
   * Constructor, initialises a couple of variables
   */
  CppWriter(UMLDoc* parent = 0, const char* name = 0);

  /**
   * Destructor, empty
   */
  virtual ~CppWriter();

  /**
   * call this method to generate cpp code for a UMLClassifier
   * @param c the class to generate code for
   */
  virtual void writeClass(UMLClassifier *c);

 private:
  /** configures a new generator using parameters of this one */
  void configGenerator(CppBaseWriter &cg);
  /** configure and runs a generator */
  void runGenerator(CppBaseWriter &cg,
                    QString fileName,
                    UMLClassifier *c);

};



#endif // CPPWRITER_H
