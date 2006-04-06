/******************************************************************************
 *                      ccclasswriter.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: ccclasswriter.h,v $ $Revision: 1.5 $ $Release$ $Date: 2006/04/06 07:08:44 $ $Author: sponcec3 $
 *
 * This generator creates a .h file containing the C interface
 * to the corresponding C++ class
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CODEGENERATORS_CCCLASSWRITER_H 
#define CODEGENERATORS_CCCLASSWRITER_H 1

// Includes Files
#include "cppbasewriter.h"
#include <qstring.h>
#include <map>
#include <set>

class CCClassWriter : public CppBaseWriter {

 public:
  /**
   * Constructor
   */
  CCClassWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CCClassWriter() {};

 public:
  /**
   * write the content of the file
   */
  void writeClass(UMLClassifier *c);

  /**
   * Initialization of the writer.
   * It creates the file and writes some header in it.
   */
  virtual bool init(UMLClassifier* c, QString fileName);
  
  /**
   * Finalizes the writer. This means writing the footer
   * of the file and closing it
   */
  virtual bool finalize(UMLClassifier* c);

 private:

	/**
	 * Writes the constructor and destructor declarations
	 */
	void writeConstructors(QTextStream &stream,
                         UMLClassifier* c); 

	/**
	 * Writes the casts methods
	 */
	void writeCasts(QTextStream &stream,
                  UMLClassifier* c); 

	/**
	 * Writes the casts methods for a given parent
	 */
	void writeCast(QTextStream &stream,
                 QString name); 

  /**
   * Writes all accessor methods for attributes and associations
   */
  void writeHeaderAccessorMethod(UMLClassifier *c,
                                 ClassifierInfo *classInfo,
                                 QTextStream &stream);

	/**
	 * calls @ref writeSingleAttributeAccessorMethods() on each of the
   * attributes in attribs list.
	 */
	void writeAttributeMethods(QPtrList<UMLAttribute>& attribs,
                             QTextStream &stream);


  /**
   * Write associations accessors
   */
  void writeAssociationMethods (QPtrList<UMLAssociation> associations,
                                Uml::IDType myID,
                                QTextStream &stream);
	/**
	 * calls @ref writeSingleAttributeAccessorMethods() or @ref
	 * writeVectorAttributeAccessorMethods() on the assocaition
	 * role
	 */
	void writeAssociationRoleMethod(QString fieldClassName,
                                  QString roleName,
                                  Multiplicity multi, 
                                  Uml::Changeability_Type change,  
                                  QTextStream &stream); 
	/**
	 * Writes getFoo() and setFoo() accessor methods for the attribute
	 */
	void writeSingleAttributeAccessorMethods(QString fieldClassName,
                                           QString fieldName,
                                           Uml::Changeability_Type change,
                                           bool isStatic,
                                           QTextStream &cpp);

	/**
	 * Writes addFoo() and removeFoo() accessor methods for the Vector attribute
	 */
	void writeVectorAttributeAccessorMethods(QString fieldClassName,
                                           QString fieldName,
                                           Uml::Changeability_Type change,
                                           QTextStream &cpp);
	/**
	 * write all operations for a given class
	 * @param c the class for which we are generating code
	 * @param j what type of method to write (by Visibility)
	 * @param k the stream associated with the output file
	 */
	void writeOperations(UMLClassifier *c,
                       QTextStream &j,
                       QValueList<QString>& alreadyGenerated);

	/**
	 * write a list of operations for a given class.
   * This is an abstract method that should be implemented
   * when needed.
	 * @param list the list of operations you want to write
	 * @param isInterfaceImpl whether or not we are implementing
   * an interface we inherit from
	 * @param j the stream associated with the output file
	 */
	void writeOperations(QPtrList<UMLOperation> &list,
                       QTextStream &j,
                       QValueList<QString>& alreadyGenerated);

  /**
   * get prefix for C functions
   */
  QString getPrefix(QString type, QString ns);

  /**
   * converts C++ type name into the corresponding C type names
   */
  QString convertType(QString type, QString ns);

  /**
   * tells whether an operation should be printed or not
   */
  bool isOpPrintable(UMLOperation* op);

 private:
  /** the prefix to add in front of any function name */
  QString m_prefix;

  /** type conversions from C++ to C */
  std::map<QString, QString> m_typeConvertions;

  /** a set of classes to define in C via typedefs */
  std::set<std::pair<QString, QString> > m_typedefs;

};

#endif // CODEGENERATORS_CCCLASSWRITER_H
