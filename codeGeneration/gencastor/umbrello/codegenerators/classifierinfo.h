/***************************************************************************
                          classifierinfo.h  -  description
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

#ifndef CLASSIFIERINFO_H
#define CLASSIFIERINFO_H

#include "../umldoc.h"
#include "../attribute.h"
#include "../association.h"
#include "../umlclassifierlist.h"
#include "../umlassociationlist.h"
#include "../umloperationlist.h"
#include "../umlattributelist.h"

#include <qptrlist.h>
#include <qstring.h>
#include <qmap.h>
#include <map>

/**
 * class ClassInfo is an object to hold summary information about a classifier
 * in a convient form for easy access by a code generator.
 */
class ClassifierInfo {
 public:

  /**
   * Constructor, initialises a couple of variables
   */
  ClassifierInfo (UMLClassifier * classifier, UMLDoc * doc);

  /**
   * Destructor, empty
   */
  virtual ~ClassifierInfo();

 public:


  /**
   * Returns the entries in m_pOpsList that are actually operations
   * and have the right permit scope. If the second argument is
   * set to true, only keeps abstract operations.
   */
  UMLOperationList*
    getFilteredOperationsList(Uml::Visibility permitVisibility,
                              bool keepAbstractOnly = false);

	/**
	 * Returns a list of concepts which a given classifier inherits from.
   * @param c the classifier we're working on
	 * @return	List of UMLClassifiers we inherit from.
	 */
	static UMLClassifierList findSuperClassConcepts(UMLClassifier * c);

  /**
   * Returns a list of interfaces which a given classifier inherits from.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we implement.
   */
  static UMLClassifierList findSuperInterfaceConcepts (UMLClassifier * c);

  /**
   * Returns the list of classes and interfaces which  a given classifier
   * inherits from.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we inherit from.
   */
  static UMLClassifierList findAllSuperClassConcepts (UMLClassifier * c);

  /**
   * Returns a list of interfaces and abtract classes which
   * a given classifier inherits from.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we implement.
   */
  static UMLClassifierList findSuperAbstractConcepts (UMLClassifier * c);

  /**
   * Returns a list of interfaces which a given classifier directly implements.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we implement.
   */
  static UMLClassifierList findImplementedAbstractConcepts (UMLClassifier * c);

  /**
   * Returns a list of interfaces which a given classifier implements.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we implement.
   */
  static UMLClassifierList findAllImplementedAbstractConcepts
    (UMLClassifier * c);

  /**
   * Returns a list of all classes and interfaces which a given
   * classifier implements.
   * @param c the classifier we're working on
   * @return      list    a QPtrList of UMLClassifiers we implement.
   */
  static UMLClassifierList findAllImplementedClasses (UMLClassifier * c);

	/**
	 * Returns the entries in m_pOpsList that are actually operations
   * and have the right permit scope. If the second argument is
   * set to true, only keeps abstract operations.
	 */
	static UMLOperationList*
    getFilteredOperationsList(UMLClassifier *c,
                              Uml::Visibility permitVisibility,
                              bool keepAbstractOnly = false);

 public:

  // Fields
  //

  /**
   * Lists of all attributes (included inherited)
   */
  UMLAttributeList allAttributes;

  /**
   * Lists of attributes of this classifier (if a class)
   * Sorted by scope.
   */
  UMLAttributeList atpub;
  UMLAttributeList atprot;
  UMLAttributeList atpriv;

  /**
   * Lists of static attributes of this classifier (if a class)
   */
  UMLAttributeList static_atpub;
  UMLAttributeList static_atprot;
  UMLAttributeList static_atpriv;

  /**
   * Lists of types of associations this classifier has
   */
  UMLAssociationList plainAssociations;
  UMLAssociationList aggregations;
  UMLAssociationList compositions;
  UMLAssociationList generalizations;
  UMLAssociationList realizations;

  /**
   * what sub and super classifiers are related to this class
   */
  UMLClassifierList superclasses;
  UMLClassifierList allSuperclasses;
  QMap<Uml::IDType, bool> allSuperclassIds;
  UMLClassifierList superInterfaces;
  UMLClassifierList superAbstracts;
  UMLClassifierList implementedAbstracts;
  UMLClassifierList allImplementedAbstracts;
  UMLClassifierList subclasses;

  /**
   * Various conditional information about our classifier.
   */
  bool isInterface; // Whether or not this classifier is an interface.
  bool isAbstract; // Whether or not this classifier is abstract.
  bool hasAssociations;
  std::map<Uml::Visibility, bool> hasAttributes;
  std::map<Uml::Visibility, bool> hasStaticAttributes;
  std::map<Uml::Visibility, bool> hasMethods;
  std::map<Uml::Visibility, bool> hasAccessorMethods;
  std::map<Uml::Visibility, bool> hasOperationMethods;
  bool hasVectorFields;

  /**
   * Package, Class and File names
   */
  QString packageName;
  QString fullPackageName;
  QString className;
  QString fileName;

  /**
   * utility functions to allow easy determination of what classifiers
   * are "owned" by the current one via named association type (e.g.
   * plain, aggregate or compositions).
   */
  UMLClassifierList getPlainAssocChildClassifierList();
  UMLClassifierList getAggregateChildClassifierList();
  UMLClassifierList getCompositionChildClassifierList();

  /**
   * Utility method to obtain list of attributes, if they exist, for
   * the current classfier.
   */
  UMLAttributeList* getAttList();

  Uml::IDType id() { return m_nID; }

 protected:
    void init (UMLClassifier *c, UMLDoc *doc);

 private:

    Uml::IDType m_nID; // id of the classifier

    /**
     * Utility method called by "get*ChildClassfierList()" methods. It basically
     * finds all the classifiers named in each association in the given
     * association list which arent the current one. Usefull for finding which
     * classifiers are "owned" by the current one via declared associations such
     * as in aggregations/compositions.
     */
    UMLClassifierList findAssocClassifierObjsInRoles
      (UMLAssociationList * list);

    /**
     *      List of all the attributes in this class.
     */
    UMLAttributeList m_AttsList;

};

#endif // CLASSIFIERINFO_H

