/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

#ifndef CANVASOBJECT_H
#define CANVASOBJECT_H

#include "umlobject.h"
#include "umlobjectlist.h"
#include "umlclassifierlist.h"
#include "umlassociationlist.h"
#include <qptrlist.h>

class IDChangeLog;

/**
 * This class contains the non-graphical information required for UMLObjects
 * which appear as moveable widgets on the canvas.
 *
 * This class inherits from @ref UMLObject which contains most of the
 * information.
 * It is not instantiated itself, it's just used as a super class for
 * actual model objects.
 *
 * @short Non-graphical information for a UMLCanvasObject.
 * @author Jonathan Riddell
 * @see	UMLObject
 * Bugs and comments to uml-devel@lists.sf.net or http://bugs.kde.org
 */

class UMLCanvasObject : public UMLObject {
	Q_OBJECT
public:
	/**
	 * Sets up a UMLCanvasObject.
	 *
	 * @param name		The name of the Concept.
	 * @param id		The unique id of the Concept.
	 */
	UMLCanvasObject(const QString & name = "", int id = -1);


	/**
	 * Standard deconstructor.
	 */
	virtual ~UMLCanvasObject();

	/**
	 *  Overloaded '==' operator
	 */
	virtual bool operator==(UMLCanvasObject& rhs);

	/**
	 * Copy the internal presentation of this object into the new
	 * object.
	 */
	virtual void copyInto(UMLCanvasObject *rhs) const;

	// The abstract method UMLObject::clone() is implemented
	// in the classes inheriting from UMLCanvasObject.

	/**
	 * Adds an association.
	 *
	 * @param assoc		The association to add.
	 */
	bool addAssociation(UMLAssociation* assoc);

	/**
	 * Determine if this canvasobject has the given association.
	 *
	 * @param assoc		The association to check.
	 */
	bool hasAssociation(UMLAssociation* assoc);

	/**
	 * Remove an association from the CanvasObject.
	 *
	 * @param o		The association to remove.
	 */
	int removeAssociation(UMLAssociation *assoc);

	/**
	 * Returns the number of associations for the CanvasObject.
	 * This is the sum of the aggregations and compositions.
	 *
	 * @return	The number of associations for the Concept.
	 */
	int associations();

 	/**
	 * Return the list of associations for the CanvasObject.
	 *
	 * @return	The list of associations for the CanvasObject.
	 */
	const UMLAssociationList& getAssociations();

	/**
	 * Return the subset of m_AssocsList that matches the given type.
	 *
	 * @param assocType	The Association_Type to match.
	 * @return	The list of associations that match assocType.
	 */
	UMLAssociationList getSpecificAssocs(Uml::Association_Type assocType);

	/**
   * Shorthand for getSpecificAssocs(Uml::at_Association)
   * and getSpecificAssocs(Uml::at_UniAssoication)
   * 
   * @return      The list of "standard" associations for the Concept.
   */
  UMLAssociationList getStdAssociations();
  
  /**
	 * Return a list of the superclasses of this concept.
	 * TODO: This overlaps with UMLClassifier::findSuperClassConcepts(),
	 *       see if we can merge the two.
	 *
	 * @return	The list of superclasses for the concept.
	 */
	UMLClassifierList getSuperClasses();

	/**
	 * Return a list of the classes that inherit from this concept.
	 * TODO: This overlaps with UMLClassifier::findSubClassConcepts(),
	 *       see if we can merge the two.
	 *
	 * @return	The list of classes inheriting from the concept.
	 */
	UMLClassifierList getSubClasses();

	/**
	 * Shorthand for getSpecificAssocs(Uml::at_Generalization)
	 *
	 * @return	The list of generalizations for the Concept.
	 */
	virtual UMLAssociationList getGeneralizations();

	/**
	 * Shorthand for getSpecificAssocs(Uml::at_Realization)
	 *
	 * @return	The list of realizations for the Concept.
	 */
	virtual UMLAssociationList getRealizations();

	/**
	 * Shorthand for getSpecificAssocs(Uml::at_Aggregation)
	 *
	 * @return	The list of aggregations for the Concept.
	 */
	UMLAssociationList getAggregations();

	/**
	 * Shorthand for getSpecificAssocs(Uml::at_Composition)
	 *
	 * @return	The list of compositions for the Concept.
	 */
	UMLAssociationList getCompositions();

	/**
	 * Find a list of associations with the given name.
	 *
	 * @param t		The type to find.
	 * @param n		The name of the object to find.
	 * @param seekStereo	Set this true if we should look at the object's
	 *			stereotype instead of its name.
	 * @return	List of objects found (empty if none found.)
	 */
	virtual UMLObjectList findChildObject(UMLObject_Type t, QString n,
					      bool seekStereo = false);

	/**
	 * Find an association.
	 *
	 * @param id		The id of the object to find.
	 * @return	Pointer to the object found (NULL if not found.)
	 */
	virtual UMLObject* findChildObject(int id);

	/**
	 * Returns a name for the new association, operation, template
	 * or attribute appended with a number if the default name is
	 * taken e.g. new_association, new_association_1 etc.
	 *
	 * @param type		The object type for which to make a name.
	 * @param seekStereo	Set this true if we should look at the object's
	 *			stereotype instead of its name.
	 * @return	Unique name string for the UMLObject_Type given.
	 */
	virtual QString uniqChildName(const UMLObject_Type type,
				      bool seekStereo = false);

	// The abstract method UMLObject::saveToXMI() is implemented
	// in the classes inheriting from UMLCanvasObject.

protected:

	/**
	 * List of all the associations in this class.
	 */
	UMLAssociationList m_AssocsList;

private:

	/**
	 * Initialises key variables of the class.
	 */
	void init();

signals:

	/**
	 * Emit when new association is added.
	 * @param assoc Pointer to the association which has been added.
	 */
	void sigAssociationAdded(UMLAssociation * assoc);

	/**
	 * Emit when new association is removed.
	 * @param assoc Pointer to the association which has been removed.
	 */
	void sigAssociationRemoved(UMLAssociation * assoc);

};

#endif
