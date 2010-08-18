class TheGroupIdService(object):
    id_to_object = {}

def newGroupId(object, parentpk, model, depth):  
    id = "group_" + parentpk.__str__() + "_" + depth.__str__() + "_" + model
    TheGroupIdService.id_to_object[id] = object
    return id

def resolveGroupId(parentpk, model, depth):
    try:
        id = "group_" + parentpk.__str__() + "_" + depth.__str__() + "_" + model
        return TheGroupIdService.id_to_object[id]
    except IndexError:
        return None
