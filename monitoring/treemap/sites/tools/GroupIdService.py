class TheGroupIdService(object):
    id_to_object = {}

def newGroupId(object, parentpk, model, depth):  
    id = "group_" + model + "_" + depth.__str__() + "_"  + parentpk.__str__()
    TheGroupIdService.id_to_object[id] = object
    return id

def resolveGroupId(parentpk, model, depth):
    try:
        id = "group_" + model + "_" + depth.__str__() + "_"  + parentpk.__str__()
        return TheGroupIdService.id_to_object[id]
    except IndexError:
        return None
