'''
Created on Jul 13, 2010
useful string functions
@author: kblaszcz
'''

def sizeInBytes(size):
    ret = []
    if size < 1024:
        ret.append(size.__str__())
        ret.append(" B")
    elif size < 1048576:
        ret.append("%.2f"%(size/1024.0))
        ret.append(" KB")
    elif size < 1073741824:
        ret.append("%.2f"%(size/1048576.0))
        ret.append(" MB")
    elif size < 1099511627776:
        ret.append("%.2f"%(size/1073741824.0))
        ret.append(" GB")
    elif size < 1125899906842624:
        ret.append("%.2f"%(size/1099511627776.0))
        ret.append(" TB")
    elif size < 1152921504606846976:
        ret.append("%.2f"%(size/1125899906842624.0))
        ret.append(" PB")
    elif size < 1180591620717411303424:
        ret.append("%.2f"%(size/1152921504606846976.0))
        ret.append(" EB")
    elif size < 1208925819614629174706176:
        ret.append("%.2f"%(size/1180591620717411303424.0))
        ret.append(" ZB") #zetta
    elif size < 1237940039285380274899124224:
        ret.append("%.2f"%(size/1208925819614629174706176.0))
        ret.append(" YB") #yotta
    elif size < 1267650600228229401496703205376:
        ret.append("%.2f"%(size/1237940039285380274899124224.0))
        ret.append(" XB") #xona
    else:
        ret.append("%.2f"%(size/1267650600228229401496703205376.0))
        ret.append(" WB") #weka
        #... 
        #weka
        #vunda, uda, treda, sorta, rinta, quexa, pepta, ocha, nena, minga, luma, end of the universe?
        
    return ''.join([bla for bla in ret])

#split text by slashes or character limit
def splitText(text, limit = 50, firstlimit = 39):
    limitold = limit
    limit = firstlimit
    assert(limit > 1)
    ret = []
    while len(text) > 0:
        if(len(text)<=limit):
            ret.append(text)
            break
        else:
            bestlimit = limit
            alternativelimit = text[:limit].rfind('/')
            if alternativelimit > 1: bestlimit = alternativelimit
            
            ret.append(text[:bestlimit])
            text = text[bestlimit:]
        limit = limitold
            
    return ret
