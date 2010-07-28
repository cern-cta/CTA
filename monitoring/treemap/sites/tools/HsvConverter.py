'''
Created on Jul 2, 2010

@author: kblaszcz
'''
import math


def hsvToRgb(h,s,v):
    
    r,g,b = 0.0, 0.0, 0.0

    hdash = h * 6.0 #sector 0 to 5
    chroma = v * s #colorfulness
    
    x = chroma * (1.0 - math.fabs((hdash % 2.0) - 1.0))

    if hdash < 1.0:
        r = chroma
        g = x
    elif hdash < 2.0:
        r = x
        g = chroma
    elif hdash < 3.0:
        g = chroma
        b= x
    elif hdash < 4.0:
        g= x
        b = chroma
    elif hdash < 5.0:
        r = x
        b = chroma
    elif hdash < 6.0:
        r = chroma
        b = x

    min = v - chroma

    r = r + min
    g = g + min
    b = b + min

    return r,g,b

#    i = math.floor(h) #sector numnber
#    f = h - i #factorial part of h
#    
#    p = v * (1.0 - s)
#    q = v * (1.0 - s * f)
#    t = v * (1.0 - s * (1.0 - f))
#    
#    if i == 0 or i == 6:
#        return v,t,p
#    if i == 1:
#        return q,v,p
#    if i == 2:
#        return p,v,t
#    if i == 3:
#        return p,q,v
#    if i == 4:
#        return t,p,v
#    if i == 5:
#        return v,p,q
    
def rgbToHsv(r,g,b):
    
    #calculate v
    v = max(r,g,b)
    if v == 0.0:
        h=0.0
        s=0.0
        return h,s,v
    
    #normalize rgb
    r = r/v
    g = g/v
    b = b/v
    
    #min and max of rgb
    rgb_min = min(r,g,b)
    rgb_max = max(r,g,b)
    
    #calculate saturation
    s = rgb_max - rgb_min;
    if s == 0.0:
        h=0.0
        return h,s,v
    
    #Normalize r,g,b
    r = (r - rgb_min)/(rgb_max - rgb_min);
    g = (g - rgb_min)/(rgb_max - rgb_min);
    b = (b - rgb_min)/(rgb_max - rgb_min);
    
    rgb_min = min(r,g,b)
    rgb_max = max(r,g,b)
    
    #calc hue
    h = 0.0
    if (rgb_max == r):
        h = 0.0 + 60.0*(g - b)
    elif (rgb_max == g):
        h = 120.0 + 60.0*(b - r)
    elif (rgb_max == b) :
        h = 240.0 + 60.0*(r - g) 
    if (h < 0.0):
        h = h + 360.0
    
    return h/360.0,s,v

def addValueToRgb(r,g,b, delta):
    if delta == 0.0: return r,g,b
    
    h,s,v = rgbToHsv(r,g,b)
    v = v + delta
    
    if v > 1.0: 
        delta = v - 1.0 
        v = 1.0
        
    if v < 0.0: 
        delta = v
        v = 0.0
        
    #in case delta still left, decrease saturation to white
    
    if delta > 0.0: 
        s = s - delta 
        if s < 0.0: s = 0.0
        
    if delta < 0.0:
        s = s - delta
        if s > 1.0: s = 1.0
        
    r,g,b = hsvToRgb(h, s, v)
    return r,g,b

def rotateHueInRgb(r,g,b, delta):
    assert (delta <= 1.0)
    h,s,v = rgbToHsv(r,g,b)
    h = h + delta
    if h > 1.0: h = h - 1.0
    if h < 0.0: h = h + 1.0
    r,g,b = hsvToRgb(h, s, v)
    return r,g,b

def setHueToRgb(r,g,b,hue):
    assert(hue <= 1.0 and hue >= 0.0)
    h,s,v = rgbToHsv(r,g,b)
    h = hue
    return hsvToRgb(h, s, v)
        
        





    


    
    

        