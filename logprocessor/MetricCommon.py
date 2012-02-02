
#######################################################################
#                           Some informations                         #
#######################################################################
#
# Data test file: c2aggregated.test.log ( 31MB, 100000 log messages)
# 
# Data test Metric:
#  <metric>
#   name: CountHosts
#   window: 300
#   conditions: MESSAGES=='ALLMESSAGES'
#   groupbykeys: INSTANCE, HOSTNAME
#   data: Counter(MSG), Avg(USECS)
#   handle_unordered: time_threshold
#   nbins: 10
#  </metric>
#  
# Execution times on lxbrb1916:
# 
# With metric matching and Timestamp to EPOCH cnversion:  14 sec
# With metric matching messages EPOCH                  :  10 sec
# Without metric matching messages EPOCH               :   8 sec





#######################################################################
#                           INITIALIZATION                            #
#######################################################################

# Imports
import time
import pickle as pk
import sys
import math
import glob

# Nedeed for debugging (human-readable dates):
import datetime


# Debugging switch
debug_on=False



#######################################################################
#                                                                     #
#                              Data Objects                           #
#                                                                     #
#######################################################################



#######################################################################
#   Class Avg (List)                                                  #
#######################################################################

class Avg(list):
    """
    A data object that provide Averages
    """

    #def __init__(self, start=None):
    #    if (start==None):
    #        self.extend([None,None,None,None,None])
    #    else:
    #        self=start

    def __add__(self, other):

        # Merge two Avg objects:

        # Check if one of the two is empty. Here instead of returning self or other,
        # we should create a new element, but with numeric types it is fine also like that.
        if (len(self)==0):
            return other
        if (len(other)==0):
            return self

        # n:
        n = self[0] + other[0]
        
        # Min:
        if (other[1] < self[1]):
            Min = other[1]
        else:
            Min = self[1]

        # Max:
        if (other[2] > self[2]):
            Max = other[2]
        else:
            Max = self[2]

        # Sum:
        Sum = self[3] + other[3]

        # Sum_sqr        
        Sum_sqr = self[4] + other[4]

        # We return a *new* object with the new values
        return Avg([n,Min,Max,Sum,Sum_sqr])


    def add(self, element):

        # Add an element

        # Make sure that we are adding a numeric type    
        if type(element) not in ['int','float']:
            # If we are trying to add a non-numeric value, try to convert it to float
            try:
                element=float(element)
            except ValueError:
                # If we are unable to conver it, skip.
                return None
                 
        
        # If this is the first element to be added to the object, initialize it
        if (len(self)==0):
            self.extend([None,None,None,None,None])
            self[0]=1               # n
            self[1]=element         # Min
            self[2]=element         # Max
            self[3]=element         # Sum
            self[4]=element*element # Sum_sqr

        else:
            # n:
            self[0]+=1
            
            # Min:
            if (element < self[1]):
                self[1]=element

            # Max:
            if (element > self[2]):
                self[2]=element

            # Sum:
            self[3]+=element

            # Sum_sqr:        
            self[4]+=(element*element)

        return None


    def getData(self):
        if len(self)==0:
            return[0,0,0,0,0]
        else:
        
            # http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
            # mean = Sum/n
            # variance = (Sum_sqr - Sum*mean)/(n - 1)
            
            n        = self[0]
            Sum      = self[3]
            Sum_sqr  = self[4]
            Min      = self[1]
            Max      = self[2]
            mean     = Sum/n
            
            if (n != 1):
                variance = (Sum_sqr - Sum*mean)/(n-1)
            else:
                variance=0.0

            stddev=math.sqrt(variance)

            #return [n, Sum, mean, Min, Max, stddev]
            return [mean, n, Sum , Min, Max, stddev]



#######################################################################
#   Class EstimateDelay (List)                                        #
#######################################################################

class EstimateDelay(list):
    """
    To estimate unordered messages mean delay
    """

    def __add__(self, other):

        # Merge two Avg objects:
        if (len(self)==0):
            return other
        if (len(other)==0):
            return self

        # nb:
        anb = self[0] + other[0]        
        # min:
        if (other[1] < self[1]):
            amin = other[1]
        else:
            amin = self[1]
        # max:
        if (other[2] > self[2]):
            amax = other[2]
        else:
            amax = self[2]
        # sum:
        asum = self[3] + other[3]

        # sqsum:        
        asqsum = self[4] + other[4]
        
        return EstimateDelay([anb,amin,amax,asum,asqsum])


    def add(self, element):

        # Add an element

        if element=='':
            return None 

        # Make sure that we are adding a numeric type    
        if type(element) not in ['int','float']:
            # If we are trying to add a non-numeric value, try to convert it to float
            element=float(element)
            
        if element==0:
            return None 
        
        # If this is the first element to be added to the object, initialize it
        if (len(self)==0):
            self.extend([None,None,None,None,None])
            self[0]=1           # nb
            self[1]=element       # min
            self[2]=element       # max
            self[3]=element       # sum
            self[4]=element*element # sqsum

        else:
            # nb:
            self[0]+=1
            
            # min:
            if (element < self[1]):
                self[1]=element

            # max:
            if (element > self[2]):
                self[2]=element

            # sum:
            self[3]+=element

            # sqsum:        
            self[4]+=(element*element)

        return None

    def getData(self):
    
        if len(self)==0:
            return[0,0,0,0,0]
        else: 
            #         nb           mean        min     max   sq.sum
            return [self[0], self[3]/self[0],self[1],self[2],self[4]]



#######################################################################
#   Class DummyStore (List)                                           #
#######################################################################
class DummyStore(list):
    """
    To store entries as they are found (for data mining).
    """

    def __init__(self, initlist=None):
        if initlist==None:
            self.data=[]
        else:
            self.data=initlist

    def __add__(self,other):
    
        # Merge two lists
        return DummyStore(self.data+other.data) 

    def add(self, element):

        # Add an element
        self.data.append(element)

    def getData(self):
        return self.data
      
        

#######################################################################
#   Class ListOne (list)                                             #
#######################################################################
class ListLast(list): 
    """
    To store one entry (the last one found), to have an idea of what is in a field (for data mining).
    """
       
    def __init__(self, initlist=None):
        if initlist==None:
            self.data=[]
        else:
            self.data=initlist

    def __add__(self,other):

        # It keeps at every step the last element found
        return ListLast(other.data) 

    def add(self, other):

        # It keeps at every step the last element found
        self.data=[other]       

    def getData(self):
        return self.data



#######################################################################
#   Class ListUnique (list)                                           #
#######################################################################
class ListUnique(list):
    """
    To store uniques entries found i.e.: 16MB, 32MB, 16MB -> 16MB, 32MB
    """

    def __init__(self, initlist=None):
        if initlist==None:
            self.data=[]
        else:
            self.data=initlist

    def __add__(self,other):

        # For merging two objects, we first need to copy the data,
        # since we want to preserve the original object.
        resultlist=self.data[:]

        # Then, we merge: we have to add only elements not already present
        for element in other.data:
            if element not in resultlist:
                resultlist.append(element)
            
        # Return a new instance of the object containing the results
        return ListUnique(resultlist) 

    def add(self, element):

        # Add an element, if it is not already present
        if element not in self.data:
            self.data.append(element)

    def getData(self):
        return self.data



#######################################################################
#   Class ListAndMerge (dict)                                         #
#######################################################################
class ListAndMerge(dict):
    """
    To store uniques keys (with one example value) of a dictionary
    """
    
    def __init__(self, initdict=None):

        # This trick prevents linking to the empty dict two differnet objects,
        # and it let to avoid using the in place copy initdic=dic(initdic)
        
        if initdict==None:
            self.data={}
        else:
            self.data=initdict

    def __add__(self,other):

        # For merging two objects, we first need to copy the data,
        # since we want to preserve the original object.
        returndict=dict(self.data)
        
        # Then, we merge: we have to add only elements not already present 
        for key in other.data:
            if not returndict.has_key(key):
                returndict[key]=other.data[key]

        return ListAndMerge(returndict)


    def add(self, dictionary):

        # Add an element, if it is not already present
        for key in dictionary:
            if not self.data.has_key(key):
                self.data[key]=dictionary[key]    

    def getData(self):
        return self.data



#######################################################################
#   Class Counter (inherits from int)                                 #
#######################################################################
class Counter(int):
    """
    Count how many times it is called. The argoument can be whatever.
    """

    def __init__(self,init_to=0):
        self.count=init_to

    def __add__(self, other):
        return Counter(self.count+other.count)

    def add(self, element):       
        # Here we increase the counter discarding the element,
        # which can be whatever thing.
        self.count+=1

    def getData(self):
        return self.count
        
        
#######################################################################
#   Class CounterHz (inherits from int)                                 #
#######################################################################
class CounterHz(int):
    """
    Count how many times it is called. The argoument can be whatever.
    """

    def __init__(self,init_to=0):
        self.count=init_to

    def __add__(self, other):
        return CounterHz(self.count+other.count)

    def add(self, element):       
        # Here we increase the counter discarding the element,
        # which can be whatever thing.
        self.count+=1

    def getData(self):
        return self.count/300
        
        
#######################################################################
#   Class Max (inherits from int)                                 #
#######################################################################
class Max(int):
    """
    Count how many times it is called. The argoument can be whatever.
    """

    def __init__(self,init_to=0):
        self.count=init_to

    def __add__(self, other):
    
        if self.count>other.count:        
            return Max(self.count)
            
        else:
            return Max(other.count)

    def add(self, element):       
        # Here we increase the counter discarding the element,
        # which can be whatever thing.
        self.count+=1

    def getData(self):
        return self.count
        
 
#######################################################################
#   Class MaxMsgsPerSecOverMinute (inherits from int)                                 #
#######################################################################
class MaxMsgsPerSecOverMinute(int):
    """
    Over one minute interval, count the messages on one second interval. Then get the maximum value
    """

    first_time=True
    offset=0

    def __init__(self):
        self.minute=[]
        for i in range(0,60):
            self.minute.append(0)

    def __add__(self, other):
        # This is meant to be an object used with one bin. Since whe calculating the result
        # to give from this data object we merge a empty resultBin with a realBin with the data,
        # we basically retrn realBin (other) which has data inside it. Yes. it's tricky.. :/
        return other

    def add(self, TIMESTAMP):       

        #We have the TIMESTAMP, like: 2012-01-27T16:06:11+01:00
        
        #Get the second of the timestamp: 
        second=int(TIMESTAMP.split(':')[2][:2])

        if self.first_time:
            self.first_time=False
            self.offset=second
            
        # mod 60 math..
        if second-self.offset<0:
            second+=60
            
        # Increase the counter for this second..     
        self.minute[second-self.offset]+=1
        

    def getData(self):
        Max=0
        for i in range(0,60):
            if self.minute[i] > Max:
                Max=self.minute[i]
                
        return Max 
 
 
#######################################################################
#   Class Adder (inherits from int)                                 #
#######################################################################
class Adder(float):
    """
    Sum up the values passed as argouments.
    """

    def __init__(self,init_to=0):
        self.partialsum=init_to

    def __add__(self, other):
        return Adder(self.partialsum+other.partialsum)

    def add(self, element):       
        self.partialsum+=float(element)

    def getData(self):
        return self.partialsum
        
        
#######################################################################
#   Class AdderMB (inherits from int)                                 #
#######################################################################
class AdderMB(float):
    """
    Sum up the values passed as argouments, dividing by 1024*1024 when returning data.
    """

    def __init__(self,init_to=0):
        self.partialsum=init_to

    def __add__(self, other):
        return AdderMB(self.partialsum+other.partialsum)

    def add(self, element):       
        self.partialsum+=float(element)

    def getData(self):
        return self.partialsum/(1024*1024)

#######################################################################
#   Class AdderGB (inherits from int)                                 #
#######################################################################
class AdderGB(float):
    """
    Sum up the values passed as argouments, dividing by 1024*1024 when returning data.
    """

    def __init__(self,init_to=0):
        self.partialsum=init_to

    def __add__(self, other):
        return AdderGB(self.partialsum+other.partialsum)

    def add(self, element):       
        self.partialsum+=float(element)

    def getData(self):
        return self.partialsum/(1024*1024*1024)

#######################################################################
#                                                                     #
#                 Here begins the real Metric Engine                  #
#                                                                     #
#######################################################################


#######################################################################
#                       METRIC COMMON FUNCTIONS                       #
#######################################################################


#---------------------------------------------------------------------------
# Debug handling...
def debug (str):    
    if debug_on:
        print str

#---------------------------------------------------------------------------
def timestamp_to_epoch(msg_timestamp, msg_epoch=None):
    """
    Timestamp (i.e. 2011-08-23T10:27:35+02:00) to epoch seconds conversion
    """
    
    # First, some hypothesis and concepts:
    
    # UTC (Unified Temps Coordinated) = GMT (Greenwich Mean Time)
    
    # Leap seconds:
    # Added to the year duration to compensate wrong year estimation, epoch time will just be oen second longer:
    # 798 -> 799 -> 800 -> 800 -> 801 -> 802
    # biut be careful: 800.25 800.50 800.75 800.0 800.25 etc.. -> risk of negative numbers in calculating deltas
    
    # Summertime:
    # UTC+1 becomes UTC+2, and the local hour changes: it is like if parallels are shifted, 
    # so handling timezones will automatically handle summertime.
    
    # EPOCH (Unix time) definition:
    # it is the number of seconds elapsed since midnight UTC on the morning 
    # of January 1, 1970, not counting leap seconds
    
    # ..so, we have a timestamp in the form: 2011-08-23T10:27:35+02:00. 
    # let's split it, since Python support for epoch with timezone sucks

    msg_timezone_sign  = msg_timestamp[-6] 
    msg_timezone       = msg_timestamp[-5:]
    msg_timestamp      = msg_timestamp[:-6]

    # Convert the timezone into seconds, preserve the sign, invert it (for Python internal data handling)
    # and subtract the local timezone (since Python keep it in account for time conversions)
    msg_timezone_hour, msg_timezone_minutes = msg_timezone.split(':')
    msg_timezone_epoch = (-1) * int(msg_timezone_sign+'1') * ( (int(msg_timezone_hour) * 3600) + (int(msg_timezone_minutes) * 60) )
    
    # Previus approach (too expensive) was:     
    # Convert the timezone into seconds in the same way (dirty trick but works more than fine :) )
    # timezone_epoch  = (-1) * float(sign+str(time.mktime(time.strptime("1970-01-01T"+timezone+":00","%Y-%m-%dT%H:%M:%S")) - local_timezone_epoch))


    if ( msg_epoch==None ):
    
        # There is no epoch filed that we can use, we have to calculate the epoch from the timestamp...
           
        # Convert the timestamp (without the timezone) to a time object and then to EPOCH seconds
        msg_timestamp_epoch = time.mktime(time.strptime(msg_timestamp,"%Y-%m-%dT%H:%M:%S"))

        # get the local timezone -> Python internal one! We have to take care also about it since Python
        # use it in the conversions from timestamps to epoch et vice versa.
        local_timezone_epoch = time.timezone
       

        # And finally calculate the epoch in UTC: get the epoch of the timestamp withouth the timezone,
        # correct it by subtracting the local time zone, since Python adds it by default;
        # and then add the timezone of the original timestamp. 
        msg_timestamp_utc_epoch = ( msg_timestamp_epoch - local_timezone_epoch ) + msg_timezone_epoch 

        # Debugging...
        # print "msg_timestamp_epoch:      " + str(msg_timestamp_epoch)
        # print "local_timezone_epoch: " + str(local_timezone_epoch)
        # print "msg_timezone_epoch:       " + str(msg_timezone_epoch)
        # print "msg_utc_epoch:            " + str(utc_epoch)
        # print "TIMESTAMP (original)       : " + timestamp
        # print "TIMESTAMP (from utc_epoch) : " + str(datetime.datetime.fromtimestamp(utc_epoch))
        
        return msg_timestamp_utc_epoch
        
    else:
     
        # There is a field with the epoch alreay computed in the message, we can use it!
        # EPOCH field it has been calculated by Scribe injectors on every node.
        # This trick pulls down execution time on the test data set from 14 to 10 seconds!!
        
        # ..but we have to correct it, taking into account the timezone in which the epoch was generated.
        # IMHO, this is a python bug. By definition, EPOCH is from 1st Jan 1970, 00:00, UTC. So it should be computed
        # in respect to UTC, and not be affected by the local timezone, or summertime like the two following messages
        #
        # TIMESTAMP=2011-10-30T02:58:42+02:00 USECS=921295 EPOCH=1319939922 HOSTNAME=lxfsrc54a03 DAEMON=gcd PID=2957 ...
        # TIMESTAMP=2011-10-30T02:18:43+01:00 USECS=310435 EPOCH=1319937523 HOSTNAME=lxfsrc54a03 DAEMON=gcd PID=2957 ...
        #
        # Epoch difference, newer-older: 1319937523-1319939922 = -2399 (Should never happen with epoch time!)

        return ( int(msg_epoch) + msg_timezone_epoch )
        


#######################################################################
#   Class Bin                                                         #
#######################################################################

class Bin:

    """
    This is the Bin class, the basic container. It takes care of adding data and merging.
    """
    
    # Note: the Bin could also be developed as an "enhanced" dictionary.
    
    #---------------------------------------------------------------------------
    def __init__(self, groupbykeys, dataobjects):
    
        # Local vars
        self.groupbykeys=groupbykeys
        self.dataobjects=dataobjects

        # Initialize data
        self._initializeData()


    #---------------------------------------------------------------------------
    def _initializeData(self):

        # If there is no group by we skip the dictionary part (not implemented yet)
        if (len(self.groupbykeys)==0):
            self.data=[]
        else:
            self.data={}


    #---------------------------------------------------------------------------
    def reset(self):

        # This method is called to reset (empty) a bin (once it has expired).
        self._initializeData()
        

    #---------------------------------------------------------------------------
    def getData(self):
        return self.data



    #---------------------------------------------------------------------------
    def mergeWith(self, anotherBin):

        # Call the recursive merge function, on the other bin's data
        self.merge(anotherBin.getData())


    #---------------------------------------------------------------------------
    def merge(self, anotherBin_subtree, parents=[], curcount=0):

        # Explore recursively the bin content:

        if len(self.groupbykeys) != curcount:

            # We are still going through the GROUPBY levels: keep exploring the bin, go more in depth.
            for new_anotherBin_subtree in anotherBin_subtree:
            
                # Values of the next recursive call are here explicitly assigned to make things clear 
                newparents=parents
                newparents.append(new_anotherBin_subtree)
                newcurcount=curcount+1
                
                # Recursive call...
                self.merge(anotherBin_subtree[new_anotherBin_subtree], newparents, newcurcount)
                
                # Remove the last parent (it's a recursive call, we have to go back to the staring point!)
                newparents.pop()            
                
        else:
          
            # We are at the DATA level: we have finished in exploring this piece of the tree.
            # So, insert the data. For every parent check if the path exists; if not, create it.
            # Then, sum evry data object. The overloading of the '+' will take care of doing it
            # in the right way. :)

            # This is now a list od data objects:
            anotherBin_data=anotherBin_subtree
            
            # Now, check if *this* bin has the needed structure to be merged with the anotherBin.
            # In other words, are there the righ dictionaries (path)? Watch out: self.data is the entire
            # data structure, not only the data objects! That's why we loop over it.          
            thisBin_curlevel=self.data
            
	    # Hard debug..
            #print "thisBin_curlevel="+str(thisBin_curlevel)
            
            # Support vars...            
            something_created_in_path=0
            i=0            
                 
            # parents is a (ordered) list of the parents encountered during the recoursive 
            # exploration of this barnach of anotherBin, which brought us to this data level:
            # we have to chech that in this bin we have all the parents.         
            for parent in parents:
                # Since the list is ordered, we are going trought it in the right sequence.
                # But we have to skup the last one, since it has a list of data:
                if len(self.groupbykeys) != i:
                #if len(self.groupbykeys)-1 == i: break
                    if not thisBin_curlevel.has_key(parent):                
                    	# We didn/t find the right structure at this step, handle it:
                        if (i==len(self.groupbykeys)-1):
                            # Data level, add a list:
                            thisBin_curlevel[parent]=[]
                        else:
                            # Still in the path, add a dictionary:
                            thisBin_curlevel[parent]={}
                        
                        # Set that we have created something
                        something_created_in_path=1
                        
                    thisBin_curlevel=thisBin_curlevel[parent]
                    i+=1            

            # Now that we are gone throught *this* bin, the curlevel variable points automagically 
            # to the level where to insert data:
            wheretoinsertdata=thisBin_curlevel

            # If this path has just been created, it means that the data level it's empty,
            # so we have to initialize it (adding the right data objects in the right place):
            if something_created_in_path:

                # Create the objects that will accept data 
                for dataobject in self.dataobjects:

                    # Now dataobject is the name of the object (string), but
                    # we have to call the real object from it (Improve Ref: #0002)
                    dataobject_object = globals()[dataobject]
                    wheretoinsertdata.append(dataobject_object())


            # Okay, we now have the right structure. We can merge, and we do it by adding the data objects of *this*
            # bin with the data objects of the anotherBin. Data objects know how to sum themselves in the right way.
            
   
            for i in range(0,len(self.dataobjects)):
            
                # For every data object of this bin sum it with the other one.
                # Note: '+=' is not overloaded, just the '+' it is.
                wheretoinsertdata[i]=wheretoinsertdata[i]+anotherBin_data[i] 




    #---------------------------------------------------------------------------
    def addData(self, groupbyvalues,datavalues):

        # Start with the recursion for adding data:

        # At every level, the index follows the GROUPBY keys, as well as the depth of the nested dictionaries in the bin.
        # example: 3rd level: explorabledata=data[][][][....]
        
        # For every GROUPBY key, if the key for this level does not exists add it, and keep going trought the bin.
        # Once arrived to the data level, the last key has to be linked to a list instead of a dictionary,
        # which has to be filled with the data objects ready to take the data, according to the metric definition. 

        self.explorabledata=self.data

        for i in range(0,len(self.groupbykeys)):
        
            if ( i != len(self.groupbykeys)-1 ):  

                # We are still going through the GROUPBY levels:
                # keep exploring the bin, go more in depth

                # 1) Create the key if it does not exists and link it to another dictionary
                if not self.explorabledata.has_key(groupbyvalues[i]):
                    self.explorabledata[groupbyvalues[i]]={}

                # 2) Go to the next level...
                self.explorabledata=self.explorabledata[groupbyvalues[i]]
                                
            else:
            
                # We are at the DATA level: insert the data.
                # Check if objects for storing the data, for this last keyword, are already here. If not,
                # create this last keyword, link it with a list, and fill it with the data objects.
                # Then, pass the data values to the objects, they will know how to handle it.
           
                if not self.explorabledata.has_key(groupbyvalues[i]):

                    # Create the list where to put objects that will accept data
                    self.explorabledata[groupbyvalues[i]]=[]

                    # Set the shortcut to where to insert data
                    wheretoinsertdata=self.explorabledata[groupbyvalues[i]]
                    
                    # Create the objects that will accept data
                    for dataobject in self.dataobjects:

                        # Now dataobject is the name of the object (string), but
                        # we have to call the real object from it (Improve Ref: #0002)
                        obj = globals()[dataobject]
                        wheretoinsertdata.append(obj())

                else:

                    # Just set the shortcut to where to insert data
                    wheretoinsertdata=self.explorabledata[groupbyvalues[i]]
               
                
                # And now, insert the data!
                for j in range(len(wheretoinsertdata)):

                    wheretoinsertdata[j].add(datavalues[j])
                    # wheretoinsertdata[j] is the data object
                    # datavalues[j] is the data in the msg that has to go in that object


   



#######################################################################
#   Class Bins                                                        #
#######################################################################

class Bins:

    """
    This is the Bin class, the less basic container
    """

    #---------------------------------------------------------------------------
    def __init__(self, parentmetric, window, nbins, groupbykeys, dataobjects, handle_unordered):

        # Local vars..
        self.parentmetric=parentmetric
        self.window=window
        self.nbins=nbins
        self.groupbykeys=groupbykeys
        self.dataobjects=dataobjects
        self.handle_unordered=handle_unordered

        # Initial vars..
        self.currentBin=0
        self.binValidFrom=None;
        self.binValidTo=None;
        self.binDuration=(self.window/self.nbins)

        # Initialize every bin
        self.bins=[]
        for i in (range(0,nbins)):
            self.bins.append(Bin(self.groupbykeys,self.dataobjects))
            
        # Initialize the saved "bin" (the last outdated one)
        self.savedBin=Bin(self.groupbykeys,self.dataobjects)
        
        # Ready flag:
        self.ready=False
            

    #---------------------------------------------------------------------------
    def addData(self, groupbyvalues, datavalues, msg_timestamp, msg_epoch=None):
    
        # This function takes care to add data and to shift bins if necessary,
        # dealing with unordered / late arrival messages.
   
        # Convert message timestamp in epoch seconds
        msg_epoch_timestamp = timestamp_to_epoch(msg_timestamp, msg_epoch) 
        
        # If we are at the very first step (the first message pushes the start button):
        if self.binValidFrom==None:
            self.binValidFrom  = msg_epoch_timestamp
            self.binValidUntil = msg_epoch_timestamp + self.binDuration
            
            debug(self.parentmetric +
                  ": Shifting bins, now using bin #" + str(self.currentBin) +
                  ", valid from " + str(datetime.datetime.fromtimestamp(self.binValidFrom)) +
                  " to " + str(datetime.datetime.fromtimestamp(self.binValidUntil)) +                  
                  " (msg_timestamp: " + str(msg_timestamp) + ")")


        # 1) Check if the message timestamp preceeds the beginning of the current bin. 
        # if so, it means that we are facing an unordered message, or a late arrival

        if msg_epoch_timestamp < self.binValidFrom:
            
             # (epoch_delta it's calcultaed from the beginning of the bin)
            epoch_delta = msg_epoch_timestamp - self.binValidFrom
                            
            # Check if the Metric wants to estimate the delay of late messages arrivals..
            if 'EstimateDelay' in self.dataobjects:
                
                # Get its position in the list of the data objects...
                i = self.dataobjects.index('EstimateDelay')
                
                # And prepare the data to be inserted in the right place
                datavalues[i]=abs(epoch_delta)


            # Okay, now use one of the available policies to handle the late arrival:

            #______________________
            # a) skip: 
            # This policy will just trash old messages
            
            if self.handle_unordered == 'skip':
                debug(self.parentmetric + 
                      ": Message skipped since too old (epoch_delta: "+str(epoch_delta) + " timestamp: " + str(msg_timestamp))
                return
                            
            #______________________
            # b) rewrite_history:
            # This policy will add to the right (old) bin, if still exists, otherwise will trash the message.
            
            if self.handle_unordered == 'rewrite_history':
                #debug(self.parentmetric + 
                #      ": rewrite_history not yet implemented (epoch_delta: "+str(epoch_delta) + " timestamp: " + str(msg_timestamp))
                #print "ERROR: rewrite_history not yet implemented"
                #sys.exit(1)
                #return
                
                
                # So we have a late arrival.
                
                # - current message timestamp is: msg_epoch_timestamp
                # - current validity is: self.binValidFrom
                # - window is : self.window
                # - bin lenght in seconds is: self.window/self.nbins

                # Compute which should be the right bin:
                # -1 bin: self.binValidFrom - (self.window/self.nbins)
                
                #print ""
                #print "[rewrite history]: self.currentBin= " +str(self.currentBin)
                #print "[rewrite history]: msg_epoch_timestamp= " +str(msg_epoch_timestamp)
                #print "[rewrite history]: self.binValidFrom= " +str(self.binValidFrom) 
                        
                i=1
                while (msg_epoch_timestamp < (self.binValidFrom - (self.window/self.nbins)*i)):
                    #print "[rewrite history]: prev bin.."
                    i=i+1
                

                # The bin we were lookign for is cur_bin-1
                # First: is this number grater than self.nbins-1?
                #print "[rewrite history]: ----- right bin(i): cur-"+str(i)

                if i > (self.nbins-1):
                    print "[rewrite history]: msg too old, giving up"
                    # Message too old, give up.. 
                    return
                
                # Else, go backward untill the right bin:
                
                rightBin_index=self.currentBin-i
                #print "[rewrite history]: right bin: "+str(rightBin_index)
                # We have a circular list, we have to take care about it...                
                if rightBin_index<0:
                    rightBin_index=self.nbins+rightBin_index
                
                #print "[rewrite history]: right bin: "+str(rightBin_index)
                # 3) Add the data to the right bin
                self.bins[rightBin_index].addData(groupbyvalues, datavalues)
                
                return

                
                
                

            #______________________
            # c) percent_threshold:
            # This policy will accept the message in the current bin even if it is too old,
            # but not more than a given percentage of the current bin duration the default is 10%.
            
            if self.handle_unordered == 'percent_threshold':
                if (abs(epoch_delta) <= (binDuration/10)):
                    pass
                else:
                    return
   
            #______________________
            # d) time_threshold:
            # This policy will accept the message in the current bin even if it is too old,
            # but not more than a given time in seconds. This is supposed to be estimated analyzing the
            # mean delay of unordered messages or according the infrastructure
            #
            # Scribe flush timeout is 30 seconds, so this is the default setting.
            
            time_threshold=31
            
            if self.handle_unordered == 'time_threshold':
                if (abs(epoch_delta) <= time_threshold):
                    debug(self.parentmetric +
                          ": WARNING: Unordered message accepted since not older than " + str(time_threshold) +
                          " sec (epoch_delta: " + str(epoch_delta) + " timestamp: " + str(msg_timestamp))
                    pass
                    
                else:
                    debug(self.parentmetric +
                          ": WARNING: Unordered message skipped since older than " + str(time_threshold) +
                          " sec (epoch_delta: " + str(epoch_delta) + " timestamp: " + str(msg_timestamp))
                    return
                    
            #______________________
            # *) No policy defined.. 
            else:
                print "ERROR: no valid policy for unordered messages given"
                sys.exit(1)

        
        # 2) Check if we need to shift the bins (because the the message is too new for this bin).
        while ( msg_epoch_timestamp >= self.binValidUntil ):
        
            # Yes, we need to shift. First of all save the bin
        
        
            # Set current bin validity
            self.binValidFrom  = self.binValidUntil
            self.binValidUntil = self.binValidFrom + self.binDuration

            # We need to use a new bin: a circular list has been implemented for this.    
            self.currentBin += 1

            # Overflow? Go back to the bin #0 and start cycling
            # PLUS: set the ready flag!! We can now start taking data out from the bins.
            if (self.currentBin >= self.nbins):
                self.currentBin=0
                if not self.ready:
                    debug(self.parentmetric + ": Ready to give data")
                    self.ready=True

            # Save the bin that is going to be trashed, this is done 
            # to be able to give back data for bins -1, 0, 1, 2,..., n-1: for a total of n.
            self.savedBin.reset()
            self.savedBin.mergeWith(self.bins[self.currentBin])
            
            # Reset the current bin
            self.bins[self.currentBin].reset()

            debug(self.parentmetric +
                  ": Shifting bins, now using bin #" + str(self.currentBin) +
                  ", valid from " + str(datetime.datetime.fromtimestamp(self.binValidFrom)) +
                  " to " + str(datetime.datetime.fromtimestamp(self.binValidUntil)) +                  
                  " (msg_timestamp: " + str(msg_timestamp) + ")")
    
        # 3) Add the data to the right bin
        self.bins[self.currentBin].addData(groupbyvalues, datavalues)
        
        # Hard Debug...
        # debug(self.parentmetric+": Adding data to bin #"+str(self.currentBin)+", hour: "+timestamp)


    #---------------------------------------------------------------------------
    def mergeAll(self):
    
        # Create an empty result bin (in wich to merge all the other bins)
        resultBin=Bin(self.groupbykeys,self.dataobjects)
    
        # For every bin, merge it with the newly created one
        for i in range(0,self.nbins):
            resultBin.mergeWith(self.bins[i])
    
        return resultBin
        
    #---------------------------------------------------------------------------
    def mergeAllShiftedByOne(self):
    
        # Create an empty result bin (in wich to merge all the other bins)
        resultBin=Bin(self.groupbykeys,self.dataobjects)
    
        # For every bin except the last one, merge it with the newly created one
        for i in range(0,self.nbins):
        
            # Merge, except the current:
            if self.currentBin != i: 
                resultBin.mergeWith(self.bins[i])
                
        # And now merge with the saved one (the minus one)
                
        resultBin.mergeWith(self.savedBin)
        
        
        # THE MISSING STEP (IDIOT!)
        # Compute the data inside the bin!!!
     
        self.recursive_compute_data(resultBin.data, 0, self.groupbykeys)

        return resultBin

        
    #---------------------------------------------------------------------------        
    def recursive_compute_data(self, bin, depth, groupbykeys):
    
        for subdic in bin:           
            if not depth==len(groupbykeys)-1:
                # Again        
                self.recursive_compute_data(self, bin[subdic], depth+1, groupbykeys) 
            else:
                pos=0
                
                # Here we are at the datalevel
                for dataobject in bin[subdic]:


                    #for more verobose printing, unvomment the following
                    #print self.dataobjects[pos]+"("+self.datakeys[pos]+"):",
                    #print str(dataobject.getData())
                    
                    bin[subdic][pos]=dataobject.getData()
                    
                    pos+=1



    #---------------------------------------------------------------------------
    def getData(self, method='All'):
    
        if method=='All':
        
            # Merge every bin into a object of type "Bin", consistent with
            # the other bins (parameters are the same, they are stored in the 
            # Bins object of every metric).
            resultBin=self.mergeAll()
            
        elif method=='AllShiftedByOne':
        
            # Merge every bin into a object of type "Bin", except the last one to avoid 
            # having incomplete data, and with the previously trahed one ( we are shifting
            # by one)
            resultBin=self.mergeAllShiftedByOne()
            
        else:
            return {}
                
               
        # Extract and return the aggregated data
        return resultBin.getData()




#######################################################################
#   Class Metric                                                      #
#######################################################################

class Metric:

    #---------------------------------------------------------------------------
    def __init__( self, name, window, nbins, conditions, groupbykeys, data, handle_unordered):

        # Local vars..

        # - name, string
        self.name=name
        
        # - window, int
        self.window=int(window)
        
        # - nbins, int
        self.nbins=int(nbins)

        # - conditions, string    
        self.conditions=conditions

        # - handle_unordered, string
        self.handle_unordered=handle_unordered

        # - groupbykeys, list
        self.groupbykeys=groupbykeys

        # - data, dictionary

        # Convert from list of touples to two lists
        # (should be changed in future, to use the dictionary everywhere instead of two separate lists).
        self.datakeys=[]
        self.dataobjects=[]
        
        for touple in data:
            self.datakeys.append(touple[0])
            self.dataobjects.append(touple[1])
        
               
        # Define the "safe" functions. Only needed to be able to use complex statements
        # in the conditions field. Uncomment and see the function checkconditions() to enable it.
        # self.safefunctions={'True':True,'False':False}   
            
        # Compile the test that decide if the message being evaluated belongs to the metric:
        try:                
            if conditions!='NONE':
            
                # Compiling the test makes a speedup of about 10%
                self.test=compile(conditions, '', 'eval')

                # For benchmarking without compiling the test:
                # self.test=conditions
     
        except:
        
            # If we were not able to compile the test for whatever reason, we cannot go ahead.
            # Print the exception and exit.
            print self.name+": Unexpected error while compiling the metric (Err except: "+str(sys.exc_info()[0])+")"
            sys.exit (1)


        # Just a counter...
        self.accepted=0

        # Create the bins
        self.metricBins=Bins(self.name, self.window, self.nbins, self.groupbykeys, self.dataobjects, self.handle_unordered)
        
        # Debug...
        # print self.metricBins.bins



    #---------------------------------------------------------------------------
    def addData(self, msg):

        # Fill the groupbyvalues and datavalues lists:

        groupbyvalues=[]
        for key in self.groupbykeys:
            groupbyvalues.append(msg[key])


        # --KEYPOINT--        
        # Prepare the datavalues
        
        # There are some special keywords that will affect the data preparation,
        # this is important to keep in mind in case of further editing.

        datavalues=[]
        
        for key in self.datakeys:

            # This handles objects that have to operate on the entire message
            # (i.e.: ListAndMerge(KEYVALUES) or DummyStore(KEYVALUES))
            if key=='KEYVALUES':
                datavalues.append(msg)
        
            # This handles an object without argouments (i.e.: Counter() )
            elif key=='':
                datavalues.append('')
        
            # In all the other cases, take the right value for the keyword
            else:
                datavalues.append(msg[key])



        # Add the data to the bins        
        if msg.has_key('EPOCH'):
            self.metricBins.addData(groupbyvalues,datavalues,msg['TIMESTAMP'],msg['EPOCH'])
        else:
            self.metricBins.addData(groupbyvalues,datavalues,msg['TIMESTAMP'])
        

        # If you do not have a try-except handler at an higher level, i.e. you are using
        # this module standalone, you may want to do something like:
        
        # try:
        #     self.metricBins.addData(groupbyvalues,datavalues,msg['TIMESTAMP'])
        # except:
        #     debug( "ERROR, caused by:" )
        #     debug( msg )
                   


    #---------------------------------------------------------------------------
    def getData(self, method='returnDict'):

        # How to get data out from the metric. The standard method is returnDict, which gives back
        # a python dictionary, in which bins are already merged and final data (like averages) is already computed

        if method=='returnDict':
            return self.metricBins.getData('All')
            
        if method=='AllShiftedByOne':
            return self.metricBins.getData('AllShiftedByOne')

        # The next functions are here for debugging / proof of concepts.
        # The idea is that the metric should give just the dictionary
        # cointaining the aggregated data already computed, and then
        # should be up to the next layer how to deal with it.
        if method=='print':
            print ""
            print "====== "+self.name+" ======"
            recursive_print(self.metricBins.getData(),0, self.groupbykeys, self.datakeys)

        if method=='print_std':
            print ""
            print "====== "+self.name+" ======"
            recursive_print_std(self.metricBins.getData(),0, self.groupbykeys)



    #---------------------------------------------------------------------------
    def checkconditions(self, msg):

        # 1) Check that the current message has every keyword needed for the metric:
    
        # a) on the groupby
        for keyword in self.groupbykeys:
            if not msg.has_key(keyword):
                return False

        # b) on the data (if not empty)
        for keyword in self.datakeys:
            
            # Skip the test if we have a special keyword:
            if (keyword=='KEYVALUES' or keyword==''):
                pass
            
            # Switch to the next one, please...    
            # if keyword not in ['KEYVALUES','']:        
                
            else:
                if not msg.has_key(keyword):
                    return False

        # 2) Check that the conditions match our metric 
        try:
   
            if self.conditions!='NONE':
            
                # To be able to use complex statements in the conditions field, use
                # the following. but it costs +10% execution time.
                
                # safedomain = dict(msg.items() + self.safefunctions.items())
                safedomain = msg

                if eval(self.test, {"__builtins__":None}, safedomain):                                     
                    # Message accepted from metric
                    
                    # If you want hard verbosity debugging uncomment the following...        
                    # print self.name+": METRIC MATCHED"    
                    return True
                    
                else:   
                    # Message rejected from metric
                    return False
                    
            else:
                return True
                
        except:
        
            # If there is an exception here, most likely it is because one of the keywords were
            # not found in the message, so the message is not even a candidate for this metric

            return False
        
        # If you want hard verbosity debugging uncomment the following...        
        # print self.name+": METRIC MATCHED"    



    #---------------------------------------------------------------------------
    def apply(self, msg):      
        if msg.has_key('MSG_NOT_VALID'):
            return


        # Check that there is the TIMESTAMP keyword, which it's mandatory:        
        if msg.has_key('TIMESTAMP'):

            # Ok, now add some custom keywords:

            # For Count and Delay special object
            msg['COUNT']='+1'       # Deprecated...
            msg['DELAY']='0'
                     
            # For grouping by nothing:
            msg['NONE']='Grouped by NONE'
            
            # Easier date for grouoing by           
            msg['DATE']=msg['TIMESTAMP'].split('T')[0]
        
        
            # Evaluate the message: does it match the metric?
            if self.checkconditions(msg):

                # Add this data to the metric
                self.addData(msg)
                
                # Increrase the counter of accepted messages
                self.accepted+=1









#######################################################################
#                                                                     #
#        Helper functions. To be moved to a "Helper" class?           #
#                                                                     #
#######################################################################

     
## Parse a file and load metrics data...
#---------------------------------------------------------------------------
def parseMetrics(metricsfile):
                

    # THIS IS THE WORST PARSER __EVER__ .  Sorry. It has to be fixed soon.

    debug("MetricCommon.loadMetrics: Loading metrics from " + metricsfile)
    
    # Load the metric from the def file
    f = open(metricsfile)
    lines = f.readlines()
    f.close()
    
    #Print the metric on the output as reference
    #for line in lines:
    #    print line,

    # Initilazion...
    metrics=[]
    metric={}


    # extract keyvalues:
    for line in lines:

       
        # Check if the metic we are analyzing
        if '</metric>' in line:

            
            # The metric is ended, lety's check that we have all the needed keywords:
            if metric.has_key('name') and metric.has_key('window') and metric.has_key('conditions') and metric.has_key('groupbykeys') and metric.has_key('data') and metric.has_key('handle_unordered') and metric.has_key('nbins'):

                debug('MetricCommon.loadMetrics: Found valid metric "' + metric['name'] + '"')


                # Perfect, we have all the keys. Let's now rearrange the data array:
                
                # Parsing (will have to be included in the metricommon.loadmetrics
                metric['groupbykeys']=metric['groupbykeys'].replace(' ', '').split(',')
                
                # rimuovere tutti gli spazi e fare lo split sulla virgola. poi togliere l'ultimo carattere che 
                # (sara' la parentesi chiusa) e fare lo split sulla parentesi aperta
                data=metric['data'].replace(' ', '').split(',') # ma questo copia o linka?
                
                metric['data']=[]
               
                for datadef in data:
                                         #      the data key       #    #  the data object  #
                    metric['data'].append( (datadef.split('(')[1][:-1],datadef.split('(')[0]) )

                

                # If yes, flush data:
                metrics.append(metric)

                # Clear dictionary (will this work?!
                metric={}

                # Print new metric
                # print metric


        # Extract the key:
        index =0
        while line[index]==" ":
            index+=1
        index1=index

        # If there is the comment symbol, skip
        if line[index]=='#':
            continue
        
        try:
            while line[index]!=":":
                index+=1
            index2=index
        except:
            continue
            # it was not a configuration line

        keyword=line[index1:index2]
        #print "keyword: " + keyword

        #Skip colon
        index+=1

        # Extract the value:
        while line[index]==" ":
            index+=1
        index1=index
        #print line[index1:]

        # Go to the end of line
        while line[index]!="\r" and line[index]!="\n":
            index+=1

        shifted=0
        # Remove spaces backward
        while line[index]==" " or line[index]=="\r" or line[index]=="\n":
            index-=1
            shifted=1

        if shifted==1:
            index=index+1

        index2=index

        value=line[index1:index2]
        #print "value: " + value

        metric[keyword]=value


    return metrics




# Parse a file and load metrics data...
#---------------------------------------------------------------------------
def loadMetrics(path):

    # Prepare the array..
    metrics=[]


    for metricfile in glob.glob(path):

        # Load the metric from file
        parsed_metrics=parseMetrics(metricfile)

        # For every metric read..
        for metric in parsed_metrics:

            # Debugging...
            #print metric

            # Create the metric
            #def __init__( self, name, window, nbins, conditions, groupbykeys, data, handle_unordered):
            metrics.append(Metric(metric['name'],
                                    metric['window'],
                                    metric['nbins'],
                                    metric['conditions'],
                                    metric['groupbykeys'],
                                    metric['data'],
                                    metric['handle_unordered']))
                                            
    return metrics











## Pretty printing...
#---------------------------------------------------------------------------
def recursive_print(bin, depth, groupbykeys, datakeys):
    for subdic in bin:           
        print ""
        for i in range(0,depth):
            print "\t",                

        print str(subdic)+":"
        if not depth==len(groupbykeys)-1:
            

            # Again          
            recursive_print(bin[subdic], depth+1, groupbykeys, datakeys) 
        else:

            pos=0
            # Here we are at the datalevel
            for dataobject in bin[subdic]:
                #print type(dataobject)
                #print dataobject

                
                # print a nice list:
                if type(dataobject.getData()) is list:
                    print ""
                    # Align it
                    for i in range(0,depth+1):
                        print "\t",
                    print str(type(dataobject)).split('.')[1].split("'")[0]+"("+datakeys[pos]+"):" #.split('.')[1].split("'")[0]

                    for item in dataobject.getData():                        
                        # Align it
                        for i in range(0,depth+1):
                            print "\t  ",
                        
                        print item

                else:
                    print ""
                    # Align it
                    for i in range(0,depth+1):
                        print "\t",   
                    print str(type(dataobject)).split('.')[1].split("'")[0]+"("+datakeys[pos]+"):"

                    # Align it
                    for i in range(0,depth+1):
                        print "\t  ", 
                    print str(dataobject.getData())
                pos+=1

            print ""


## Pretty printing standard...
#---------------------------------------------------------------------------
def recursive_print_std(bin, depth, groupbykeys):
    for subdic in bin:           
        print ""
        for i in range(0,depth):
            print "\t",                

        print str(subdic)+":"
        if not depth==len(groupbykeys)-1:
            

            # Again          
            recursive_print_std(bin[subdic], depth+1, groupbykeys) 
        else:

            pos=0
            # Here we are at the datalevel
            for dataobject in bin[subdic]:
                # Align it
                for i in range(0,depth+1):
                    print "\t",
                #for more verobose printing, unvomment the following
                #print self.dataobjects[pos]+"("+self.datakeys[pos]+"):",
                print str(dataobject.getData())
                pos+=1

            print ""


# This is a fuction useful for debugging..
#---------------------------------------------------------------------------
def printTree(tree, depth = 0):
    if tree == None or len(tree) == 0:
        print "\t" * depth, "-"
    else:
        for key, val in tree.items():
            print "\t" * depth, key
            printTree(val, depth+1)








