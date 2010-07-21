from itertools import chain, islice

class ListChain(object):
    """
    Chains multiple subquerysets (possibly of different models) and behaves as
    one queryset.  Supports minimal methods needed for use with
    django.core.paginator.
    """

    def __init__(self, *subquerysets):
        self.querysets = subquerysets
        self.ends = []
        self.ends.append(0)
        
        lastend = 0
        for qs in self.querysets:
            lastend = lastend + len(qs)
            self.ends.append(lastend)

    def __len__(self):
        """
        Performs a .count() for all subquerysets and returns the number of
        records as an integer.
        """
        return self.ends[len(self.ends)-1]

    def _clone(self):
        "Returns a clone of this queryset chain"
        return self.__class__(*self.querysets)

#    def _all(self):
#        "Iterates records in all subquerysets"
#        result = []
#        for qs in self.querysets:
#            result += list(qs)
#        return result
            

    def __getitem__(self, ndx):
        """
        Retrieves an item or slice from the chained set of results from all
        subquerysets.
        """
        
        subindex = 0
        for end in self.ends:
            if ndx < end:
                break
            subindex = subindex + 1
        
#        print ndx-self.ends[subindex-1]
        ret = self.querysets[subindex-1][ndx-self.ends[subindex-1]]
#        if ret.__str__() == "/castor/cern.ch/grid/alice/vo_box":
#        print ndx, ret
        #[self.ends[subindex-1]+ndx]
        return ret
            
