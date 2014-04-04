import re

def printList(l):
    for el in l:
        if type(el)!=list:
            print el
        else:
            el=[str(d) for d in el]
            e='\t'.join(el)
            print e
            
def extractNumeric(string):
    regex=re.compile('[\d]+')
    match=regex.findall(string)
    return match[0]


def list2dict(l1,l2):
    pair=zip(l1,l2)
    return dict(pair)

class Enumerator(object):
    def __init__(self, *names):
        self._values = dict((value, index) for index, value in enumerate (names))
    def __getattribute__(self, attr):
        try:
            return object.__getattribute__(self,"_values")[attr]
        except KeyError:
            return object.__getattribute__(self, attr)
    def __getitem__(self, item):
        if isinstance (item, int):
            return self._values.keys()[self._values.values().index(item)]
        return self._values[item]
    def __repr__(self):
        return repr(self._values.keys())
    
stkType=Enumerator("sii","otc")