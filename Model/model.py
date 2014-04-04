# -*- coding:utf-8 -*-

import wx
import wx.grid               
from pysqlite2 import dbapi2 as sqlite3

class Entry:
    def __init__(self, data):
        self.data = data
    
    def toString(self):
        dataPrint = '\t'.join([str(s) for s in self.data])
        return dataPrint
        
class TableModel(wx.grid.PyGridTableBase):
    title1=[u'股票代碼',u'公司',u'十年均現金股利',u'股利價值',u'均本益比',u'EPS TTM',u'EPS成長率',u'淨值成長率',u'銷售成長率',u'現金流成長率',u'EPS預估成長率', \
             u'成長價值',u'現價',u'評估價格',u'俗價比','股利價比',u'1年均配息率',u'3年均配息率',u'6年均配息率',u'10年均配息率',u'1年均ROE',u'3年均ROE',u'6年均ROE',u'10年均ROE']
    digits=[-1,-1,2,2,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]
    colType=[0,0,1,1,1,1,2,2,2,2,2,1,1,1,1,1,2,2,2,2,2,2,2,2]
    FORMAT_TEXT,FORMAT_VALUE,FORMAT_PERCENTANGE=(0,1,2)
    
    def __init__(self, titles, entries): 
        wx.grid.PyGridTableBase.__init__(self)
        self.entries = entries
        self.colLabels = titles
   
    def GetNumberRows(self):
        if len(self.entries) != 0:
            print "row=%s" % len(self.entries)
            return len(self.entries)
        else:
            print 'row = 0'
            return 1
        
    def GetNumberCols(self):
        if len(self.entries) != 0:
            return len(self.entries[0].data)

    def GetColLabelValue(self, col):
        return self.colLabels[col]
    
    def isEditable(self,row,col):
        return False
    
    def IsEmptyCell(self, row, col):
        return False
        
    def GetValue(self, row, col):
        try:
            if TableModel.colType[col]==TableModel.FORMAT_TEXT:
                return self.entries[row].data[col]
            elif TableModel.colType[col]==TableModel.FORMAT_VALUE:
                digFormat="{0:.%sf}" % TableModel.digits[col]
                return digFormat.format(self.entries[row].data[col])
            else:
                digFormat="{0:.%s%%}" % TableModel.digits[col]
                return digFormat.format(self.entries[row].data[col])
        except:
            pass
            #print "r,c=%s,%s, ---> %s" % (row,col,self.entries[row].data[col])

    def SetValue(self, row, col, value):
        pass
        
    def addEntry(self,entry):
        self.entries.add(entry)

class condition():
    CONDITION_EQUALTO,CONDITION_LARGERTHAN,CONDITION_SMALLERTHAN,CONDITION_LIKE,CONDITION_KEYWORD=(0,1,2,3,4)
    CONDITION_SYMBOL=("=",">","<"," like "," like ")
    def __init__(self):
        self.cds=[]
    
    def addCondition(self,col,value,cond):
        if value!='':
            if cond==condition.CONDITION_LIKE:
                self.cds.append("%s%s'%s%%'" % (col,condition.CONDITION_SYMBOL[cond],value))       
            elif cond==condition.CONDITION_KEYWORD:
                self.cds.append("%s%s'%%%s%%'" % (col,condition.CONDITION_SYMBOL[cond],value))
            else:
                self.cds.append("%s%s%s" % (col,condition.CONDITION_SYMBOL[cond],value))
                
    def clear(self):
        del self.cds[:]
        
    def getCondition(self):
        if self.cds!=[]:
            conditionString='where %s' % (' and '.join(self.cds),)
        else:
            conditionString=''
        return conditionString


       
