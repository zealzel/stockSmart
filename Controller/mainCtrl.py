# -*- coding: utf8 -*-

from Model import model

class orderType:
    new=-1
    asc=0
    desc=1
    @staticmethod
    def toString(type):
        if type==orderType.asc:
            return 'asc'
        elif type==orderType.desc:
            return 'desc'
        else:
            return ''
    
class Controller():
    tblName='Final'
    cd=model.condition()  
    def __init__(self, db, frame):  
        self.myDb=db
        self.frame=frame
        self.grid = frame.gridPanel
        
        self.frame.setController(self)
        self.grid.setController(self)
        
        self.updateCondition('')
        self.fieldsToSort=[]
        self.orderString=''
        self.updateTable(Controller.tblName)   
        
        self.grid.setPopupMenu()     
        
    def updateCondition(self,conditionString):
        self.conditionString=conditionString
    
    def updateAscType(self,field,append=False):
        if append:
            if field in self.fieldsToSort:
                pass
            else:
                self.fieldsToSort.append([field,orderType.new])
        else:
            if len(self.fieldsToSort)==1:
                print "f1=%s,f2=%s" % (self.fieldsToSort[0],field)
                print "type(f1)=%s,type(f2)=%s" % (type(self.fieldsToSort[0]),type(field))
                if self.fieldsToSort[0][0]!=field:
                    self.fieldsToSort=([field,orderType.new],)
                    print "-----1"
                    #self.orderString='order by %s %s' % (field,orderType.toString(orderType.new))

            else:
                for e in self.fieldsToSort: self.fieldsToSort.remove(e)
                self.fieldsToSort.append([field,orderType.new])
                print "-----2"
                #self.orderString='order by %s %s' % (field,orderType.toString(orderType.new))
        
        print "self.fiedltoStart=%s" % self.fieldsToSort
        self.orderString=['']*len(self.fieldsToSort)
        for f in self.fieldsToSort:
            print "f=%s" % f
            print "f[1]==orderType.asc?%s" % f[1]==orderType.asc
            if f[1]==orderType.new:
                f[1]=orderType.asc
            elif f[1]==orderType.asc:
                f[1]=orderType.desc
            else:
                f[1]=orderType.asc
            print "f=%s" % f
            print self.fieldsToSort.index(f)
            print self.orderString[self.fieldsToSort.index(f)]
            self.orderString[self.fieldsToSort.index(f)]=' '.join([f[0],orderType.toString(f[1])])
            print "orderString=%s" % self.orderString[self.fieldsToSort.index(f)]
        
        if self.orderString=='':return
        else:
            self.orderString='order by '+','.join(self.orderString)
        print "orderString=%s" % self.orderString
        
    def updateTable(self, tableName):
        self.myTable=self.myDb.getTable(Controller.tblName)
        print "sql command=%s" % "select * from %s %s %s" % (tableName,self.conditionString,self.orderString)
        self.selectRecord("select * from %s %s %s" % (tableName,self.conditionString,self.orderString))
        
    def selectRecord(self,sqlString):
        records = self.myDb.cursor.execute(sqlString).fetchall()  
        if len(records) != 0:
            entries = [model.Entry(r) for r in records]
        else:
            entries = []
        self.grid.updateData(entries)
    
    def getTblFieldByIndex(self,i):
        fields=self.myTable.getFields()
        return fields[i]
  
        