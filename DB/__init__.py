# -*- coding: utf8 -*-
import sys
#import sqlite3
from pysqlite2 import dbapi2 as sqlite3
import Util
from datetime import datetime
import lxml.html
from UrlConnection import *
from datetime import datetime,timedelta
import time
import re
from os import path
import socket
import sys
from Util import stkType


class date:
    now=datetime.now()
    
class stType:
    sii='sii'
    otc='otc'
    
class db:
    cursor=None
    conn=None
    def __init__(self,db):
        # db as xxx.db sqlite file
        print 'db file=%s' % db
        print"realpath=%s" % path.realpath(db)
        self.connect(db)
        
        print 'after connect'
        self.tables=[]
        self.initTables()
    
    @staticmethod
    def enableExtension(enabled):
        db.conn.enable_load_extension(enabled)
    
    @staticmethod
    def loadExtension():
        try:
            db.enableExtension(True)
            file="%s/%s" % (sys.path[0],'libsqlitefunctions.dylib')
            db.cursor.execute('select load_extension("%s")' % file)
            print 'select load_extension("%s")' % file
        except Exception as ex:
            print ex
            return
    
    def getCursor(self):
        db.cursor=db.conn.cursor()
    
    def closeCursor(self):
        db.cursor.close()
    
    def connect(self,Db):
        print "db connect..."
        db.conn=sqlite3.connect(Db)
        self.getCursor()
        
    def addTable(self,table):
        self.tables.append(table)
    
    def addTables(self,tables):
        for table in tables:
            self.tables.append(table)
    
    def getTable(self,tblName):
        table=None
        for t in self.tables:
            if t.name==tblName:
                table=t
        return table
        
    def createView(self,viewName,query):
        try: self.cursor.execute('drop view %s' % (viewName,))
        except: pass
        self.cursor.execute('create view %s as %s' % (viewName,query))
        
    def updateDB(self):
        try:
            self.conn.commit()
        except sqlite3.OperationalError as ex:
            print ex

    def initTables(self):
        table=[(TableBasic,'stock'), \
               (TableShortName,'stockShortName'), \
               (TableDividend,'Dividend'), \
               (TableIncome,'Income'), \
               (TableBalance,'Balance'), \
               (TableSale,'Sale'), \
               (TableCash,'CashFlow'), \
               (TableMPrice,'Price'), \
               (TablePER,'PER'), \
               (TableAvePer,'AvePER'), \
               (TableTenYearsAveDiv,'AveDIV'), \
               (TableEpsRateForRegress,'EpsRateForRegress'), \
               (TableBvpsRateForRegress,'BvpsRateForRegress'), \
               (TableSaleRateForRegress,'SaleRateForRegress'), \
               (TablePriceNew,'PriceNew'), \
               (TableCashRateForRegress,'CashRateForRegress'), \
               (TableCashRateForRegress,'ROE'), \
               (TablePayRatio,'PayRatio'), \
               (TableFinalValue,'Final')]
        
        tables=[]
        for i in range(len(table)): tables.append(table[i][0](self,table[i][1]))
        self.addTables(tables)
        #self.getTable("Dividend").enableUpdate(True) #ok
        #self.getTable("Income").enableUpdate(True) #ok
        #self.getTable("Balance").enableUpdate(True) #ok
        
        #self.getTable("CashFlow").enableUpdate(True) #ok
        
        #----OK
        #self.getTable("Sale").enableUpdate(True) #ok  
        #self.getTable("Price").enableUpdate(True) #ok
        #self.getTable("AvePER").enableUpdate(True) #ok
        #----OK
        #self.getTable("AveDIV").enableUpdate(True)
        
        #self.getTable("EpsRateForRegress").enableUpdate(True) #ok
        #self.getTable("BvpsRateForRegress").enableUpdate(True) #ok
        #self.getTable("SaleRateForRegress").enableUpdate(True) #ok
        #self.getTable("CashRateForRegress").enableUpdate(True) #ok
        #self.getTable("PayRatio").enableUpdate(True)
        
        
        self.getTable("PriceNew").enableUpdate(True)
        self.getTable("Final").enableUpdate(True)
                    
class Table:
    thisYear=date.now.year
    thisQuarter=(date.now.month-1)/3+1
    thisMonth=date.now.month
    def __init__(self,myDB,name,fields=[]):
        self.myDb=myDB
        self.name=name
        self.fields=fields
        self.urlSources=[]
        self.shouldUpdate=False
        
    def clearAll(self):
        try:
            db.cursor.execute('delete from '+self.name+' where rowid>0')
        except Exception as ex:
            print ex
            
    def insert(self,values,isAutoInc=False):
        if len(values)>0:
            if not isAutoInc:
                db.cursor.execute('insert into '+self.name+' values('+','.join(['?']*len(values))+')',values)
            else:
                db.cursor.execute('insert into '+self.name+ '('+ ','.join(self.fields) +') values(' + ','.join(values) +')')                      
                    
    def addUrlSource(self,urlconn):
        self.urlSources.append(urlconn)
    
    def enableUpdate(self,shouldUpdate):
        self.shouldUpdate=shouldUpdate
    
    def update(self,fields,values,ids,idValues):
        setField='=?,'.join(fields)+'=?'
        setId='=? and '.join(ids)+'=?'
        print 'update '+self.name+' set '+setField+' where '+setId, values+idValues
        db.cursor.execute('update '+self.name+' set '+setField+' where '+setId, values+idValues)
    
    def select(self,fields,where=None):
        # ex: where= ('Y=98','Q=1','eps>1.0')
        fields=','.join(fields)
        if where!=None:
            conditions=' and '.join(where)
        #print 'select '+fields+' from '+self.name+' where '+conditions
        cursor=db.cursor.execute('select '+fields+' from '+self.name+' where '+conditions)
        return cursor
    
    def getFields(self):
        record=db.cursor.execute("select type,tbl_name,sql from sqlite_master where tbl_name=?",(self.name,)).fetchall()
        print record
        for r in record:
            if r[0]=='table':
                regex=re.compile('create[\s]+table[\s]+%s[\s]*\([\w\s\,(\)]*\)' % self.name,re.IGNORECASE)
                str=regex.findall(r[2])[0]
                str=str[str.find('(')+1:-1]
                fields=str.split(',')
                fields=[e.strip().lower() for e in fields]
                for i in range(len(fields)):
                    if fields[i].find('foreign key')!=-1:
                        fields.pop(i)
                        continue
                    fields[i]=fields[i][:fields[i].find(' ')]
            elif r[0]=='view':
                pass
            else:
                pass
        return fields
        
        
    def modifyData(self,urlConn,*args,**kvargs):
        self.ls=list(args)
        self.kv=dict(kvargs)
        print args
        print kvargs
        print "in table: args=%s,kv=%s" % (self.ls,self.kv)
        
class TableBasic(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        self.addUrlSource(UrlConnBasic(stType.sii))
        self.addUrlSource(UrlConnBasic(stType.otc))
    
    def modifyData(self,urlConn,*args,**kvargs):
        html=urlConn.getHtml()
        el1=html.cssselect('table')[1].cssselect('tr')
        for i in range(1,len(el1)):
            tds=el1[i].cssselect('td')            
            data=[td.text_content().strip() for td in tds if tds.index(td) in (0,1,4,5,10,11)]  
            self.insert(data) 
 
    def update(self, type=stType.sii,code=''):
        self.clearAll()
        for s in self.urlSources:
            self.modifyData(s)        
        self.myDb.updateDB()
        
class TableShortName(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        self.addUrlSource(UrlConnShort(stType.sii))
        self.addUrlSource(UrlConnShort(stType.otc))
    
    def modifyData(self,urlConn,*args,**kvargs):
        html=urlConn.getHtml()
        el1=html.cssselect('table')[1].cssselect('tr')
        for i in range(1,len(el1)):
            tds=el1[i].cssselect('td')
            idNext=Util.extractNumeric(tds[0].text_content())
            if len(idNext)<4:continue       
            data=[td.cssselect('a')[0].text_content().strip() for td in tds if tds.index(td) in (0,1)]
            self.insert(data)
    
    def update(self, type=stType.sii,code=''):
        self.clearAll()
        for s in self.urlSources:   
            self.modifyData(s)        
        self.myDb.updateDB()

class TableDividend(Table):
    def __init__(self,myDB,name):
        fields=['dID','Y','stkID','cashDiv','stokDiv']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableDivUpdateRecord(myDB,'divUpdateRecord')
        self.addUrlSource(UrlConnDividend(stType.sii))
        self.addUrlSource(UrlConnDividend(stType.otc))
        
    def modifyData(self,urlConn,year):
        urlConn.updateUrlData({'YEAR':year}) 
        html=urlConn.getHtml(['big5_hkscs','ISO-8859-2'],reload=True)
        #el1=html.cssselect('table[class="hasBorder"] tr')
        #el1=html.cssselect('table')[1].cssselect('tr')
        el1=html.cssselect('table')
        
        print 'len(el1)=%s' % len(el1)
        stkid=''
        y=year
        for i in range(3,len(el1)-1):
            print "y=%s,i=%s" % (y,i)
            
            el2=el1[i]
            el3=el2.cssselect('tr')
            
            for j in range(2,len(el3)):
                tds=el3[j].cssselect('td')
                if len(tds)==0:continue # ignore the <th> tags
                
                idNext=Util.extractNumeric(tds[0].text_content())
                if idNext!=stkid: # take the first row data of certain stock, usually the 股東會認可之資料
                    try:     
                        data=[str(y)+'_'+str(idNext),str(y),idNext]
                        if y<98:
                            data=data+[td.text_content().strip() for td in tds if tds.index(td) in (5,6)] # need to confirm
                        else:
                            data=data+[td.text_content().strip() for td in tds if tds.index(td) in (9,12)]
                        for d in data:
                            if d=="": data[data.index(d)]=None
                        Util.printList(data)
                        self.insert(data)
                        stkid=idNext
                    except Exception as ex:
                        # exception caught, mainly the update situation
                        print ex
                        Table.update(self, self.fields, data, ('dID',),[data[0],])
    
    def update(self):
        thisYear=date.now.year-1911
        for y in range(89,thisYear+1):
            
            # if the dividend record DB has records before this year, ignore the subsequent action
            db.cursor.execute('select Y from '+ self.myRecord.name+' where Y=?',(y,))
            if db.cursor.fetchone()!=None and y<thisYear: continue
            
            for s in self.urlSources:       
                self.modifyData(s,y)
            
            self.myRecord.insert([str(y)], True)
            self.myDb.updateDB()
            
class TableDivUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y']
        Table.__init__(self, myDB, name,fields)

class TableIncome(Table):
    def __init__(self,myDB,name):
        fields=['iID','Y','Q','stkID','sales','cost','profit','expense','OpIncome','NonOpIncome','NonOpExp','PreTaxIncome','TaxExp','NetIncome','cumeps','eps']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableIncomeUpdateRecord(myDB,'incomeUpdateRecord')
        self.addUrlSource(UrlConnIncome(stType.sii))
        self.addUrlSource(UrlConnIncome(stType.otc))
        
    def update(self):  
        #y=2001
        y=2013
        while (y<self.thisYear+1):  
            q=1
            while (q<5):
                if y==self.thisYear and q>=self.thisQuarter:break
                # if the income record DB has records before this year or this quarter, ignore the subsequent action
                db.cursor.execute('select Y from '+ self.myRecord.name+' where Y=? and Q=?',(y,q))
                if db.cursor.fetchone()!=None:
                    print '%sQ%s already has income record' % (y,q)
                    if y<self.thisYear:
                        q+=1
                        continue 
                n=0 
                second=4
                while (n<2): # n=0:sii  n=1:otc
                    s=self.urlSources[n]
                    s.updateUrlData({'year':y-1911,'season':'0'+str(q)})
                    html=s.getHtml(['utf-8','ISO-8859-2'],True)             
                    el1=html.cssselect('table[class="hasBorder"] tr')
                    
                    # wait a few seconds if server-denying happens
                    if len(el1)==0:
                        second+=1
                        time.sleep(second)
                        continue
                    
                    stkid=''
                    for i in range(1,len(el1)):
                        tds=el1[i].cssselect('td')
                        if len(tds)==0:continue # ignore the <th> tags
                        
                        stkid=tds[0].text_content().strip()
                        try:     
                            data=[str(y)+'_'+str(q)+'_'+stkid,y,q,stkid,n]
                            #data=data+[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (2,3,4,5,6,7,8,9,10,16,18)] #comboned income before IFRS
                            data=data+[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (2,3,4,8,10,11)]
                            data=data+[tds[11].text_content().strip().replace(',','')]+[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (12,13,17,24)]
                            print data
                            for d in data:
                                if d=="" : data[data.index(d)]=None
                            #if data[-1]!=None and float(data[-1])==0.0:data[-1]=None

                            cumeps=data[-1]
                            if q==1: 
                                eps=float(cumeps)
                            else:
                                records=self.select(('cumeps',),('stkID='+str(stkid),'Y=%s and Q=%s' % (y,q-1))).fetchall()
                                if len(records)>0:
                                    eps=records[0][0]
                                else:
                                    eps=None
                                #cumeps=data[-1]
                                if eps!=None and cumeps!=None:
                                    eps=float(cumeps)-float(eps)
                            
                            records2=self.select(('eps',),('stkID='+str(stkid),'((Y=%s and Q<%s) or (Y=%s and Q>=%s))' % (y,q,y-1,q))).fetchall()
                            if len(records2)==4:
                                epsTTM_forPER=0
                                try:
                                    for r in records2: 
                                        epsTTM_forPER+=float(r[0])
                                except:
                                    epsTTM_forPER=None
                            else:
                                #print "stkID=%s" % stkid
                                #Util.printList(records2)
                                epsTTM_forPER=None
                            
                            records3=self.select(('eps',),('stkID='+str(stkid),'((Y=%s and Q<=%s) or (Y=%s and Q>%s))' % (y,q,y-1,q))).fetchall()
                            if len(records3)==3:
                                epsTTM=0
                                try:
                                    for r in records3: 
                                        epsTTM+=float(r[0])
                                    epsTTM+=eps
                                except:
                                    epsTTM=None
                            else:
                                #print "stkID=%s" % stkid
                                #Util.printList(records3)
                                epsTTM=None
                                
                            data=data+[eps,epsTTM_forPER,epsTTM]
                            self.insert(data)
                        except sqlite3.IntegrityError as ex:
                            # ignore if there's data in the record
                            print "%s already has record in %sQ%s" % (data[3],y,q)
                            #Table.update(self, self.fields, data, ('iID',),[data[0],])
                        except Exception as ex:
                            print "stkid=%s, error:%s" % (stkid,ex)
                    n+=1
                self.myRecord.insert([str(y),str(q)], True)
                self.myDb.updateDB()
                q+=1
            y+=1

class TableIncomeUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','Q']
        Table.__init__(self, myDB, name,fields)
        
class TableBalance(Table):
    def __init__(self,myDB,name):
        fields=['bID','Y','Q','stkID','curAsset','investment','fixAsset','intAsset','otherAsset','totAsset', \
                'curLbts','longLbts','otherLbts','totLbts','capital','additionalPaidInCap','retainEarning','equity','bvps']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableBalanceUpdateRecord(myDB,'balanceUpdateRecord')
        self.addUrlSource(UrlConnBalance(stType.sii))
        self.addUrlSource(UrlConnBalance(stType.otc))
        
    def update(self):
        #y=1996
        y=2013
        while (y<self.thisYear+1):  
            q=1
            while (q<5):
                if y==self.thisYear and q>=self.thisQuarter:break
                # if the income record DB has records before this year or this quarter, ignore the subsequent action
                db.cursor.execute('select Y from '+ self.myRecord.name+' where Y=? and Q=?',(y,q))
                if db.cursor.fetchone()!=None:
                    print '%sQ%s already has income record' % (y,q)
                    if y<self.thisYear:
                        q+=1
                        continue 
                n=0
                second=4
                while (n<2):
                    s=self.urlSources[n]
                    s.updateUrlData({'year':y-1911,'season':'0'+str(q)})
                    html=s.getHtml(['utf-8','ISO-8859-2'],reload=True)
                    
                    #el0=html.cssselect('table')
                    #el1=el0[1].cssselect('tr')
                    
                    el1=html.cssselect('table[class="hasBorder"] tr')
                    
                    
                    # wait a few seconds if server-denying happens
                    print len(el1)
                    if len(el1)==0:
                        second+=1
                        time.sleep(second)
                        continue
                    
                    stkid=''
                    try:
                        for i in range(1,len(el1)):
                            tds=el1[i].cssselect('td')
                            if len(tds)==0:continue # ignore the <th> tags
                            
                            stkid=tds[0].text_content().strip()
                            #if stkid!=2317: continue
                            try:     
                                data=[str(y)+'_'+str(q)+'_'+stkid,y,q,stkid]
                                #data=data+[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (2,3,4,5,6,7,8,9,11,12,13,14,15,19,22)] #combind balance before IFRS
                                
                                # after 2013
                                data=data+[tds[2].text_content().strip().replace(',','')]*2 \
                                         +[tds[3].text_content().strip().replace(',','')]*3 \
                                         +[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (4,5,6)] \
                                         +[tds[6].text_content().strip().replace(',','')] \
                                         +[td.text_content().strip().replace(',','') for td in tds if tds.index(td) in (7,8,9,10,16,20)]
                                  
                                for d in data:
                                    if d=="": data[data.index(d)]=None
                                Util.printList(data)
                                self.insert(data)
                            except sqlite3.IntegrityError as ex:
                                print ex
                                # ignore if there's data in the record
                                #print "%s already has record in %sQ%s" % (data[3],y,q)
                    except Exception as ex:
                            print ex
                    n+=1
                self.myRecord.insert([str(y),str(q)], True)
                self.myDb.updateDB()
                q+=1
            y+=1

class TableBalanceUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','Q']
        Table.__init__(self, myDB, name,fields)

class TableSale(Table):
    def __init__(self,myDB,name):
        fields=['sID','Y','M','stkID','Sales']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableSaleUpdateRecord(myDB,'saleUpdateRecord')
        self.addUrlSource(UrlConnSale())
    
    def hasRecords(self,y,m):
        db.cursor.execute('select Y from '+ self.myRecord.name+' where Y=? and M=?',(y,m))
        if len(db.cursor.fetchall())>0: # has records
            print '%sM%s already has income record' % (y,m)
            return True
        else:
            return False
    
    def IgnoreExtracting(self,y,m):
        if y<self.thisYear or (y==self.thisYear and m<self.thisMonth-3):
            print "year=%s, month=%s , ignore extracting..." % (y,m)
            return True
        else:
            return False
    
    def extractData(self,stType,y,m):
        s=self.urlSources[0]
        s.updateUrlData(stType,y-1911,m)
        html=s.getHtml(['ISO-8859-2','utf-8'],reload=True)
        el1=html.cssselect('table table tr')
        
        stkid=''
        for i in range(1,len(el1)):
            
            tds=el1[i].cssselect('td')
            if len(tds)<10:continue # ignore the <th> tags and other merge-cell rows
            
            stkid=tds[0].text_content().strip()
            sale=tds[2].text_content().strip().replace(',','')
            
            try:
                data=[str(y)+'_'+str(m)+'_'+stkid,y,m,stkid,sale]
                #Util.printList(data)
                self.insert(data)
            except sqlite3.IntegrityError as ex: # update if there's data in the record
                print ex
                Table.update(self, self.fields, data, ('sID',),[data[0],])
                
    def update(self):
        for y in range(2002,self.thisYear+1):
            for m in range(1,13):
                if y==self.thisYear and m>=self.thisMonth:break
                #if self.hasRecords(y,m) and self.IgnoreExtracting(y,m): continue  
                if self.hasRecords(y,m): continue 
                
                #if (y==2012 and m>2) or y>2012:break
                for s in ['sii','otc']: self.extractData(s, y, m)
                self.myRecord.insert([str(y),str(m)], True)
                self.myDb.updateDB()
        
class TableSaleUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','M']
        Table.__init__(self, myDB, name,fields)   
        
class TableCash(Table):
    def __init__(self,myDB,name):
        fields=['fID','Y','Q','stkID','opCF','invCF','finanCF']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableCashUpdateRecord(myDB,'cashUpdateRecord')
        self.addUrlSource(UrlConnCash())
    
    # new cash extraction after IFRS
    #
    def extractData(self,stkid,y,q):
        s=self.urlSources[0]
        s.updateUrlData({'co_id':stkid,'year':y-1911,'season':q})
        #html=s.getHtml(['utf-8','ISO-8859-2'],reload=True)
        html=s.getHtml(['utf-8'],reload=True)
        el0=html.cssselect('table[class="hasBorder"] tr')
        if len(el0)==0: return
        
        print "in Tablecash..."
        
        cash=[]
        cashName=['營運產生之現金流入（流出）','投資活動之淨現金流入（流出）','籌資活動之淨現金流入（流出）']
        cashValue=[0,0,0]
        
        for i in range(1,len(el0)):
            tds=el0[i].cssselect('td')
            if len(tds)==0:continue
            
            title=tds[0].text_content().strip()
            
            for i in range(3):
                if title in cashName[i]:     
                    cashValue[i]=int(tds[1].text_content().strip().replace(',',''))
                    print "%s : %s" % (cashName[i],tds[1].text_content().strip().replace(',',''))
                        
        try:
            data=[str(y)+'_'+str(q)+'_'+str(stkid),y,q,stkid]+cashValue
            #Util.printList(data)
            print data
            self.insert(data)
        except sqlite3.IntegrityError as ex: # update if there's data in the record
            print ex
            Table.update(self, self.fields, data, ('sID',),[data[0],])
    
    def update(self):
        for y in range(2013,self.thisYear+1):
            for q in range(1,5):
                
                if y==self.thisYear and q>=self.thisQuarter:break
                #if self.hasRecords(y,m) and self.IgnoreExtracting(y,m): continue  
                
                try:
                    records=self.myDb.getTable('Income').select(('stkID',),('Y=%s'% y,'Q=%s'% q)).fetchall()
                    i=0
                    while (i<len(records)):
                        stkID=records[i][0]
                        if len(str(stkID))>4:
                            i=i+1
                            continue
                        self.extractData(stkID, y, q)
                        self.myRecord.insert([str(y),str(q),str(stkID)], True)
                        i=i+1
                        self.myDb.updateDB()
                except Exception as ex:
                    print ex
    
    '''
    # old cash extraction before IFRS
    #
    def update(self):
        y=2013
        while (y<self.thisYear+1): 
            q=1
            while (q<5):
                if y==self.thisYear and q>=self.thisQuarter:break
                
                records=self.myDb.getTable('Income').select(('stkID',),('Y=%s'% y,'Q=%s'% q)).fetchall()
                i=0
                while (i<len(records)):
                    
                    stkID=records[i][0]
                    # if the income record DB has records before this year or this quarter, ignore the subsequent action
                    db.cursor.execute('select Y from '+ self.myRecord.name+' where Y=? and Q=? and stkID=?',(y,q,stkID))
                    if db.cursor.fetchone()!=None:
                        print '%sQ%s %s already has cash record' % (y,q,stkID)
                        if y<self.thisYear:
                            i+=1
                            continue 
                    second=4
                    s=self.urlSources[0]
                    s.updateUrlData({'co_id':stkID,'year':y-1911,'season':'0'+str(q)})
                                     
                    try:
                        s.flush()
                        content=s.getContent()
                        if content.find("資料庫中查無需求資料")!=-1: 
                            i+=1
                            continue
                    except:    
                        content=s.getContent("big5")
                        second+=1
                        time.sleep(second)
                        continue
                    
                    strCashOper=(u"營[\s　]*.[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+",)
                    strCashInv=(u"投[\s　]*資[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+",)
                    strCashFinan=(u"融[\s　]*資[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+", \
                                  u".[\s　]*財[\s　]*活[\s　]*動[\s　]*[產生之淨現縣金流出入數量]+")
                    strCash=(strCashOper,strCashInv,strCashFinan)
                    
                    cash=[0,0,0]  # 累加現金流量 cash[0]:營運現金 cash[1]:投資現金
                    
                    for k in range(3):
                        n=0
                        while n<len(strCash[k]):
                            print "i=%s,k=%s, n=%s" % (i,k,n)
                        
                            #mp1='[\s　$]*[\(（]?[\s]*[\d,]+[\s　]*[\)）]?'      #  $ ( 592,026 )
                            #mp2='[\(（]?[\s　$]*[\d,]+[\s　]*[\)）]?'           #   ( $ 592,026 )
                            #mp3='[-]?[\s　$]*[\d,]+'                           #  - $ 592,026
                            #mp4='[\s　$]*[-]?[\s　]*[\d,]+'                    #  $ - 37,878
                            
                            mp1='[-\(（\[\s　$]*[\d,]+[\s　]*[\)）\]]*' # money pattern
                        
                            ptn1=strCash[k][n]+u"[（） ﹝﹞\s\(\)產生之淨現縣金流出入數量]+[:\s　|│]*"+ mp1+"[\s　|│]*"+mp1
                            ptn2=strCash[k][n]+u"[（） ﹝﹞\s\(\)產生之淨現縣金流出入數量]+[-]+[-]+[\s　]*"+ mp1
                            
                            pattern=(ptn1,ptn2)
                            m,mMatch=(0,-1)
                            while m<len(pattern):
                                #print pattern[m]
                                
                                regex=re.compile(pattern[m])
                                match=regex.findall(content)
                                if len(match)>0: 
                                    mMatch=m
                                    pos1=content.find(match[0])
                                    dataLineLength=len(match[0])
                                    break
                                else: 
                                    m+=1
                            
                            #print "len(match)=%s" % len(match)
                            if len(match)>0:
                                break
                            else:
                                n+=1
                        if n==len(strCash[k]): continue
                                    
                        strTemp=content[pos1:pos1+dataLineLength]
                        #print strTemp
                       
                        try:
                            regex=re.compile(mp1)
                            cash[k]=regex.findall(strTemp)[0]
                            
                            #print "cash=%s" % cash[k] 
                            
                            if cash[k].find('(')!=-1 or cash[k].find('（')!=-1 or cash[k].find(')')!=-1 or cash[k].find('）')!=-1:
                                cash[k]=cash[k].replace('-','')
                                cash[k]=cash[k].replace(']','').replace('[','')
                                cash[k]=cash[k].replace('）','').replace('（','')
                                cash[k]="-" + cash[k].replace(')','').replace('(','')
                        
                            cash[k]=cash[k].replace('$','')
                            cash[k]=int(cash[k].replace(",",""))
                            #print "cash=%s" % cash[k] 
                        except:
                            if cash[k]=="-": 
                                cash[k]=0
                            else:
                                break
                        
                    try:    
                        dollor=re.compile(u'單位[：:\s　\u4e00-\u9fff]*[元]').findall(content)
                        #print "len of dollor %s" % len(dollor)
                        if len(dollor)!=0:
                            dollor=re.compile(u'[仟千]').findall(content)
                            if len(dollor)==0:
                                #print "%s uses 仟元!" % stkID
                                cash=[int(round(c/1000.)) for c in cash]
                    except:
                        pass  
                    
                    try:    
                        data=[str(y)+'_'+str(q)+'_'+str(stkID),y,q,stkID]+cash
                        #Util.printList(data)  
                        self.insert(data)
                    except sqlite3.IntegrityError as ex:
                        # ignore if there's data in the record
                        print "%s already has record in %sQ%s" % (data[3],y,q)

                    self.myRecord.insert([str(y),str(q),str(stkID)], True)
                    self.myDb.updateDB()
                    i+=1
                q+=1
            y+=1
    '''     

class TableCashUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','Q','stkID']
        Table.__init__(self, myDB, name,fields)  
        
class TablePrice(Table):
    def __init__(self,myDB,name):
        fields=['pID','date','stkID','price']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TablePriceUpdateRecord(myDB,'priceUpdateRecord')
        self.addUrlSource(UrlConnPrice('sii'))
        self.addUrlSource(UrlConnPrice('otc'))
        
    def update(self):      
        y=self.thisYear-1
        while (y<self.thisYear+1): 
            m=1
            while (m<13):
                if y==self.thisYear and m>self.thisMonth:
                    print "(y,m)=(%s,%s)" % (y,m)
                    break
                
                q=(m-1)/3+1
                if q<3: 
                    Y,Q=(y-1,q+2)
                else: 
                    Y,Q=(y,q-2)
                
                
                n=0 
                # n=0 : stkType=0--> sii
                # n=1 : stkType=1--> otc
                while n<len(self.urlSources):   
                    s=self.urlSources[n]
                    records=self.myDb.getTable('Income').select(('stkID',),('stkType='+str(n),'Y='+str(Y),'Q='+str(Q))).fetchall()
                    dbRecords=db.cursor.execute('select stkID from '+ self.myRecord.name+' where Y=? and M=?',(y,m)).fetchall()
                    dbRecords=[r[0] for r in dbRecords]
                    
                    #print "Y=%s,M=%s" % (y,m)
                    print "count=%s" % len(records)
                    i=0
                    while i < len(records):
                        stkID=records[i][0]
                        print "stkID=%s" % stkID
                        
                        if stkID in dbRecords:
                            if y<self.thisYear or (y==self.thisYear and m<self.thisMonth):
                                print "there's already data in the record database : (Y,M,stkID)=(%s,%s,%s)" % (y,m,stkID)
                                i+=1
                                continue 
                       
                        s.updateUrlData((y,m,stkID,n))
                        
                        print '....1'
                        #print "Y=%s,Q=%s" % (Y,Q)
                        print "processing %s" % s.getWholeUrlName()             
                        try:
                            html=s.getHtml('big5_hkscs',n)
                        except urllib2.HTTPError as ex:
                            print ex
                            i+=1
                            continue
                        except sqlite3.OperationalError as ex:
                            print ex
                            continue
                        except Exception as ex:
                            print ex
                            i+=1
                            continue
                        
                        print '....2' 
                        if n==0:   
                            el0=html.cssselect('table tr td span[class="basic2"]')
                            if len(el0)!=0:
                                if el0[0].text_content().find('查無資料')!=-1: 
                                    print "%s 查無資料" % stkID
                                    i+=1
                                    continue
                            el1=html.cssselect('table[class="board_trad"] tr')
                        elif n==1:
                            el1=html.cssselect('table[class="table-board"] tr')
                            if len(el1)==1:
                                print "%s 查無資料" % stkID
                                i+=1
                                continue
                        
                        #print "len of el1 = %s " % len(el1)
                        
                        print '....3'
                        for j in range(1,len(el1)):
                            if n==0 and (j<2 or j==len(el1)-1):
                                continue
                            
                            tds=el1[j].cssselect('td')
                            d=int(tds[0].text_content().strip()[-2:])
                            #Date=Date.replace('/','-')
                            #Date=str(int(Date[:Date.find('-')])+1911)+Date[Date.find('-'):]
                            Date="%s-%s-%s" % (y,m,d)
                            
                            print '....4'
                            if n==0:
                                price=tds[1].text_content().strip()
                            elif n==1:
                                price=tds[6].text_content().strip()
                            try:     
                                #data=[Date+'_'+str(stkID),Date,stkID,price]
                                data=[Date+'_'+str(stkID),y,m,d,stkID,price]
                                self.insert(data)
                            except sqlite3.IntegrityError as ex:
                                print "%s already has record in %s" % (data[2],data[1])
                        
                        
                        self.myRecord.insert([str(y),str(m),str(stkID)], True)
                        self.myDb.updateDB()
                        
                        i+=1
                    n+=1
                m+=1
            y+=1    
            
class TablePriceUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','M','stkID']
        Table.__init__(self, myDB, name,fields)  

class TableMPrice(Table):
    def __init__(self,myDB,name):
        fields=['pID','date','stkID','price']
        Table.__init__(self, myDB, name,fields)
        self.myRecord=TableMPriceUpdateRecord(myDB,'priceUpdateRecord')
        self.addUrlSource(UrlConnMPrice(stkType.sii))
        self.addUrlSource(UrlConnMPrice(stkType.otc))
    
    
    def extractData(self,s,y):
        records=self.myDb.getTable('Income').select(('stkID',),('stkType=%s' % (s.stkType,),'Y=%s and Q=%s' % (y-1,4))).fetchall()
        dbRecords=db.cursor.execute('select stkID from '+ self.myRecord.name+' where Y=?',(y,)).fetchall()
        dbRecords=[r[0] for r in dbRecords]
        
        s.enableProxyConnection(True)
        for i in range(len(records)):
            stkID=records[i][0]
            if stkID in dbRecords:
                if y<self.thisYear:
                    print "there's already data in the record database : (Y,stkID)=(%s,%s)" % (y,stkID)
                    continue 
           
            s.updateUrlData((y,stkID))   
            try:
                html=s.getHtml('big5_hkscs',reload=True)
            except Exception as ex:
                print ex
            
            if s.stkType==stkType.sii:   
                el0=html.cssselect('table tr td span[class="basic2"]')
                if len(el0)!=0:
                    if el0[0].text_content().find('查無資料')!=-1: 
                        print "%s 查無資料" % stkID
                        continue
                el1=html.cssselect('table[class="board_trad"] tr')
            elif s.stkType==stkType.otc:
                el1=html.cssselect('table[class="table-board"] tr')
                if len(el1)==1:
                    print "%s 查無資料" % stkID
                    continue
           
            for j in range(1,len(el1)):
                if s.stkType==0 and j<2: continue
                
                tds=el1[j].cssselect('td')
                m=int(tds[1].text_content().strip())
                price=tds[4].text_content().strip()
                
                pID="%s_%s_%s" % (y,m,stkID)
                try:     
                    data=[pID,y,m,stkID,price]
                    print "data=%s" % data
                    self.insert(data)
                except sqlite3.IntegrityError as ex:
                    print "%s already has record" % (data[0],)
            
                self.myRecord.insert([str(y),str(m),str(stkID)], True)

            self.myDb.updateDB()
    
    def update(self):
        y=self.thisYear-1
        while (y<self.thisYear+1):             
            #n=0 
            # n=0 : stkType=0--> sii
            # n=1 : stkType=1--> otc
                      
            print "update MPrice : y=%s" % y
            self.extractData(self.urlSources[stkType.sii], y)
            self.extractData(self.urlSources[stkType.otc], y)
            y+=1    
            
class TableMPriceUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['Y','M','stkID']
        Table.__init__(self, myDB, name,fields)           

class TablePER(Table):
    def __init__(self,myDB,name):
        fields=['stkID','avePER','stdevPER']
        Table.__init__(self, myDB, name,fields)
        
    def update(self):
        thisYear=datetime.now().year
        
        db.cursor.execute('delete from per')
        self.myDb.updateDB()
        
        db.loadExtension()  
        '''
        dbRecordsAveStdPer=db.cursor.execute('select p.stkID stkID,sum(p.price/i.epsTTM_forPER)/count(*) as avePER \
                                             ,stdev(p.price/i.epsTTM_forPER) as stdevPER \
                                             from price p,income i where p.Y=i.Y and (P.M-1)/3+1=i.Q and \
                                             p.stkID=i.stkID and p.Y>=%s group by p.stkID' % (thisYear-10,)).fetchall()
        '''
        dbRecordsAveStdPer=db.cursor.execute('select p.stkID stkID,sum(p.price/i.epsTTM_forPER)/count(*) as avePER \
                                             ,stdev(p.price/i.epsTTM_forPER) as stdevPER \
                                             from price p,income i where p.Y=i.Y and (P.M-1)/3+1=i.Q and \
                                             p.stkID=i.stkID and p.Y>=%s group by p.stkID' % (thisYear-10,)).fetchall()
        print "len of dbRecordsAveStdPer=%s" % len(dbRecordsAveStdPer)
        
        for r in dbRecordsAveStdPer:
            print "stkID=%s , avePER=%s , stdPER=%s" % (r[0],r[1],r[2])
            data=(r[0],r[1],r[2])
            self.insert(data)
        self.myDb.updateDB()
        
class TableAvePer(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name,['stkID','avePer'])
        
    def update(self):
        thisYear=datetime.now().year    
        try:
            db.cursor.execute('CREATE TABLE avePer(stkID integer not null primary key \
                            references stock(stkID) on delete restrict on update cascade, avePer numeric);')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from avePer')
        
        self.myDb.updateDB()
        dbRecordsAvePer=db.cursor.execute('select p.stkID stkID,sum(p.price/i.epsTTM_forPER)/count(*) as PER from price p,income i,per  \
                                        where p.Y=i.Y and (P.M-1)/3+1=i.Q and p.stkID=i.stkID and p.stkID=per.stkID and p.Y>=%s \
                                        and p.price/i.epsTTM_forPER<per.avePER+per.stdevPER and p.price/i.epsTTM>0 group by p.stkID;' % (thisYear-10,)).fetchall()
        for r in dbRecordsAvePer:
            self.insert((r[0],r[1]))
        self.myDb.updateDB()
        
class TableTenYearsAveDiv(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name,['stkID','aveDiv'])
        
    def update(self):
        thisYear=datetime.now().year
        db.cursor.execute('delete from AveDiv')
        dbRecordsAveDiv=db.cursor.execute('select stkID, sum(cashDiv)/count(*) from dividend D where \
            case when exists(select * from dividend where stkID=D.stkID and Y+1911=%s) then  \
                Y+1911>%s \
            else Y+1911>=%s  \
            end \
            group by stkID;' % (thisYear,thisYear-10,thisYear-10)).fetchall()
        for r in dbRecordsAveDiv:
            self.insert((r[0],r[1]))
        self.myDb.updateDB()
          
class TableEpsRateForRegress(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        try: 
            db.cursor.execute('CREATE TABLE %s(id text not null primary key, Year integer,Q integer,stkID integer,X integer,EPS numeric, \
                            foreign key(stkID) references stock(stkID) on delete restrict on update cascade);'% (self.name,))
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from %s' % (self.name))
            db.cursor.execute('drop view if exists EpsRate_pre1')
            db.cursor.execute('drop view if exists EpsRate')
            
        #dbRecords=db.cursor.execute('select ((Y-%s+10)*4+Q-(select Q from income where stkID=i.stkID order by Y desc,Q desc limit 1)+4) X, \
        #Y,Q,stkID,eps from income i where iId in (select iId from income where stkID=i.stkID order by Y desc,Q desc limit 40) and eps!=""' % (thisYear,)).fetchall()
        
        dbRecords=db.cursor.execute('select ((Y-%s+9)*4+Q-(select Q from income where stkID=i.stkID order by Y desc,Q desc limit 1)+4) X, \
        Y,Q,stkID,eps from income i where iId in (select iId from income where stkID=i.stkID order by Y desc,Q desc limit 40) and eps!=""' % (thisYear,)).fetchall()
        
        for r in dbRecords:
            self.insert(("%s_%s_%s" % (r[1],r[2],r[3]),r[1],r[2],r[3],r[0],r[4]) )      
        db.loadExtension()    
        self.myDb.createView('EpsRate_pre1','select stkID,x,log(eps) Y from %s where eps>0' % (self.name))
        #self.myDb.createView('BvpsRate_pre1','select stkID,X,case when eps=0 then null else log(eps) end Y from %s' % (self.name,))
        #self.myDb.createView('EpsRate','select stkID, \
        #power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1 as rate \
        #from EpsRate_pre1 group by stkID;')
        self.myDb.createView('EpsRate','select stkID, \
        (power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1)*count(*)/40 as rate \
        from EpsRate_pre1 group by stkID;')
        self.myDb.updateDB()

class TableBvpsRateForRegress(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        try: 
            db.cursor.execute('CREATE TABLE %s(id text not null primary key, Year integer,Q integer,stkID integer,X integer,BVPS numeric, \
                            foreign key(stkID) references stock(stkID) on delete restrict on update cascade);'% (self.name,))
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from %s' % (self.name))
            db.cursor.execute('drop view if exists BvpsRate_pre1')
            db.cursor.execute('drop view if exists BvpsRate')
        #dbRecords=db.cursor.execute('select (Y-%s+10)*4+(Q-(select Q from income where stkID=b.stkID order by Y desc,Q desc limit 1)+4) X, \
        #Y,Q,stkID,bvps from balance b where bID in (select bID from balance where stkID=b.stkID order by Y desc,Q desc limit 40) and bvps!=""' % (thisYear,)).fetchall()
        
        dbRecords=db.cursor.execute('select (Y-%s+9)*4+(Q-(select Q from income where stkID=b.stkID order by Y desc,Q desc limit 1)+4) X, \
        Y,Q,stkID,bvps from balance b where bID in (select bID from balance where stkID=b.stkID order by Y desc,Q desc limit 40) and bvps!=""' % (thisYear,)).fetchall()
        
        for r in dbRecords: self.insert(("%s_%s_%s" % (r[1],r[2],r[3]),r[1],r[2],r[3],r[0],r[4]) )      
        db.loadExtension()    
        self.myDb.createView('BvpsRate_pre1','select stkID,X,log(bvps) Y from %s where bvps>0' % (self.name,))
        #self.myDb.createView('BvpsRate_pre1','select stkID,X,case when bvps=0 then null else log(bvps) end Y from %s' % (self.name,))
        #self.myDb.createView('BvpsRate','select stkID, \
        #power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1 as rate \
        #from BvpsRate_pre1 group by stkID;')
        self.myDb.createView('BvpsRate','select stkID, \
        (power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1)*count(*)/40 as rate \
        from BvpsRate_pre1 group by stkID;')
        self.myDb.updateDB()
        
class TableSaleRateForRegress(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        thisMonth=datetime.now().month
        try: 
            db.cursor.execute('CREATE TABLE %s(id text not null primary key, Year integer,M integer,stkID integer,X integer,SALE numeric, \
                            foreign key(stkID) references stock(stkID) on delete restrict on update cascade);'% (self.name,))
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from %s' % (self.name))
            db.cursor.execute('drop view if exists SaleRate_pre1')
            db.cursor.execute('drop view if exists SaleRate')
        dbRecords=db.cursor.execute('select \
        ((case when M<%s then Y-1 else Y end)-%s+10)*12+(M-%s+12)%%12 X,Y,M,stkID,sales \
        from Sale s where ((Y=%s and M>%s) or Y>%s) and sales!=""' % \
         (thisMonth-1,thisYear,thisMonth-1,thisYear-10,thisMonth-1,thisYear-10)).fetchall()
        for r in dbRecords: self.insert(("%s_%s_%s" % (r[1],r[2],r[3]),r[1],r[2],r[3],r[0],r[4]) )   
        self.myDb.updateDB()
           
        db.loadExtension()    
        #self.myDb.createView('SaleRate_pre1','select stkID,X,case when sale=0 then null else log(sale) end Y from %s' % (self.name,))
        #self.myDb.createView('SaleRate','select stkID, \
        #power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),12)-1 as rate \
        #from SaleRate_pre1 group by stkID;')
        self.myDb.createView('SaleRate_pre1','select stkID,X,log(sale) Y from %s where sale>0' % (self.name,))
        self.myDb.createView('SaleRate','select stkID, \
        (power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),12)-1)*count(*)/120 as rate \
        from SaleRate_pre1 group by stkID;')
        self.myDb.updateDB()        
 
class TablePriceNew(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        self.myRecord=TablePriceNewUpdateRecord(myDB,'priceNewUpdateRecord')
        self.addUrlSource(UrlConnPriceNew('sii'))
        #self.addUrlSource(UrlConnPriceNew('otc'))
        
    def update(self):
        date=datetime.now()
        print 'select date from '+ self.myRecord.name
        dbRecords=db.cursor.execute('select date from '+ self.myRecord.name).fetchall()
        print 'after'
        if len(dbRecords)>0:
            dRecord=dbRecords[0][0].split('_')
            print datetime(int(dRecord[0]),int(dRecord[1]),int(dRecord[2]))
            print datetime(date.year,date.month,date.day)
            if datetime(int(dRecord[0]),int(dRecord[1]),int(dRecord[2]))==datetime(date.year,date.month,date.day):
                return
        
        try: 
            db.cursor.execute('CREATE TABLE %s(stkID integer not null primary key, price numeric, \
                            foreign key(stkID) references stock(stkID) on delete restrict on update cascade);'% (self.name,))
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from %s' % (self.name))
            db.cursor.execute('delete from priceNewUpdateRecord')
        
        
        hasError=False
        for n in range(2):
            s=self.urlSources[n]
            
            while True:
                try:
                    s.updateUrlData({'date':date, 'stkType':n})
                    print "processing %s" % s.getWholeUrlName()
                    html=s.getHtml('big5_hkscs')
                    if n==0:
                        el1=html.cssselect('table[class="board_trad"] tr')
                    elif n==1:
                        el1=html.cssselect('table[id="contentTable"] tr')
                        
                    if len(el1)<=1: 
                        date=date+timedelta(days=-1)
                    else:
                        break
                except TypeError as err: # 404 Http not-found error
                    print 'HTTP Error 404 : Not Found !!'
                    date=date+timedelta(days=-1)
            
            endOfRow=[len(el1)-1,len(el1)-4]   
            for i in range(1,endOfRow[n]):
                tds=el1[i].cssselect('td')
                if len(tds)==0:continue # ignore the <th> tags
                
                stkid=tds[0].text_content().strip()
                if len(stkid)>4:continue
                try:     
                    if n==0:
                        data=[td.text_content().strip() for td in tds if tds.index(td) in (0,5)]
                    elif n==1:
                        data=[td.text_content().strip() for td in tds if tds.index(td) in (0,2)]
                        
                    if data[1]=='---': data[1]=None
                    Util.printList(data)
                    self.insert(data)
                except sqlite3.IntegrityError as ex:
                    hasError=True
                    print ex
        if not hasError:
            print "no error"
            self.myRecord.insert(["%s_%s_%s"%(date.year,date.month,date.day),])
            self.myDb.updateDB()
                    
class TablePriceNewUpdateRecord(Table):
    def __init__(self,myDB,name):
        fields=['date']
        Table.__init__(self, myDB, name,fields)      
       
class TableCashRateForRegress(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        thisMonth=datetime.now().month
        try: 
            db.cursor.execute('CREATE TABLE FreeCash(id text not null primary key, Y integer,Q integer,stkID integer, \
            opCFQ numeric,invCFQ numeric,freeCFQ numeric, foreign key(stkID) references stock(stkID) on delete restrict on update cascade)')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from FreeCash')
        dbRecords=db.cursor.execute('select c1.Y Y,c1.Q Q,c1.stkID stkID,c1.opCF opCF,c1.invCF invCF,c1.finanCF finanCF, \
        case when c1.Q=1 then c1.opCF else c1.opCF-c2.opCF end opCFQ, \
        case when c1.Q=1 then c1.invCF else c1.invCF-c2.invCF end invCFQ, \
        case when c1.Q=1 then c1.opCF+c1.invCF else (c1.opCF-c2.opCF)+(c1.invCF-c2.invCF) end freeCFQ \
        from CashFlow c1,CashFlow c2 where c1.Y=c2.Y and case when c1.Q=1 then c1.Q=c2.Q else c1.Q=c2.Q+1 end \
        and c1.stkID=c2.stkID').fetchall()
          
        for r in dbRecords:
            db.cursor.execute("insert into FreeCash values(?,?,?,?,?,?,?)",("%s_%s_%s"% (r[0],r[1],r[2]),r[0],r[1],r[2],r[6],r[7],r[8]))        
        self.myDb.updateDB()
        
        #---
        try: 
            db.cursor.execute('CREATE TABLE %s(id text not null primary key, Year integer,Q integer,stkID integer,X integer,freeCFQ numeric, \
                            foreign key(stkID) references stock(stkID) on delete restrict on update cascade);'% (self.name,))
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from %s' % (self.name))
            db.cursor.execute('drop view if exists CFRate_pre1')
            db.cursor.execute('drop view if exists CFRate')
            
        #dbRecords=db.cursor.execute('select (Y-%s+10)*4+(Q-(select Q from FreeCash where stkID=f.stkID order by Y desc,Q desc limit 1)+4) X, \
        #    Y,Q,stkID,freeCFQ from FreeCash f where id in (select id from FreeCash where stkID=f.stkID order by Y desc,Q desc limit 40) and freeCFQ!=""' % (thisYear,)).fetchall()
        dbRecords=db.cursor.execute('select (Y-%s+9)*4+(Q-(select Q from FreeCash where stkID=f.stkID order by Y desc,Q desc limit 1)+4) X, \
            Y,Q,stkID,freeCFQ from FreeCash f where id in (select id from FreeCash where stkID=f.stkID order by Y desc,Q desc limit 40) and freeCFQ!=""' % (thisYear,)).fetchall()
        
        for r in dbRecords: self.insert(("%s_%s_%s" % (r[1],r[2],r[3]),r[1],r[2],r[3],r[0],r[4]) )      
        db.loadExtension()    
        #self.myDb.createView('CFRate_pre1','select stkID,X,case when freeCFQ=0 then null else log(freeCFQ) end Y from %s where freeCFQ>0' % (self.name,))
        #self.myDb.createView('CFRate','select stkID, \
        #(power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1)*count(*)/40 as rate \
        #from CFRate_pre1 group by stkID;')
        self.myDb.createView('CFRate_pre1','select stkID,X,log(freeCFQ) Y from %s where freeCFQ>0' % (self.name,))
        self.myDb.createView('CFRate','select stkID, \
        (power(exp((sum(X*Y)-sum(X)*sum(Y)/count(*))/(sum(power(X,2))-power(sum(X),2)/count(*))),4)-1)*count(*)/40 as rate \
        from CFRate_pre1 group by stkID;')
        self.myDb.updateDB()
        
class TableROE(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        db.cursor.execute(
        'create view if not exists netIncomeQ as \
         select i1.iID, \
                i1.Y, \
                i1.Q, \
                i1.stkID, \
                case when i1.Q=1 then i1.NetIncome else i1.NetIncome-i2.NetIncome end iQ \
         from income i1,income i2 \
         where ((i1.Y=i2.Y and i1.Q=i2.Q+1 and i1.stkID=i2.stkID) or (i1.Y=i2.Y and i1.Q=1 and i2.Q=1 and i1.stkID=i2.stkID))')
        
        db.cursor.execute(
        'create view if not exists netIncomeYearByQ as \
         select i.iID, \
                i.Y, \
                i.Q, \
                i.stkID, \
                i.iQ, \
                (select sum(iQ) from netincomeQ where stkID=i.stkID and ((Y=i.Y and Q<=i.Q) or (Y=i.Y-1 and Q>i.Q)) order by Y desc,Q desc limit 4) sumIQ \
         from netincomeQ i')
        
        db.cursor.execute(
        'create view if not exists aveEquityYearByQ as \
         select b1.Y, \
                b1.Q, \
                b1.stkID, \
                b1.equity Eq, \
                (b1.equity+b2.equity)/2 aveEq \
         from balance b1,balance b2 \
         where b1.Y=b2.Y+1 and b1.Q=b2.Q and b1.stkID=b2.stkID')
        
        try: 
            db.cursor.execute(
            'CREATE TABLE ROE( \
                iID text not null primary key, \
                Y integer, \
                Q integer, \
                stkID text , \
                iQ numeric, \
                sumIQ numeric, \
                Eq numeric, \
                aveEq numeric, \
                ROE numeric, \
                foreign key(stkID) references stock(stkID) on delete restrict on update cascade)')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from ROE')
        
        try: 
            dbRecords=db.cursor.execute(
            'select i.iID, \
                    i.Y, \
                    i.Q, \
                    i.stkID, \
                    i.iQ iQ, \
                    i.sumIQ sumIQ, \
                    e.Eq Eq, \
                    e.aveEq aveEq, \
                    cast(i.sumIQ as float)/cast(e.aveEq as float) ROE \
            from netIncomeYearByQ i,aveEquityYearByQ e \
            where i.Y=e.Y and i.Q=e.Q and i.stkID=e.stkID').fetchall()
        except Exception as ex: 
            print ex
        
        for r in dbRecords:
            db.cursor.execute('insert into ROE values(?,?,?,?,?,?,?,?,?)',r)       
        
        for i in range(1,11):
            db.cursor.execute('drop view if exists ROE%s' % i)
            self.myDb.createView('ROE%s'% i,'select stkID,sum(ROE)/count(*) ROE%s from %s where Y>=%s group by stkID' % (i,self.name,thisYear-i))
        
        self.myDb.updateDB()    

class TablePayRatio(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        
        try: 
            db.cursor.execute(
            'CREATE TABLE PayRatio( \
                pID text not null primary key, \
                Y integer, \
                stkID text , \
                EPS numeric, \
                cashDiv numeric, \
                stockDiv numeric, \
                PayRatio numeric, \
                foreign key(stkID) references stock(stkID) on delete restrict on update cascade)')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from PayRatio')
        
        try: 
            dbRecords=db.cursor.execute(
            'select i.iID, \
                    i.Y, \
                    i.stkID, \
                    i.cumeps EPS, \
                    d.cashDiv cashDiv, \
                    d.stokDiv stockDiv, \
                    cast(d.cashDiv as float)/cast(i.cumeps as float) PayRatio \
             from income i,dividend d  \
             where i.Y=d.Y+1910 and i.stkID=d.stkID and i.Q=4 ').fetchall()
        except Exception as ex: 
            print ex
        
        for r in dbRecords:
            db.cursor.execute('insert into PayRatio values(?,?,?,?,?,?,?)',r)
        
        for i in range(1,11):
            db.cursor.execute('drop view if exists PayRatio%s' % i)
            self.myDb.createView('PayRatio%s'% i,'select stkID,sum(PayRatio)/count(*) PayRatio%s from %s where Y>=%s group by stkID' % (i,self.name,thisYear-i))     
          
        self.myDb.updateDB()             
    
class TableFinalValue(Table):
    def __init__(self,myDB,name):
        Table.__init__(self, myDB, name)
        
    def update(self):
        thisYear=datetime.now().year
        thisMonth=datetime.now().month
        try: 
            db.cursor.execute(
            'CREATE TABLE FinalPre1( \
                stkID text not null primary key, \
                name text, \
                aveDiv numeric, \
                PriceDDM numeric, \
                avePER numeric, \
                epsTTM numeric, \
                epsRate numeric, \
                bvpsRate numeric, \
                saleRate numeric, \
                cashRate numeric, \
                rate numeric, \
                PriceGrowth numeric, \
                Price numeric, \
                foreign key(stkID) references stock(stkID) on delete restrict on update cascade)')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from FinalPre1')
        
        db.loadExtension()    
        try: 
            dbRecords=db.cursor.execute(
            'select \
                s.stkID, \
                trim(sh.shortName) Company, \
                d.aveDiv aveDiv, \
                d.aveDiv/0.15 PriceDDM, \
                per.avePer avePER, \
                (select epsTTM from income where stkID=s.stkID order by Y desc,Q desc limit 1) epsTTM, \
                eR.rate epsRate, \
                bR.rate bvpsRate, \
                sR.rate saleRate, \
                cR.rate cashRate, \
                (eR.rate+bR.rate+sR.rate+cR.rate)/4 rate, \
                (select epsTTM from income where stkID=s.stkID order by Y desc,Q desc limit 1)* \
                    power(1+(eR.rate+bR.rate+sR.rate+cR.rate)/4,10)* \
                    avePER/power(1+0.15,10)/2 PriceGrowth, \
                pN.price Price \
            from \
                stock s, \
                stockShortName sh, \
                aveDiv D, \
                avePER per, \
                epsRate eR, \
                bvpsRate bR, \
                saleRate sR, \
                CFRate cR, \
                priceNew pN \
            where \
                s.stkID=sh.stkID and s.stkID=d.stkID and s.stkID=per.stkID and s.stkID=eR.stkID and \
                s.stkID=bR.stkID and s.stkID=sR.stkID and s.stkID=cR.stkID and s.stkID=pN.stkID').fetchall() 
        except Exception as ex: 
            print ex
        
        
        for r in dbRecords:
            db.cursor.execute('insert into FinalPre1 values(?,?,?,?,?,?,?,?,?,?,?,?,?)',r)       
        self.myDb.updateDB()
           
        try: 
            db.cursor.execute(
            'CREATE TABLE Final( \
                stkID text not null primary key, \
                name text, \
                aveDiv numeric, \
                PriceDDM numeric, \
                avePER numeric, \
                epsTTM numeric, \
                epsRate numeric, \
                bvpsRate numeric, \
                saleRate numeric, \
                cashRate numeric, \
                rate numeric, \
                PriceGrowth numeric, \
                Price numeric, \
                pCal numeric, \
                PriceRatio numeric, \
                divPriceRatio numeric, \
                pay1 numeric, \
                pay3 numeric, \
                pay6 numeric, \
                pay10 numeric, \
                roe1 numeric, \
                roe3 numeric, \
                roe6 numeric, \
                roe10 numeric, \
                foreign key(stkID) references stock(stkID) on delete restrict on update cascade)')
        except Exception as ex: 
            print ex
            db.cursor.execute('delete from Final')
        self.myDb.updateDB()
        
        try:
            print "extract value from FinalPre1"
            dbRecord2=db.cursor.execute(
            'select f.*, \
                f.PriceDDM+f.PriceGrowth pCal, \
                f.Price/(f.PriceDDM+f.PriceGrowth) PriceRatio, \
                f.PriceDDM/(f.PriceDDM+f.PriceGrowth) divPriceRatio, \
                P3.PayRatio3 PayRatio3, \
                P6.PayRatio6 PayRatio6, \
                P10.PayRatio10 PayRatio10, \
                r1.ROE1 roe1, \
                r3.ROE3 roe3, \
                r6.ROE6 roe6, \
                r10.ROE10 roe10 \
            from FinalPre1 f,PayRatio3 p3, PayRatio6 p6, PayRatio10 p10 ,ROE1 r1,ROE3 r3,ROE6 r6,ROE10 r10 \
            where f.stkID=p3.stkID and f.stkID=p6.stkID and f.stkID=p10.stkID and \
                  f.stkID=r1.stkID and f.stkID=r3.stkID and f.stkID=r6.stkID and f.stkID=r10.stkID').fetchall()
        except Exception as ex:
            print "error when extract value from FinalPre1"
            print ex
        
        print "len of dbRecord2=%s" % len(dbRecord2)
        for r in dbRecord2:
            db.cursor.execute('insert into Final values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',r)
        self.myDb.updateDB()    