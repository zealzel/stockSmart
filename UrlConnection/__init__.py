# -*- coding: utf8 -*-
import urllib2
import urllib
import random
import lxml.html
from lxml.html import fromstring
import socket
from datetime import datetime
from Util import stkType


TIMEOUT=30

'''
class stType:
    sii,otc='sii','otc'
    SII,OTC=(0,1)
'''

#stkType=Enumerator("sii","otc")
    
class UrlConn(object):
    proxies=['http://proxy.hinet.net:80/',]
    #proxies=['http://58.248.217.209:80',]
    def __init__(self,URL, urldata=None,deCode="utf-8"):
        if urldata==None:
            self.req=urllib2.Request(URL)
            print "self.req=%s" % self.req
        else:
            self.req=urllib2.Request(URL,urllib.urlencode(urldata))
        self.deCode=deCode
        self.responseRead=None
        self.content,self.html = None, None
        self.url=URL
        self.urlData=urldata
        self.openers=[]
        for proxy in UrlConn.proxies:
            opener=urllib2.build_opener(urllib2.ProxyHandler({'http':proxy}))
            self.openers.append(opener)
        socket.setdefaulttimeout(TIMEOUT)
        self.enableProxyConnection()
    
    def getWholeUrlName(self):
        if self.req.get_data()!=None and self.req.get_data()!="":
            return self.url+'?'+self.req.get_data()
        else:
            return self.url
    
    def enableProxyConnection(self,enable=False):
        self.enableproxy=enable
    
    def updateUrlData(self,addedData):
        self.urlData.update(addedData)
        self.req.add_data(urllib.urlencode(self.urlData))
    
    def getResponseRead(self):
        opener=random.choice(self.openers)
        if self.enableproxy:
            self.responseRead=opener.open(self.getWholeUrlName(),timeout=30).read()
            print "opener opens url"
        else:
            self.responseRead=urllib2.urlopen(self.getWholeUrlName(),timeout=30).read()
        return self.responseRead
    
    def getContent(self,deCode="utf-8",reload=False):
        print "processing %s using codec %s" % (self.getWholeUrlName(),deCode) 
        try:
            if self.responseRead==None or reload==True:
                self.getResponseRead()
                self.content=self.responseRead.decode(deCode) 
                print "after urlConn getContent"  
                return self.content
        except urllib2.HTTPError as e:
            print "in getContent urllib2.HTTPError Exception"
            return None
        except urllib2.URLError as e:
            print "in getContent urllib2.URLError Exception"
            return None
        except Exception as e:
            print "in getContent Exception"
            print e
            return None
        
    def getHtml(self,deCodes="utf-8",reload=False): #deCodes can be list of codecs
        if type(deCodes)==list:
            for codec in deCodes:
                try:
                    self.html=fromstring(self.getContent(codec,reload))
                    break
                except Exception as ex:
                    if type(ex)==socket.error:
                        raise socket.error
                    else:
                        print "error : %s" % ex
                        continue
        else:
            self.html=fromstring(self.getContent(deCodes,reload))
        return self.html
    
    def flush(self):
        self.responseRead=None
        
    def getNode(self):
        pass

class UrlConnBasic(UrlConn):
    
    def __init__(self,type=stkType[stkType.sii],code=''):
        url='http://mops.twse.com.tw/mops/web/ajax_t51sb01'
        urlData={'step':'1','firstin':'1'}
        UrlConn.__init__(self,url,urlData)
        self.updateUrlData({'TYPEK':type,'code':code})

class UrlConnShort(UrlConn):
    def __init__(self,type=stkType[stkType.sii],code=''):
        url='http://mops.twse.com.tw/mops/web/ajax_quickpgm'
        urlData={'step':'4','firstin':'true','checkbtn':'1'}
        UrlConn.__init__(self,url,urlData)
        self.updateUrlData({'TYPEK2':type,'code1':code})

class UrlConnDividend(UrlConn):
    def __init__(self,type=stkType[stkType.sii]):
        #url='http://mops.twse.com.tw/mops/web/ajax_t05st09_new'
        url='http://mops.twse.com.tw/server-java/t05st09sub'
        #url='http://mopsov.tse.com.tw/server-java/t05st09'
        urlData={'step':'1','firstin':'1'}
        UrlConn.__init__(self,url,urlData)
        self.updateUrlData({'TYPEK':type})

class UrlConnIncome(UrlConn):
    def __init__(self,type=stkType[stkType.sii]):
        #url='http://mops.twse.com.tw/mops/web/ajax_t51sb08' 個別損益表, before IFRS
        #url='http://mops.twse.com.tw/mops/web/ajax_t51sb13' 合併損益表, before IFRS
        url='http://mops.twse.com.tw/mops/web/ajax_t163sb04' #合併損益表, after IFRS
        urlData={'step':'1','firstin':'1'}
        UrlConn.__init__(self,url,urlData)
        self.updateUrlData({'TYPEK':type})

class UrlConnBalance(UrlConn):
    def __init__(self,type=stkType[stkType.sii]):
        #url='http://mops.twse.com.tw/mops/web/ajax_t51sb07' 個別資產負債表, before IFRS
        #url='http://mops.twse.com.tw/mops/web/ajax_t51sb12'  #合併資產負債表, before IFRS
        url='http://mops.twse.com.tw/mops/web/ajax_t163sb05'  #合併資產負債表, after IFRS
        urlData={'step':'1','firstin':'1'}
        UrlConn.__init__(self,url,urlData)
        self.updateUrlData({'TYPEK':type})
        
class UrlConnSale(UrlConn):
    urlbase='http://mops.twse.com.tw/t21/%s/t21sc03_%s_%s.html'
    def __init__(self):
        url=UrlConnSale.urlbase % (stkType[stkType.sii],91,1) 
        UrlConn.__init__(self,url)
        
    def updateUrlData(self,type,Y,M):
        self.url=UrlConnSale.urlbase % (type,Y,M)
        self.req=urllib2.Request(self.url)
    
class UrlConnCash(UrlConn):
    def __init__(self):
        #url='http://mops.twse.com.tw/mops/web/ajax_t05st36?co_id=2317&year=99&season=01'
        #url='http://mops.twse.com.tw/mops/web/ajax_t05st36' 個別現金流量表, before IFRS
        #url='http://mops.twse.com.tw/mops/web/ajax_t05st39' #合併現金流量表, before IFRS
        url='http://mops.twse.com.tw/mops/web/ajax_t164sb05' #合併現金流量表, after IFRS
        #urlData={'firstin':'1'}
        urlData={'firstin':'1','step':'1'}
        UrlConn.__init__(self,url,urlData)
        self.enableProxyConnection(True)      

class UrlConnPrice(UrlConn):
    def __init__(self,stkType):
        #self.urlBase1='www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/STOCK_DAY_AVG.php?STK_NO=5608&myear=2003&mmon=09'
        self.urlBase1='http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY_AVG/genpage/Report'
        #self.urlBase2='http://www.gretai.org.tw/ch/stock/aftertrading/daily_trading_info/result_st43.php?yy=2002&mm=1&input_stock_code=5508'
        self.urlBase2='http://www.gretai.org.tw/ch/stock/aftertrading/daily_trading_info/result_st43.php'
        urlData={}
        if stkType=='sii':
            UrlConn.__init__(self,self.urlBase1,urlData)
        elif stkType=='otc':
            UrlConn.__init__(self,self.urlBase2,urlData) 
            
    def updateUrlData(self,addedData):
        # addedData : 4 parameters 
        # y : 2001 
        # m : 4
        # stkID: 5508
        # stkType: 0-->'sii' or 1-->'otc'
        y=addedData[0]
        m=addedData[1]
        stkID=addedData[2]
        stkType=addedData[3]
        
        if stkType==0: # sii
            if m<10:
                M='0'+str(m)
            else:
                M=str(m)
            self.url=self.urlBase1+"%s%s/%s%s_F3_1_8_%s.php?STK_NO=%s&myear=%s&mmon=%s" % (str(y),M,str(y),M, \
                                                                                    str(stkID),str(stkID),str(y),M)
        elif stkType==1: #otc
            data={'yy':y,'mm':m,'input_stock_code':stkID}
            #self.url=self.urlBase2+"?yy=%s&mm=%s&input_stock_code=%s" % (str(y),str(m),str(stkID))
            self.urlData.update(data)
            self.req.add_data(urllib.urlencode(self.urlData))
            
            
        #self.urlData.update(addedData)
        #self.req.add_data(urllib.urlencode(self.urlData))
    
    def getContent(self,deCode="utf-8",stkType=0):
        try:
            if stkType==0:
                response=urllib2.urlopen(self.getWholeUrlName(),timeout=30)
            elif stkType==1:
                response=urllib2.urlopen(self.req,timeout=30)
            return response.read().decode(deCode)
        except Exception as ex:
            print ex
            return None
        
    def getHtml(self,deCode="utf-8",stkType=0):
        return fromstring(self.getContent(deCode,stkType))
    
class UrlConnMPrice(UrlConn):
    def __init__(self,sType):
        #self.urlBase1='http://www.twse.com.tw/ch/trading/exchange/FMSRFK/genpage/Report200606/2006_F3_1_10_2317.php?STK_NO=2317&myear=2006'
        self.urlBase0='http://www.twse.com.tw/ch/trading/exchange/FMSRFK/FMSRFK.php'
        self.urlBase1='http://www.twse.com.tw/ch/trading/exchange/FMSRFK/genpage/Report'
        #self.urlBase2='http://www.otc.org.tw/ch/stock/statistics/monthly/result_st44.php?input_stock_code=5508&yy=2007'
        self.urlBase2='http://www.otc.org.tw/ch/stock/statistics/monthly/result_st44.php'
        urlData={}
    
        if sType==stkType.sii:
            UrlConn.__init__(self,self.urlBase1,urlData)
        elif sType==stkType.otc:
            UrlConn.__init__(self,self.urlBase2,urlData) 
        self.stkType=sType
            
    def updateUrlData(self,addedData):
        # addedData : 3 parameters 
        # y : 2001 
        # stkID: 5508
        # stkType: 0-->'sii' or 1-->'otc'
        
        self.y=y=addedData[0]
        self.stkID=stkID=addedData[1]
        
        month=datetime.now().month
        m=str(month).zfill(2)
        
        if self.stkType==stkType.sii:
            self.url=self.urlBase1+"%s%s/%s_F3_1_10_%s.php?STK_NO=%s&myear=%s" % (y,m,y,stkID,stkID,y)
        elif self.stkType==stkType.otc:
            data={'yy':y,'input_stock_code':stkID}
            #self.url=self.urlBase2+"?yy=%s&input_stock_code=%s" % (str(y),str(m),str(stkID))
            self.urlData.update(data)
            self.req.add_data(urllib.urlencode(self.urlData))
    
    
    def getContent(self,deCode="utf-8",reload=False):
        try:
            #if stkType==0:
            if self.stkType==stkType.sii:
                # after loading this, the result of redireted url then will be correct!
                #res=urllib2.urlopen('%s?STK_NO=%s&myear=%s' % (self.urlBase0,self.stkID,self.y)) 
                
                return UrlConn.getContent(self, deCode, reload)
            #elif stkType==1:
            elif self.stkType==stkType.otc:
                response=urllib2.urlopen(self.req,timeout=30)
            return response.read().decode(deCode)
        except Exception as ex:
            print "UrlConnMPrice getContent error"
            print ex
            return None
    
class UrlConnPriceNew(UrlConn):
    def __init__(self,stkType):
        #self.urlBase1='http://www.twse.com.tw/ch/trading/exchange/BFT41U/genpage/Report201206/A41120120619ALL.php?chk_date=101/06/19&select2=ALL'
        self.urlBase1='http://www.twse.com.tw/ch/trading/exchange/BFT41U/genpage/Report'
        #self.urlBase2='http://www.otc.org.tw/ch/stock/aftertrading/DAILY_CLOSE_quotes/RSTA3104_1010622.html'
        self.urlBase2='http://www.otc.org.tw/ch/stock/aftertrading/DAILY_CLOSE_quotes/'
        urlData={}
        if stkType=='sii':
            UrlConn.__init__(self,self.urlBase1,urlData)
        elif stkType=='otc':
            UrlConn.__init__(self,self.urlBase2,urlData) 
            
    def updateUrlData(self,addedData):
        
        date=addedData['date']
        year,month,day=(date.year,date.month,date.day)
        stkType=addedData['stkType']
        
        if month<10: month='0%s' % month
        if day<10: day='0%s' % day
        
        if stkType==0: # sii
            self.url=self.urlBase1+"%s%s/A411%s%s%sALL.php?chk_date=%s/%s/%s&select2=ALL" % (year,month,year,month,day,year-1911,month,day)
        elif stkType==1: #otc
            print self.urlBase2+"RSTA3104_%s%s%s.html" % (year-1911,month,day)
            self.url=self.urlBase2+"RSTA3104_%s%s%s.html" % (year-1911,month,day)
    
    def getContent(self,deCode="utf-8",stkType=0):
        try:
            if stkType==0:
                response=urllib2.urlopen(self.getWholeUrlName(),timeout=30)
            elif stkType==1:
                response=urllib2.urlopen(self.req,timeout=30)
            return response.read().decode(deCode)
        except Exception as ex:
            print ex
            return None
        
    def getHtml(self,deCode="utf-8",stkType=0):
        return fromstring(self.getContent(deCode,stkType))
    
    
    