# -*- coding=utf-8 -*-
import unittest
from UrlConnection import *
from Util import list2dict

class TestUrlConneSale(unittest.TestCase):
    
    def setUp(self):
        self.conn=UrlConnCash()
    
    def test_updateUrlData(self):
        urlKey=['co_id','year','season']
        urlData=('2317','95','01')
        wholeUrlName='http://mops.twse.com.tw/mops/web/ajax_t05st36?co_id=%s&year=%s&season=%s' % urlData
        self.conn.updateUrlData(list2dict(urlKey,urlData))
        self.assertEqual(wholeUrlName, self.conn.getWholeUrlName())
    
    
    def test_preventBlockByServer(self):
        urlKey=['co_id','year','season']
        urlData=('2317','95','01')
        self.conn.updateUrlData(list2dict(urlKey,urlData))
        
        count=0
        while count<100:
            try:
                self.conn.flush()
                content=self.conn.getContent()
                count+=1
            except:
                break
        print "count=%s" % count
        self.assertTrue(count>=100, "count=%s, the tests failed!" % count)
                
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()