'''
Created on 2012/7/22

@author: kitty
'''
import sys
import re
import unittest
from DB import Table
from DB import db

class TestTable(unittest.TestCase):
    def setUp(self):
        dbFile='/Users/kitty/Documents/eclipseWorkspace/stockW_rev1/stockDB10.db'
        self.db=db(dbFile)
        
        self.table1=('Final', \
            ['stkid','name','avediv','priceddm','aveper','epsttm','epsrate','bvpsrate','salerate','cashrate','rate','pricegrowth', \
             'price','pcal','priceratio','divpriceratio','pay1','pay3','pay6','pay10','roe1','roe3','roe6','roe10'])
        self.table2=('stock', \
                     ['stkid','name','chairman','president','builddate','publicdate'])
        
        self.t1=Table(dbFile,self.table1[0])
        self.t2=Table(dbFile,self.table2[0])
    
    def tearDown(self):
        self.db.closeCursor()

    def test_getFields(self):
        self.assertEqual(self.table1[1],self.t1.getFields())
        self.assertEqual(self.table2[1],self.t2.getFields())
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()