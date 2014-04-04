# -*- coding: utf8 -*-

import sys
import re
import wx
import wx.grid
from os import path

import View.grid
import View.mainFrame
from DB import db
from Model import model
from Controller.mainCtrl import Controller
from Util import *
        
class myApp(wx.App):
    def __init__(self,db):
        wx.App.__init__(self)
        
        frame=View.mainFrame.myFrame(None,-1,"Stock Wizard")
        ctrl = Controller(db, frame)
        frame.gridPanel.setStyle()
        frame.Show()
        frame.Raise()

if __name__ == '__main__':
    
    print "App Start"
    dbFile='stockDB.db'
    
    print path.realpath(dbFile)
    
    DB=db(dbFile)
    #DB=db("%s/%s" % (sys.path[0],dbFile))   
    
    
    for tbl in DB.tables:
        if tbl.shouldUpdate==False:continue
        try:
            tbl.update()
        except Exception as ex:
            print "error : %s" % (ex,)
    
    app = myApp(DB)
    app.MainLoop()
    
    DB.closeCursor()
    print "App End"
    
    

    
    