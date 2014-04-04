# -*- coding=utf-8 -*-

import wx
from Controller.mainCtrl import Controller
from Model.model import condition
from View.grid import gridview

class myFrame(wx.Frame):
    '''
    classdocs
    '''
    def __init__(self,*args,**kwargs):
        wx.Frame.__init__(self,*args,**kwargs)
        self.gridPanel = gridview(self)
        self.setMenu()
        self.addToolbar()
        self.setFrameSize()
        self.setLayout()
        self.Centre()
    
    def setController(self, control):
        self.controller = control
        
    def quit(self,evt):
        self.controller.myDb.closeCursor()
        self.Destroy()
        print "quit"
         
    def addToolbar(self):
        self.toolbar = self.CreateToolBar()
        
        save_ico = wx.ArtProvider.GetBitmap(wx.ART_FILE_SAVE, wx.ART_TOOLBAR, (16, 16))
        saveTool = self.toolbar.AddSimpleTool(wx.ID_ANY, save_ico, "Save", "Saves the Current Worksheet")
        self.Bind(wx.EVT_TOOL, self.DoTool1, saveTool)
        
        self.txtID1=wx.NewId()
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label=u"股票代碼", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID1, ""))
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID1)
        
        self.txtID1_2=wx.NewId()
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label=u"股票名稱", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID1_2, ""))
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID1_2)
        
        self.txtID2_1,self.txtID2_2=(wx.NewId(),wx.NewId())
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label=u"俗價比", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID2_1, ""))
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label="~", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID2_2, ""))
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID2_1)
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID2_2)
    
        self.txtID3_1,self.txtID3_2=(wx.NewId(),wx.NewId())
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label=u"股利價比", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID3_1, ""))
        self.toolbar.AddControl(wx.StaticText(parent=self.toolbar, label="~", style=wx.ALIGN_CENTER, size=wx.Size(60, 30)))
        self.toolbar.AddControl(wx.TextCtrl(self.toolbar, self.txtID3_2, ""))
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID3_1)
        self.Bind(wx.EVT_TEXT,self.filter,id=self.txtID3_2)
        
        self.Bind(wx.EVT_CLOSE,self.quit)
        self.toolbar.Realize()
    
    def getCondition(self):
        
        Controller.cd.clear()
        Controller.cd.addCondition('stkID', self.FindWindowById(self.txtID1).GetValue(),condition.CONDITION_LIKE)
        Controller.cd.addCondition('name', self.FindWindowById(self.txtID1_2).GetValue(),condition.CONDITION_KEYWORD)
        #
        Controller.cd.addCondition('PriceRatio', self.FindWindowById(self.txtID2_1).GetValue(),condition.CONDITION_LARGERTHAN)
        Controller.cd.addCondition('PriceRatio', self.FindWindowById(self.txtID2_2).GetValue(),condition.CONDITION_SMALLERTHAN)
        #
        Controller.cd.addCondition('divPriceRatio', self.FindWindowById(self.txtID3_1).GetValue(),condition.CONDITION_LARGERTHAN)
        Controller.cd.addCondition('divPriceRatio', self.FindWindowById(self.txtID3_2).GetValue(),condition.CONDITION_SMALLERTHAN)
        #
        return Controller.cd.getCondition()
    
    def filter(self,evt):
        self.controller.updateCondition(self.getCondition())
        #self.controller.loadTable(Controller.tblName)
        self.controller.updateTable(Controller.tblName)
        self.gridPanel.setStyle()
        self.gridPanel.updateColVisible()
        self.gridPanel.Refresh()
    
    def setMenu(self):
        
        menubar = wx.MenuBar()
        
        menu1 = wx.Menu()
        mI1_1 = menu1.Append(wx.NewId(), "1.1")
        mI1_2 = menu1.Append(wx.NewId(), "1.2")
        mI1_3 = menu1.Append(wx.NewId(), "1.3")
        self.Bind(wx.EVT_MENU, self.menuClick, mI1_1, 1)
        self.Bind(wx.EVT_MENU, self.menuClick, mI1_2, 2)
        self.Bind(wx.EVT_MENU, self.menuClick, mI1_3, 3)
        
        menu2 = wx.Menu()
        mI2_1 = menu2.Append(wx.ID_ANY, "2.1")
        mI2_2 = menu2.Append(wx.ID_ANY, "2.2")
        mI2_3 = menu2.Append(wx.ID_ANY, "2.3")
        menu2.AppendSeparator()
        
        menu2_4 = wx.Menu()
        mI241 = menu2_4.Append(wx.ID_ANY, "2.4.1")
        mI242 = menu2_4.Append(wx.ID_ANY, "2.4.2")
        menu2.AppendMenu(wx.ID_ANY, "2.4", menu2_4)
        
        self.doBind(mI2_1, self.menuClick)
        self.doBind(mI242, self.menuClick)
        
        menubar.Append(menu1, "File")
        menubar.Append(menu2, "Edit")
        
        self.SetMenuBar(menubar)
    
    def doBind(self, item, handler):
        ''' Create menu events. '''
        self.Bind(wx.EVT_MENU, handler, item)
    
    def DoTool1(self, evt):
        print "Saveing..."
    
    def menuClick(self, evt):
        print "menuitem clicked"
    
    def setFrameSize(self,size=(1100,700)):
        self.SetSize(size)
        
    def setLayout(self):
        self.gridPanel.SetSize(self.Size)

        