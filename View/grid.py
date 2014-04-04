# -*- coding=utf-8 -*-

import wx
import wx.grid
from Model.model import TableModel
from Model.model import Entry
from Controller.mainCtrl import Controller
  
class gridview(wx.grid.Grid):
    title1=[u'股票代碼',u'公司',u'十年均現金股利',u'股利價值',u'均本益比',u'EPS TTM',u'EPS成長率',u'淨值成長率',u'銷售成長率',u'現金流成長率',u'EPS預估成長率', \
             u'成長價值',u'現價',u'評估價格',u'俗價比','股利價比',u'3年均配息率',u'6年均配息率',u'10年均配息率',u'1年均ROE',u'3年均ROE',u'6年均ROE',u'10年均ROE']        
    
    ColorEven=wx.Colour(219,246,255)
    ColorOdd=wx.Colour(255,255,255)
    ColorSelected=wx.Colour(82,125,255)
    
    def __init__(self,parent,*args,**kwargs):
        wx.grid.Grid.__init__(self,parent,*args,**kwargs) 
        self.initData()
        self.SetTable(self.model)
        self.initUI()
        self.setEvent()
        self.EnableEditing(False)
        
    
    def setController(self, control):
        self.controller = control
        
    def initData(self):
        self.model=TableModel(self.title1,[])
    
    def updateData(self,entries):
        newModel=TableModel(self.title1,entries)
        self.model=newModel
        self.SetTable(self.model)
        self.myTable=self.model
            
    def initUI(self):
        self.SetColLabelSize(20)
        self.SetRowLabelSize(40)
        self.adjusetColSize()
        
        self.lastRowSelected=-1
        self.rowSelected=0
        self.SetCellHighlightPenWidth(0)   
    
    def adjusetColSize(self):
        for i in range(self.GetNumberCols()):
            if self.popmenu.IsChecked(i+1): 
                self.SetColSize(i,85)
            
    def colorattr(self,color):
        attr=wx.grid.GridCellAttr()
        attr.SetBackgroundColour(color)
        return attr
    
    def setStyle(self):
        for i in range(self.GetNumberRows()):
            self.setRowColorStyle(i)
            
    def setRowColorStyle(self,row):
        if row%2==0:
            self.SetRowAttr(row,self.colorattr(gridview.ColorEven))
        else:
            self.SetRowAttr(row,self.colorattr(gridview.ColorOdd))

    def paintSelectedRow(self,selectedRow):
        if self.lastRowSelected!=selectedRow:
            self.setRowColorStyle(self.lastRowSelected)
            self.SetRowAttr(selectedRow,self.colorattr(gridview.ColorSelected))
        self.lastRowSelected=selectedRow
    
    def setEvent(self):
        self.Bind(wx.grid.EVT_GRID_SELECT_CELL, self.onGrid)
        self.Bind(wx.grid.EVT_GRID_LABEL_LEFT_CLICK, self.onGridLabel)
        self.Bind(wx.grid.EVT_GRID_RANGE_SELECT, self.onRangeSelect)
        self.GetGridWindow().Bind(wx.EVT_MOTION,self.onMouseMotion)
        self.Bind(wx.grid.EVT_GRID_LABEL_RIGHT_CLICK, self.onShowPopup)
        
    def setPopupMenu(self):
        self.popmenu=pop=wx.Menu()    
        for i in range(self.NumberCols):
            item=pop.AppendCheckItem(i+1,self.GetColLabelValue(i))
            self.Bind(wx.EVT_MENU,self.onPopmenuItem,item)
            pop.Check(i+1,True)
    
    def onPopmenuItem(self,evt):
        self.updateColVisible()
        
    def updateColVisible(self):
        for i in range(self.NumberCols):        
            if self.popmenu.IsChecked(i+1): 
                self.ShowCol(i)
            else: 
                self.HideCol(i)
        self.adjusetColSize()
          
    def onShowPopup(self,evt):
        pos=evt.GetPosition()   
        self.PopupMenu(self.popmenu,pos)
        pass
    
    def onMouseMotion(self,evt):
        if evt.Dragging():
            pass
        else:
            evt.Skip()
    
    def updateRowInVisibleView(self):
        x, y = self.CalcUnscrolledPosition(0,0)
        coords = self.XYToCell(x, y)
        col = coords[1]
        row = coords[0]
        self.rowStart=row
           
    def onRangeSelect(self,evt):
        rowT = evt.GetTopRow() 
        rowB = evt.GetBottomRow() 
        if rowT != rowB: 
           for i in range(self.GetNumberRows()): 
               self.DeselectRow(i)
    
    def onGridLabel(self,evt):
        fieldName=self.controller.getTblFieldByIndex(evt.GetCol())
        self.controller.updateAscType(fieldName)
        self.controller.updateTable(Controller.tblName)
        self.setStyle()
        self.updateColVisible()
        self.Refresh()
        
        
    def onGrid(self,evt):
        #print "r,c=%s,%s" % (evt.GetRow(),evt.GetCol())
        self.BeginBatch()
        
        self.rowSelected=evt.GetRow()
        #self.updateRowInVisibleView()
        self.paintSelectedRow(self.rowSelected) 
        self.Refresh()
        
        self.EndBatch()
        #evt.Skip()
