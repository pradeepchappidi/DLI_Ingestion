
#!/usr/bin/env python


import os


## here we will have Tables that can be used for as 
## 1. Table datatype
## 2. Table for Html
## 3. Table for database
## 4. Table for csv type 

class GenericTable(object):
    #https://github.com/dprince/python-prettytable/blob/master/prettytable.py

   def __init__(self,**kwargs):
    
         """
         kwargs values
  
         column_names -- list or tuple of column names 
         column_types -- list or tupe of column types , default is all columns are considered string types


         """
   
         #Data setup 
         # member values

         self._column_names = []
         self._column_types = []
         self._rows = []
         self_column_meta = {}

         self._options = "column_names column_types".split()
        
         self._checkOptions(_options,kwargs)
         
         self._column_names = kwargs["column_names"]
         self._column_types = kwargs["column_types"]

         self_column_meta = self._checkColumns(_column_names,_column_types)   

   def _checkOptions(self,options,args):
         for option in options:
            if option not in args:
                self._usage(options)
                raise Exception("Unrecognised option: %s!" % option) 

   
   def _checkColumns(self,col_names,col_types):
        
        if (len(col_names) > len(col_types)) and (len(col_types) > 0):
          for counter, column in enumerate(col_names):
             if len(col_types) == 0 :
                tmp_dict[column] = "string"
             else:
                tmp_dict[column] = col_type[counter]          
        else:
          raise Exception("Given Columns(size) : %s  and columns type (size) : %s are not same" %len(col_names), len(col_types))          



  
