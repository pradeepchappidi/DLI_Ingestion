#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import sleep
import curses, os #curses is the interface for capturing key presses on the menu, os launches the files
screen = curses.initscr() #initializes a new window for capturing key presses
curses.noecho() # Disables automatic echoing of key presses (prevents program from input each key twice)
curses.cbreak() # Disables line buffering (runs each key as it is pressed rather than waiting for the return key to pressed)
curses.start_color() # Lets you use colors when highlighting selected menu option
screen.keypad(1) # Capture input from keypad

# Change this to use different colors when highlighting
curses.init_pair(1,curses.COLOR_BLACK, curses.COLOR_WHITE) # Sets up color pair #1, it does black text with white background
h = curses.color_pair(1) #h is the coloring for a highlighted menu option
n = curses.A_NORMAL #n is the coloring for a non highlighted menu option

MENU = "menu"
COMMAND = "command"
EXITMENU = "exitmenu"

menu_data = {
  'title': "Big Data Ingestion", 'type': MENU, 'subtitle': "select a Ingestion type...",
  'options':[
        { 'title': "RDBMS", 'type': MENU, 'subtitle': "RDBMS Based Source Ingestion", 
            'options': [
                        { 'title': "ORACLE Source ", 'type': MENU, 'subtitle': "ORACLE Source Ingestion",
                             'options': [
                                           { 'title': "ALL Tables", 'type': COMMAND, 'command': '/home/PXCHAPP/DLI/test_connection_ksh; echo "functionality comming"; read; ' },
                                           { 'title': "I will Give a List of Tables", 'type': COMMAND, 'command': ' /home/PXCHAPP/DLI/test_connection_ksh; echo "functionality comming"; read; ' },
                                           { 'title': "Will Provide a File with tables List", 'type': COMMAND, 'command': '/home/PXCHAPP/DLI/test_connection_ksh; echo "functionality comming"; read;  ' },
                                           { 'title': "Will give my SELECT statment", 'type': COMMAND, 'command': ' /home/PXCHAPP/DLI/test_connection_ksh echo "functionality comming"; read; ' },
                                           { 'title': "check my connection", 'type': COMMAND, 'command': '/home/PXCHAPP/DLI/test_connection_ksh' },
                                        ]
                         },

       
                        { 'title': "DB2 Source", 'type': COMMAND, 'command': ' echo "functionality comming"; read;  ' },
                        { 'title': "MYSQL Source", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                       ]
        },

        { 'title': "REAL-TIME", 'type': MENU, 'subtitle': "REAL-TIME Based Source Ingestion", 
            'options': [
                        { 'title': "Flume", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "KAFKA", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                       ]
        },
 
        { 'title': "FTP-BASED", 'type': MENU, 'subtitle': "FTP-Based Source Ingestion", 
            'options': [
                        { 'title': "Midnight Rescue", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Outnumbered", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Treasure Mountain", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                       ]
        },

        { 'title': "HDFS-OPERATIONS", 'type': MENU, 'subtitle': "HDFS Operations", 
            'options': [
                        { 'title': "List files", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Copy a file to my local", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Data Usage", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                       ]
        },

        { 'title': "WORK-FLOW MONITORING", 'type': MENU, 'subtitle': "Work-Flow monitoring and Operations",  
            'options': [
                        { 'title': "Suspend Workflows", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Start workflows", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                        { 'title': "Delete workflow", 'type': COMMAND, 'command': ' echo "functionality comming"; read; ' },
                       ]
        },


  ]
}



# This function displays the appropriate menu and returns the option selected
def runmenu(menu, parent):

  # work out what text to display as the last menu option
  if parent is None:
    lastoption = "Exit"
  else:
    lastoption = "Return to %s menu" % parent['title']

  optioncount = len(menu['options']) # how many options in this menu

  pos=0 #pos is the zero-based index of the hightlighted menu option. Every time runmenu is called, position returns to 0, when runmenu ends the position is returned and tells the program what opt$
  oldpos=None # used to prevent the screen being redrawn every time
  x = None #control for while loop, let's you scroll through options until return key is pressed then returns pos to program

  # Loop until return key is pressed
  while x !=ord('\n'):
    if pos != oldpos:
      oldpos = pos
      screen.border(0)
      screen.addstr(2,2, menu['title'], curses.A_STANDOUT) # Title for this menu
      screen.addstr(4,2, menu['subtitle'], curses.A_BOLD) #Subtitle for this menu

      # Display all the menu items, showing the 'pos' item highlighted
      for index in range(optioncount):
        textstyle = n
        if pos==index:
          textstyle = h
        screen.addstr(5+index,4, "%d - %s" % (index+1, menu['options'][index]['title']), textstyle)
      # Now display Exit/Return at bottom of menu
      textstyle = n
      if pos==optioncount:
        textstyle = h
      screen.addstr(5+optioncount,4, "%d - %s" % (optioncount+1, lastoption), textstyle)
      screen.refresh()
      # finished updating screen

    x = screen.getch() # Gets user input

    # What is user input?
    if x >= ord('1') and x <= ord(str(optioncount+1)):
      pos = x - ord('0') - 1 # convert keypress back to a number, then subtract 1 to get index
    elif x == 258: # down arrow
      if pos < optioncount:
        pos += 1
      else: pos = 0
    elif x == 259: # up arrow
      if pos > 0:
        pos += -1
      else: pos = optioncount

  # return index of the selected item
  return pos

# This function calls showmenu and then acts on the selected item
def processmenu(menu, parent=None):
  optioncount = len(menu['options'])
  exitmenu = False
  while not exitmenu: #Loop until the user exits the menu
    getin = runmenu(menu, parent)
    if getin == optioncount:
        exitmenu = True
    elif menu['options'][getin]['type'] == COMMAND:
      curses.def_prog_mode()    # save curent curses environment
      os.system('reset')
      if menu['options'][getin]['title'] == 'Pianobar':
        os.system('amixer cset numid=3 1') # Sets audio output on the pi to 3.5mm headphone jack
      screen.clear() #clears previous screen
      os.system(menu['options'][getin]['command']) # run the command
      screen.clear() #clears previous screen on key press and updates display based on pos
      curses.reset_prog_mode()   # reset to 'current' curses environment
      curses.curs_set(1)         # reset doesn't do this right
      curses.curs_set(0)
      os.system('amixer cset numid=3 2') # Sets audio output on the pi back to HDMI
    elif menu['options'][getin]['type'] == MENU:
          screen.clear() #clears previous screen on key press and updates display based on pos
          processmenu(menu['options'][getin], menu) # display the submenu
          screen.clear() #clears previous screen on key press and updates display based on pos
    elif menu['options'][getin]['type'] == EXITMENU:
          exitmenu = True

# Main program
processmenu(menu_data)
curses.endwin() #VITAL! This closes out the menu system and returns you to the bash prompt.
os.system('clear')
