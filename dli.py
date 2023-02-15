#!/usr/bin/env python
# -*- coding: utf-8 -*-
#title           :menu.py
#description     :This program displays an interactive menu on CLI
#author          :
#date            :
#version         :0.1
#usage           :python menu.py
#notes           :
#python_version  :2.7.6  
#=======================================================================
 
# Import the modules needed to run the script.
import sys, os
 
# Main definition - constants
menu_actions  = {}  
 
# =======================
#     MENUS FUNCTIONS
# =======================
 
# Main menu
def main_menu():
    os.system('clear')
    
    print "Welcome,\n"
    print "Please choose the menu you want to start:"
    print "1. RDBMS BASED INGESTION"
    print "2. FTP BASED INGESTION"
    print "3. REAL-TIME BASED INGESTION"
    print "4. OPERATIONS ON HDFS"
    print "5. MONITOR BIG DATA WORKFLOWS"
    print "\n0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
 
    return
 
# Execute menu
def exec_menu(choice):
    os.system('clear')
    ch = choice.lower()
    if ch == '':
        menu_actions['main_menu']()
    else:
        try:
            menu_actions[ch]()
        except KeyError:
            print "Invalid selection, please try again.\n"
            menu_actions['main_menu']()
    return
 
def RDBMS_Menu():
    print "RDBMS Ingestion Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return



def ORACLE_Menu():
    print "ORACLE Type Ingestion Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return


def MYSQL_Menu():
    print "MYSQL Type Ingestion Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return




def DB2_Menu():
    print "DB2 Type Ingestion Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return


 
def FTP():
    print "FTP Ingestion Menu!\n"
    print "9. Back"
    print "0. Quit" 
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return

def REAL_TIME():
    print "REAL_TIME Ingestion MENU !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return


def HDFS_OPS():
    print "HDFS Operation Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return


def WORKFLOW_MONITOR():
    print "WorkFlow Monitor Menu !\n"
    print "9. Back"
    print "0. Quit"
    choice = raw_input(" >>  ")
    exec_menu(choice)
    return



 
# Back to main menu
def back():
    menu_actions['main_menu']()
 
# Exit program
def exit():
    sys.exit()
 
# =======================
#    MENUS DEFINITIONS
# =======================
 
# Menu definition
menu_actions = {
    'main_menu': main_menu,
    '1': RDBMS,
    '2': FTP,
    '3': REAL_TIME,
    '4': HDFS_OPS,
    '5': WORKFLOW_MONITOR,
    '9': back,
    '0': exit,
}
 
# =======================
#      MAIN PROGRAM
# =======================
 
# Main Program
if __name__ == "__main__":
    # Launch main menu
    main_menu()
