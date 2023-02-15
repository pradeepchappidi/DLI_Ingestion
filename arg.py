#!/usr/bin/env python
from optparse import OptionParser
__version__ = '0.0.1'

def main():
    usage = u'%prog'
    parser = OptionParser(usage = usage, version = __version__)
    parser.add_option('-g', '--git',
            action = 'store_true',
            dest = 'is_git',
            help = 'show hello git!'
    )

    parser.add_option('-w', '--world',
            action = 'store_true',
            dest = 'is_world',
            help = 'show hello world!'
    )

    parser.add_option('-m', '--message',
            action = 'store',
            type = 'string',
            dest = 'msg',
            help = 'show hello msg'
    )

    parser.set_defaults(
            is_git = False,
            is_world = False,
            msg = None
    )

    options, args = parser.parse_args()
    
    helloto = ''
    if options.is_git:
        helloto = 'git!'
    
    if options.is_world:
        helloto = 'world!'
    
    msg = options.msg
    if msg != None:
        helloto = msg

    print "hello %s" % helloto

if __name__ == '__main__':
    main()
