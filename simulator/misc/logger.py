#
# Copyright (c) 2011, EPFL (Ecole Politechnique Federale de Lausanne)
# All rights reserved.
#
# Created by Marco Canini, Daniele Venzano, Dejan Kostic, Jennifer Rexford
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   -  Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   -  Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   -  Neither the names of the contributors, nor their associated universities or
#      organizations may be used to endorse or promote products derived from this
#      software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import sys
import time
import copy
import platform
import traceback

import logging


class ColoredConsoleHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stderr):
        logging.StreamHandler.__init__(self, stream)

    def emit( self, record ):
        # Need to make a actual copy of the record 
        # to prevent altering the message for other loggers
        myrecord = copy.copy(record)
        levelno = myrecord.levelno
        if( levelno >= 50 ): # CRITICAL / FATAL
            color = '\x1b[31m' # red
        elif( levelno >= 40 ): # ERROR
            color = '\x1b[31m' # red
        elif( levelno >= 30 ): # WARNING
#           color = '\x1b[35m' # pink
            color = '\x1b[33m' # yellow
        elif( levelno >= 20 ): # INFO
            color = '\x1b[0m' # normal
        elif( levelno >= 10 ): # DEBUG
            color = '\x1b[32m' # green
        else: # NOTSET and anything else
            color = '\x1b[0m' # normal
        myrecord.msg = color + str( myrecord.msg ) + '\x1b[0m'  # normal
        logging.StreamHandler.emit( self, myrecord )

def getLogger(name, level):
    global file_handler

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not (len(logger.handlers) > 0 and type(logger.handlers[0]) == logging.StreamHandler):
        if platform.system() != 'Windows':
            # all non-Windows platforms are supporting ANSI escapes so we use them
            con_handler = ColoredConsoleHandler(sys.stdout)
            con_formatter = logging.Formatter('\x1b[1;33m%(name)s:\x1b[0m %(message)s')
        else:
            con_handler = logging.StreamHandler(sys.stdout)
            con_formatter = logging.Formatter('%(name)s: %(message)s')

        con_handler.setFormatter(con_formatter)
        con_handler.setLevel(level)
        logger.addHandler(con_handler)
    if level == logging.DEBUG:
        try:
            import SimPy.Simulation as Simulation
            class SimPyAdapter(logging.LoggerAdapter):
                def __init__(self, log, extra):
                    logging.LoggerAdapter.__init__(self, log, extra)

                def process(self, msg, kwargs):
                    return '[%f] %s' % (Simulation.now(), msg), kwargs

            logger = SimPyAdapter(logger,{})
        except:
            pass
    return logger

def init(outfile="stdout", level=logging.INFO):
    root_logger = logging.getLogger()

    if outfile != "stdout" and outfile != "stderr":
        file_handler = logging.FileHandler(outfile, mode="w")
        file_handler.setLevel(level)
        file_formatter = logging.Formatter('%(name)s: %(message)s')
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

