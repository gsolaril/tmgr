import os, sys

PATH_MAIN = __file__.split(os.path.sep)[: -2]
PATH_MAIN = str.join(os.path.sep, PATH_MAIN)
sys.path.append(PATH_MAIN)