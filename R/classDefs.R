################################################################################
# Class definitions and extensions for default classes and other
# packages
################################################################################
setClassUnion("characterOrNULL", c("character", "NULL"))
setOldClass(c("igraph"), prototype=structure(list(), class="igraph"))
