#' getHashString
#' 
#' Get a hash string, i.e. a string unlikely to occur again
#' @param pattern   a prefix that will be used in the returned hash string
#' @param useDate   Should the current time and date be used in the hash string to make it even more unique
#' @return a character string unlikely to occur again
#' @author Fabian Mueller
#' @export 
#' @examples 
#' getHashString()
getHashString <- function(pattern="", useDate=TRUE){
	pat <- pattern
	if (useDate) {
		pat <- paste(pat, format(Sys.time(), "%Y%m%d_%H%M%S_"), sep="_")
	}
	res <- basename(tempfile(pattern=pat))
	return(res)
}

# #' checkErrorsInLogFiles
# #' 
# #' Check and report lines containing error keywords in text files
# #' @param logFns   (vector of) log/text file names
# #' @return ...
# #' @author Fabian Mueller
# #' @export
# checkErrorsInLogFiles <- function(logFns, errorKeys=c("error", "fail", "except")){
# 	return("")
# }
