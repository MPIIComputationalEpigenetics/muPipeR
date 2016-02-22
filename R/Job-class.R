#' Job
#'
#' A class for storing information on pipeline jobs.
#'
#' @section Slots:
#' \describe{
#'   \item{\code{cmd}}{
#'		The command or tool to be executed. Must be a single element character
#'      string containing no white spaces.
#'   }
#'   \item{\code{args}}{
#'		A character vector specifying additional arguments to the command or tool
#'   }
#'   \item{\code{id}}{
#'		A name or id for the job
#'   }
#'   \item{\code{dependsOn}}{
#'		A list of other \code{Job} objects that the current job depends on.
#'   }
#' }
#'
#' @section Methods:
#' \describe{
#'    \item{\code{
#' 		\link{getId,Job-method},
#' 		\link{getCmd,Job-method},
#' 		\link{getArgs,Job-method},
#' 		\link{getDepJobs,Job-method}
#'    }}{
#'      Getters for the \code{id, cmd, args, dependsOn} slots
#'    }
#'    \item{\code{\link{getCallString,Job-method}}}{
#'      concatenate the command and the arguments into one string 
#'    }
#' }
#'
#' @name Job-class
#' @rdname Job-class
#' @author Fabian Mueller
#' @exportClass Job
setClass("Job",
	slots = list(
		cmd       = "character",
		args      = "character",
		id        = "characterOrNULL",
		dependsOn = "list" # list of jobs the current job depends on
	),
	package = "muPipeR"
)
setMethod("initialize","Job",
	function(
		.Object,
		cmd,
		args=character(),
		id=NULL,
		dependsOn=list()
	) {
		.Object@cmd       <- cmd
		.Object@args      <- args
		.Object@id        <- id
		.Object@dependsOn <- dependsOn
		.Object
	}
)
#' @param cmd       The command or tool to be executed. Must be a single element
#'                  character string containing no white spaces.
#' @param args      A character vector specifying additional arguments to the 
#'                  command or tool
#' @param id        A name or id for the job
#' @param dependsOn A list of other \code{Job} objects that the current job depends on.
#' @name Job
#' @rdname Job-class
#' @aliases intialize,Job-method
#' @export
Job <- function(cmd, args=character(), id=NULL, dependsOn=list()){
	obj <- new("Job",
		cmd, args, id, dependsOn
	)
	return(obj)
}
################################################################################
# Getters
################################################################################
if (!isGeneric("getId")) {
	setGeneric(
		"getId",
		function(object) standardGeneric("getId"),
		signature=c("object")
	)
}
#' getId-methods
#'
#' Return the id/name of the object
#'
#' @param object \code{\linkS4class{Job}} object
#' @return Character specifying the id.
#'
#' @rdname getId-Job-method
#' @docType methods
#' @aliases getId
#' @aliases getId,Job-method
#' @author Fabian Mueller
#' @export
setMethod("getId",
	signature(
		object="Job"
	),
	function(
		object
	) {
		return(object@id)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getCmd")) {
	setGeneric(
		"getCmd",
		function(object) standardGeneric("getCmd"),
		signature=c("object")
	)
}
#' getCmd-methods
#'
#' Return the command/tool of the object
#'
#' @param object \code{\linkS4class{Job}} object
#' @return Character specifying the command/tool.
#'
#' @rdname getCmd-Job-method
#' @docType methods
#' @aliases getCmd
#' @aliases getCmd,Job-method
#' @author Fabian Mueller
#' @export
setMethod("getCmd",
	signature(
		object="Job"
	),
	function(
		object
	) {
		res <- object@cmd
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getArgs")) {
	setGeneric(
		"getArgs",
		function(object) standardGeneric("getArgs"),
		signature=c("object")
	)
}
#' getArgs-methods
#'
#' Return the additional arguments for the command call of the object
#'
#' @param object \code{\linkS4class{Job}} object
#' @return Character vector specifying additional arguments to the tool/command
#'
#' @rdname getArgs-Job-method
#' @docType methods
#' @aliases getArgs
#' @aliases getArgs,Job-method
#' @author Fabian Mueller
#' @export
setMethod("getArgs",
	signature(
		object="Job"
	),
	function(
		object
	) {
		res <- object@args
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getCallString")) {
	setGeneric(
		"getCallString",
		function(object) standardGeneric("getCallString"),
		signature=c("object")
	)
}
#' getCallString-methods
#'
#' Concatenate the command and arguments into a single string for system calls.
#'
#' @param object \code{\linkS4class{Job}} object
#' @return Character string concatenating the command and arguments
#'
#' @rdname getCallString-Job-method
#' @docType methods
#' @aliases getCallString
#' @aliases getCallString,Job-method
#' @author Fabian Mueller
#' @export
setMethod("getCallString",
	signature(
		object="Job"
	),
	function(
		object
	) {
		sep <- " "
		res <- paste(getCmd(object), paste(getArgs(object), collapse=sep), sep=sep)
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getDepJobs")) {
	setGeneric(
		"getDepJobs",
		function(object) standardGeneric("getDepJobs"),
		signature=c("object")
	)
}
#' getDepJobs-methods
#'
#' Return other jobs the current job depends on
#'
#' @param object \code{\linkS4class{Job}} object
#' @return The list of other \code{Job} objects that the current job depends on.
#'
#' @rdname getDepJobs-Job-method
#' @docType methods
#' @aliases getDepJobs
#' @aliases getDepJobs,Job-method
#' @author Fabian Mueller
#' @export
setMethod("getDepJobs",
	signature(
		object="Job"
	),
	function(
		object
	) {
		return(object@dependsOn)
	}
)
