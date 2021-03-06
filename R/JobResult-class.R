#' JobResult
#'
#' A class for storing information on the results of pipeline jobs.
#'
#' @section Slots:
#' \describe{
#'   \item{\code{out}}{
#'		Character vector storing the output of the job.
#'   }
#'   \item{\code{err}}{
#'		Character vector storing errors that occurred during the execution of the job
#'   }
#'   \item{\code{status}}{
#'		Integer specifying the job's status. \code{0} for success.
#'   }
#'   \item{\code{command}}{
#'		Character storing the command used to run the job.
#'   }
#' }
#'
#' @section Methods:
#' \describe{
#'    \item{\code{
#' 		\link{getOut,JobResult-method},
#' 		\link{getErr,JobResult-method},
#' 		\link{getStatus,JobResult-method}
#'    }}{
#'      Getters for the \code{out, err, status} slots
#'    }
#' }
#'
#' @name JobResult-class
#' @rdname JobResult-class
#' @author Fabian Mueller
#' @exportClass JobResult
setClass("JobResult",
	slots = list(
		out     = "character",
		err     = "character",
		status  = "integer",
		command = "character"
	),
	package = "muPipeR"
)
setMethod("initialize","JobResult",
	function(
		.Object,
		out=character(),
		err=character(),
		status=0L,
		command=character()
	) {
		.Object@out     <- out
		.Object@err     <- err
		.Object@status  <- status
		.Object@command <- command
		.Object
	}
)
#' @param out    Character vector storing the output of the job.
#'                  character string containing no white spaces.
#' @param err    Character vector storing errors that occurred during the execution of the job
#'                  command or tool
#' @param command    The command used to run the job as character.
#' @param status Integer specifying the job's status. \code{0} for success.
#' @name JobResult
#' @rdname JobResult-class
#' @aliases intialize,JobResult-method
#' @export
JobResult <- function(out=character(), err=character(), status=0L, command=character()){
	obj <- new("JobResult",
		out, err, status, command
	)
	return(obj)
}
################################################################################
# Getters
################################################################################
if (!isGeneric("getOut")) {
	setGeneric(
		"getOut",
		function(object) standardGeneric("getOut"),
		signature=c("object")
	)
}
#' getOut-methods
#'
#' Return the output of the job.
#'
#' @param object \code{\linkS4class{JobResult}} object
#' @return Character vector specifying the output.
#'
#' @rdname getOut-JobResult-method
#' @docType methods
#' @aliases getOut
#' @aliases getOut,JobResult-method
#' @author Fabian Mueller
#' @export
setMethod("getOut",
	signature(
		object="JobResult"
	),
	function(
		object
	) {
		return(object@out)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getErr")) {
	setGeneric(
		"getErr",
		function(object) standardGeneric("getErr"),
		signature=c("object")
	)
}
#' getErr-methods
#'
#' Return the errors of the job.
#'
#' @param object \code{\linkS4class{JobResult}} object
#' @return Character vector specifying the errors.
#'
#' @rdname getErr-JobResult-method
#' @docType methods
#' @aliases getErr
#' @aliases getErr,JobResult-method
#' @author Fabian Mueller
#' @export
setMethod("getErr",
	signature(
		object="JobResult"
	),
	function(
		object
	) {
		return(object@err)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getStatus")) {
	setGeneric(
		"getStatus",
		function(object) standardGeneric("getStatus"),
		signature=c("object")
	)
}
#' getStatus-methods
#'
#' Return the status of the job.
#'
#' @param object \code{\linkS4class{JobResult}} object
#' @return Integer specifying the job's status. \code{0} for success.
#'
#' @rdname getStatus-JobResult-method
#' @docType methods
#' @aliases getStatus
#' @aliases getStatus,JobResult-method
#' @author Fabian Mueller
#' @export
setMethod("getStatus",
	signature(
		object="JobResult"
	),
	function(
		object
	) {
		return(object@status)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getCommand")) {
	setGeneric(
		"getCommand",
		function(object) standardGeneric("getCommand"),
		signature=c("object")
	)
}
#' getCommand-methods
#'
#' Return the command used to run the job.
#'
#' @param object \code{\linkS4class{JobResult}} object
#' @return Command used to run the job as character
#'
#' @rdname getCommand-JobResult-method
#' @docType methods
#' @aliases getCommand
#' @aliases getCommand,JobResult-method
#' @author Fabian Mueller
#' @export
setMethod("getCommand",
	signature(
		object="JobResult"
	),
	function(
		object
	) {
		return(object@command)
	}
)
