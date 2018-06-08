#' CommandRsystem Class
#'
#' A child class of \code{\linkS4class{CommandR}} implementing job submission using system calls.
#'
#' @details
#' Follow this template if you want to create your own \code{\linkS4class{CommandR}} class.
#'
#' @section Slots:
#' Includes all the slots of the \code{\linkS4class{CommandR}} class
#'
#' @section Methods:
#' implements all the methods of the \code{\linkS4class{CommandR}} class.
#'
#' @name CommandRsystem-class
#' @rdname CommandRsystem-class
#' @author Fabian Mueller
#' @exportClass CommandRsystem
setClass("CommandRsystem",
	contains="CommandR",
	package = "muPipeR"
)
setMethod("initialize","CommandRsystem",
	function(
		.Object,
		logDir=NULL
	) {
		.Object <- callNextMethod(.Object)
		.Object@logDir <- logDir
		.Object@handlesJobDeps <- FALSE
		logger.info("Dependend jobs are not supported during execution by CommandRsystem. --> Assuming dependencies have already completed")
		.Object
	}
)
#' @param logDir logging directory
#' 
#' @name CommandRsystem
#' @rdname CommandRsystem-class
#' @aliases intialize,CommandRsystem-method
#' @export
CommandRsystem <- function(logDir=NULL){
	obj <- new("CommandRsystem",
		logDir
	)
	return(obj)
}
#-------------------------------------------------------------------------------
#' exec-methods
#'
#' Executes a job given the job specifics using system calls
#' @details
#' Instantiates the method \code{\link{exec,CommandR-method}} for \code{\linkS4class{CommandRsystem}} objects.
#'
#' @param object   \code{\linkS4class{CommandRsystem}} object
#' @param job      \code{\linkS4class{Job}} object
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @return \code{\linkS4class{JobResult}} object
#'
#' @rdname exec-CommandRsystem-method
#' @docType methods
#' @aliases exec,CommandRsystem-method
#' @author Fabian Mueller
#' @export
setMethod("exec",
	signature(
		object="CommandRsystem"
	),
	function(
		object,
		job,
		result=FALSE,
		wait=TRUE,
		...
	) {
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		# if (length(getDepJobs(job)) > 0){
		# 	logger.info("Dependend jobs are not supported during execution by CommandRsystem. --> Assuming dependencies have already completed")
		# }

		logStruct <- getLoggingStruct(object, job)
		logFile   <- logStruct$logFile
		errFile   <- logStruct$errFile
		jid       <- logStruct$jobId

		#currently system2 does not support ~ in file names:
		logFile <- path.expand(logFile)
		errFile <- path.expand(errFile)
		
		sysRes <- system2(getCmd(job), getArgs(job), wait=wait, stdout=logFile, stderr=errFile, ...)
		print(getCmd(job))
		print(getArgs(job))

		sysOut <- character()
		sysErr <- character()
		if (wait){
			sysStatus <- sysRes
		} else {
			sysStatus <- attr(sysRes, "status")
			if (is.null(sysStatus)) sysStatus <- 0L
		}

		if (result){
			sysOut <- readLines(logFile)
			sysErr <- readLines(errFile)
		}

		res <- JobResult(out=sysOut, err=sysErr, status=sysStatus, command=getCallString(job))
		return(res)
	}
)
#-------------------------------------------------------------------------------
#' lexec-methods
#'
#' executes an array of jobs (list execute --> lexec) given the job specifics in the input list of jobs
#' @details
#' Instantiates the method \code{\link{lexec,CommandR-method}} for \code{\linkS4class{CommandRsystem}} objects.
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param jobList  a list of \code{\linkS4class{Job}} objects
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @return a list of \code{\linkS4class{JobResult}} objects
#'
#' @rdname lexec-CommandRsystem-method
#' @docType methods
#' @aliases lexec,CommandRsystem-method
#' @author Fabian Mueller
#' @export
setMethod("lexec",
	signature(
		object="CommandRsystem"
	),
	function(
		object,
		jobList,
		result=FALSE,
		wait=TRUE
	) {
		#require(RnBeads) for parallel computing
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		if (parallel.isEnabled()){
			res <- foreach(i=1:length(jobList)) %dopar% {
				exec(object, jobList[[i]], wait=wait, result=result)
			}
		} else {
			res <- lapply(jobList, FUN=function(jj){
				exec(object, jj, wait=wait, result=result)
			})
		}
		return(res)
	}
)
