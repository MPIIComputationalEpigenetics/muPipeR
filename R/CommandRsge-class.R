#' CommandRsge Class
#'
#' A child class of \code{\linkS4class{CommandR}} implementing job submission using system calls.
#'
#' @details
#' Follow this template if you want to create your own \code{\linkS4class{CommandR}} class.
#'
#' @section Slots:
#' Includes all the slots of the \code{\linkS4class{CommandR}} class. Additionally includes:
#' \describe{
#'   \item{\code{req}}{Default resource requirements for submitting jobs to the grid engine.}
#' }
#'
#' @section Methods:
#' implements all the methods of the \code{\linkS4class{CommandR}} class.
#'
#' @name CommandRsge-class
#' @rdname CommandRsge-class
#' @author Fabian Mueller
#' @exportClass CommandRsge
setClass("CommandRsge",
	slots = list(
		req   = "characterOrNULL"
	),
	contains="CommandR",
	package = "muPipeR"
)
setMethod("initialize","CommandRsge",
	function(
		.Object,
		logDir=NULL,
		req=NULL
	) {
		if (is.null(logDir)){
			logDir <- tempdir()
			logger.warning(c("Did not specify directory for logging. --> using directory:", logDir))
		}
		if (!is.null(req)){
			if (any(is.na(names(req))) || is.null(names(req))){
				logger.error("Invalid resource requirement specification. Need names for all requirements")
			}
		}
		.Object <- callNextMethod(.Object)
		.Object@logDir <- logDir
		.Object@handlesJobDeps <- TRUE
		.Object@req <- req
		.Object
	}
)
#' @param logDir logging directory
#' @param req default resource requirements. Will be specified for each submitted job, if no
#'            other requirements are specified. Must be a named character vector.
#' 
#' @name CommandRsge
#' @rdname CommandRsge-class
#' @aliases intialize,CommandRsge-method
#' @export
CommandRsge <- function(logDir=NULL, req=NULL){
	obj <- new("CommandRsge",
		logDir, req
	)
	return(obj)
}
################################################################################
# Helpers
################################################################################
#' getSgeJobStatus
#' 
#' Query the grid engine using qstat and retrieve the status (known or unknown) 
#' of the job for a given job id
#' @param jids   job id(s)
#' @return a character string: "known" if the job is still known to the grid engine
#'         (running or scheduled). "unknown" if not.
#' @author Fabian Mueller
#' @noRd
getSgeJobStatus <- function(jids){
	require(XML)
	stateXmlStr <- system2("qstat", c("-xml"), stdout=TRUE)
	stateXml <- xmlToList(xmlParse(stateXmlStr), simplify=FALSE)
	
	jobList <- c(stateXml[["queue_info"]], stateXml[["job_info"]])
	if (is.null(jobList)) logger.error("failed to get proper info using qstat")
	jobNames <- vapply(jobList, FUN=function(x){
		if (is.element("JB_name", names(x))){
			return(x[["JB_name"]])
		}
		return(as.character(NA))
	}, character(1))
	res <- ifelse(jids %in% jobNames, "known", "unknown")
	return(res)
}
#' waitForJobsToTerminate
#' 
#' Wait for the given jobs to terminate
#' @details
#' repeatedly queries the grid engine to see whether the jobs are still "known".
#' Initially checks whether it can find all jobs on the grid engine
#' @param jids   character vector of job ids
#' @return nothing of particular interest
#' @author Fabian Mueller
#' @noRd
waitForJobsToTerminate <- function(jids){
	lag <- .config$waitLag
	initialize.lag  <- 1
	initialize.iter <- 60

	# initial check if all jobs are registered
	jids.invalid <- jids
	while (initialize.iter > 0 && length(jids.invalid) > 0){
		Sys.sleep(initialize.lag)
		jobStatus <- getSgeJobStatus(jids.invalid)
		jids.valid <- jids.invalid[jobStatus %in% c("known")]
		jids.invalid <- setdiff(jids.invalid, jids.valid)
		initialize.iter <- initialize.iter - 1
	}
	if (length(jids.invalid) > 0){
		logger.error(c("Could not retrieve status for the following jobs (already finished?):", paste(jids.invalid, collapse=",")))
	}
	logger.status("Waiting for jobs to complete...")
	# wait for jobs to complete:
	# check status of jobs until they become unknown to the grid engine
	jids.incomplete <- jids
	while (length(jids.incomplete) > 0){
		Sys.sleep(lag)
		jobStatus <- sapply(jids.incomplete, getSgeJobStatus)
		jids.done <- jids.incomplete[jobStatus %in% c("unknown")]
		jids.incomplete <- setdiff(jids.incomplete, jids.done)
	}
}
#-------------------------------------------------------------------------------
#' doQsub
#' 
#' Construct a submission string for a given \code{\linkS4class{Job}} object
#' and submit it using qsub and a system call.
#' @details
#' If the length of the command string of the job exceeds 1024 characters, it will be be
#' automatically be written to a shell script in the \code{shellScriptDir} directory
#' and the submitted job will execute this script file instead of the command directly.
#' @param job     \code{\linkS4class{Job}} object
#' @param logFile a log file where the output of job should be written to
#' @param errFile a error file where the error stream of the job should be written to
#' @param jobName a name for the submitted job
#' @param req     resource requirements for the submitted job. Must be a named character vector.
#' @param shellScriptDir directory where the shell script containing the command will be written
#'                to if the submitted command is too long for binary submission
#' @return the result of the system call using \link{system2}
#' @author Fabian Mueller
#' @noRd
doQsub <- function(job, logFile, errFile, jobName=getId(job), req=NULL, shellScriptDir=NULL){
	longCommandThres <- 1024L #submit via shell script if command length is longer than this
	reqTokens <- NULL
	if (length(req) > 0) {
		if (any(is.na(names(req))) || is.null(names(req))){
			logger.error("Invalid resource requirement specification. Need names for all requirements")
		}
		rr <- as.vector(rbind(rep("-l",length(req)),paste0(names(req),"=",req)))
		reqTokens <- c(reqTokens, rr)
	}

	depJobIds <- do.call("c", lapply(getDepJobs(job), getId))
	depToken <- NULL
	if (length(depJobIds)>0){
		depToken <- c("-hold_jid", paste0(paste(depJobIds, collapse=",")))
	}
	nameToken <- NULL
	if (!is.null(jobName)) nameToken <-c("-N", jobName)

	binSubmitToken <- "-b y"
	shellScriptSub <- FALSE
	runCmd <- getCallString(job)
	# check if the length of the command string exceeds the threshold
	# if it does, redirect the command to a shell script instead of executing it binary
	if (nchar(runCmd) > longCommandThres){
		if (is.null(shellScriptDir) || !dir.exists(shellScriptDir)){
			logger.warning(c("The command to submit exceeds", longCommandThres,
				"characters but no directory for shell script submission has been specified or it does not exist."))
		} else {
			scrptFn <- NULL
			if (!is.null(jobName)) {
				scrptFn <- file.path(shellScriptDir, paste0(jobName,".sh"))
			} else {
				scrptFn <- tempfile(pattern="scrpt", tmpdir=shellScriptDir, fileext=".sh")
			}
			fileConn <- file(scrptFn)
			writeLines(c("#!/bin/sh", runCmd), fileConn)
			close(fileConn)
			Sys.chmod(scrptFn, mode = "0755")
			runCmd <- scrptFn
			binSubmitToken <- "-b n"
			shellScriptSub <- FALSE
		}
	}
	if (!shellScriptSub){
		runCmd <- paste0("'", runCmd, "'")
	}
	qsubCmd <- "qsub"
	args <- c(
		reqTokens, #resource requirements
		"-o", logFile, #log file
		"-e", errFile, #error stream file
		# "-j y", #merge standard error and output stram
		nameToken, #job id
		depToken, #job ids the current job depends on
		"-V", #environment variables exported into jobs
		binSubmitToken, #binary submission or shell script
		runCmd
	)
	subRes <- system2(qsubCmd, args)
	cmd <- paste(qsubCmd, paste(args, collapse=" "), sep=" ")
	res <- list(result=subRes, command=cmd)
	return(res)
}
################################################################################
# Execution of jobs
################################################################################
#' exec-methods
#'
#' Executes a job given the job specifics using system calls
#' @details
#' Instantiates the method \code{\link{exec,CommandR-method}} for \code{\linkS4class{CommandRsge}} objects.
#'
#' @param object   \code{\linkS4class{CommandRsge}} object
#' @param job      \code{\linkS4class{Job}} object
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @param req      Resource requirements to the grid engine when submitting the job. Must be a named character vector.
#' @return \code{\linkS4class{JobResult}} object
#'
#' @rdname exec-CommandRsge-method
#' @docType methods
#' @aliases exec,CommandRsge-method
#' @author Fabian Mueller
#' @export
setMethod("exec",
	signature(
		object="CommandRsge"
	),
	function(
		object,
		job,
		result=FALSE,
		wait=TRUE,
		req=NULL
	) {
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		if (is.null(req)) req <- object@req

		logStruct <- getLoggingStruct(object, job)
		logFile   <- logStruct$logFile
		errFile   <- logStruct$errFile
		jid       <- logStruct$jobId
		scrptDir  <- logStruct$logDir

		sysOut <- character()
		sysErr <- character()
		sysStatus <- 0L

		subRes <- doQsub(job, logFile, errFile, jobName=jid, req=req, shellScriptDir=scrptDir)

		if (wait){
			waitForJobsToTerminate(jid)
		}
		if (result){
			sysOut <- readLines(logFile)
			sysErr <- readLines(errFile)
		}

		res <- JobResult(out=sysOut, err=sysErr, status=sysStatus, command=subRes$command)
		return(res)
	}
)
#-------------------------------------------------------------------------------
#' lexec-methods
#'
#' executes an array of jobs (list execute --> lexec) given the job specifics in the input list of jobs
#' @details
#' Instantiates the method \code{\link{lexec,CommandR-method}} for \code{\linkS4class{CommandRsge}} objects.
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param jobList  a list of \code{\linkS4class{Job}} objects
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @param req      Resource requirements to the grid engine when submitting the job. Must be a named character vector.
#' @return a list of \code{\linkS4class{JobResult}} objects
#'
#' @rdname lexec-CommandRsge-method
#' @docType methods
#' @aliases lexec,CommandRsge-method
#' @author Fabian Mueller
#' @export
setMethod("lexec",
	signature(
		object="CommandRsge"
	),
	function(
		object,
		jobList,
		result=FALSE,
		wait=TRUE,
		req=NULL
	) {
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		if (is.null(req)) req <- object@req

		subJobList <- lapply(jobList, FUN=function(jj){
			logStruct <- getLoggingStruct(object, jj)
			logFile   <- logStruct$logFile
			errFile   <- logStruct$errFile
			jid       <- logStruct$jobId
			scrptDir  <- logStruct$logDir
			subRes <- doQsub(jj, logFile, errFile, jobName=jid, req=req, shellScriptDir=scrptDir)
			rr <- list(
				jobId=jid,
				logFile=logFile,
				errFile=errFile,
				subRes=subRes
			)
			return(rr)
		})
		
		jids <- sapply(subJobList, FUN=function(x){x[["jobId"]]})
		if (wait){
			waitForJobsToTerminate(jids)
		}

		jr.default <- JobResult()
		res <- lapply(subJobList, FUN=function(x){
			sysOut <- character()
			sysErr <- character()
			sysStatus <- 0L
			if (result){
				sysOut <- readLines(x[["logFile"]])
				sysErr <- readLines(x[["errFile"]])
			}
			rr <- JobResult(out=sysOut, err=sysErr, status=sysStatus, command=x[["subRes"]][["command"]])
			return(rr)
		})
		return(res)
	}
)
