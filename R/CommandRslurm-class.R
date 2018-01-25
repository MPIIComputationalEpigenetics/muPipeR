#' CommandRslurm Class
#'
#' A child class of \code{\linkS4class{CommandR}} implementing job submission using slurm job scheduling.
#'
#' @details
#' Follow this template if you want to create your own \code{\linkS4class{CommandR}} class.
#'
#' @section Slots:
#' Includes all the slots of the \code{\linkS4class{CommandR}} class. Additionally includes:
#' \describe{
#'   \item{\code{req}}{Default resource requirements for submitting jobs to the slurm engine.}
#' }
#'
#' @section Methods:
#' implements all the methods of the \code{\linkS4class{CommandR}} class.
#'
#' @name CommandRslurm-class
#' @rdname CommandRslurm-class
#' @author Fabian Mueller
#' @exportClass CommandRslurm
setClass("CommandRslurm",
	slots = list(
		req   = "characterOrNULL"
	),
	contains="CommandR",
	package = "muPipeR"
)
setMethod("initialize","CommandRslurm",
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
#' @name CommandRslurm
#' @rdname CommandRslurm-class
#' @aliases intialize,CommandRslurm-method
#' @export
CommandRslurm <- function(logDir=NULL, req=NULL){
	obj <- new("CommandRslurm",
		logDir, req
	)
	return(obj)
}
################################################################################
# Helpers
################################################################################
#' getSlurmJobStatus
#' 
#' Query the slurm engine using retrieve the status (known or unknown) 
#' of the job for a given job id
#' @param jids   job id(s)
#' @return a character string: "known" if the job is still known to the grid engine
#'         (running or scheduled). "unknown" if not.
#' @author Fabian Mueller
#' @noRd
getSlurmJobStatus <- function(jids){
	tmpFn <- tempfile(pattern="squeue", fileext=".tsv")
	stateStr <- system2("squeue", c("-o", paste0('"', paste(c("%j", "%i", "%T"), collapse="\t"), '"')), stdout=tmpFn)
	stateTab <- read.table(tmpFn, header=TRUE, sep="\t", stringsAsFactors=FALSE)
	unlink(tmpFn)

	stateTab <- stateTab[stateTab[,"STATE"] %in% c("PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "SUSPENDED"),]
	res <- ifelse(jids %in% stateTab[,"NAME"], "active", "inactive")
	return(res)
}
#' waitForSlurmJobsToTerminate
#' 
#' Wait for the given jobs to terminate
#' @details
#' repeatedly queries the slurm engine to see whether the jobs are still "known".
#' Initially checks whether it can find all jobs on the grid engine
#' @param jids   character vector of job ids
#' @return nothing of particular interest
#' @author Fabian Mueller
#' @noRd
waitForSlurmJobsToTerminate <- function(jids){
	lag <- .config$waitLag
	initialize.lag  <- 3
	initialize.iter <- 20

	# initial check if all jobs are registered
	jids.invalid <- jids
	while (initialize.iter > 0 && length(jids.invalid) > 0){
		Sys.sleep(initialize.lag)
		jobStatus <- getSlurmJobStatus(jids.invalid)
		jids.valid <- jids.invalid[jobStatus %in% c("active")]
		jids.invalid <- setdiff(jids.invalid, jids.valid)
		initialize.iter <- initialize.iter - 1
	}
	if (length(jids.invalid) > 0){
		logger.error(c("Could not retrieve status for the following jobs (already finished?):", paste(jids.invalid, collapse=",")))
	}
	logger.status("Waiting for jobs to complete...")
	# wait for jobs to complete:
	# check status of jobs until they become inactive in the slurm engine
	jids.incomplete <- jids
	while (length(jids.incomplete) > 0){
		Sys.sleep(lag)
		jobStatus <- sapply(jids.incomplete, getSgeJobStatus)
		jids.done <- jids.incomplete[jobStatus %in% c("inactive")]
		jids.incomplete <- setdiff(jids.incomplete, jids.done)
	}
}
#-------------------------------------------------------------------------------
#' doSbatch
#' 
#' Construct a batch submission string for a given \code{\linkS4class{Job}} object
#' and submit it using sbatch and a system call.
#' @details
#' If the length of the command string of the job exceeds 1024 characters, it will be be
#' automatically be written to a shell script in the \code{shellScriptDir} directory
#' and the submitted job will execute this script file instead of the command directly.
#' @param job     \code{\linkS4class{Job}} object
#' @param logFile a log file where the output of job should be written to
#' @param errFile a error file where the error stream of the job should be written to
#' @param jobName a name for the submitted job
#' @param req     resource requirements for the submitted job. Must be a named character vector.
#' @param batchScriptDir directory where the batch script containing the command will be written to
#' @return the result of the system call using \link{system2}
#' @author Fabian Mueller
#' @noRd
doSbatch <- function(job, logFile, errFile, jobName=getId(job), req=NULL, batchScriptDir=NULL){
	reqStrs <- NULL
	if (length(req) > 0) {
		if (any(is.na(names(req))) || is.null(names(req))){
			logger.error("Invalid resource requirement specification. Need names for all requirements")
		}
		reqStrs <- paste0(names(req),"=",req))
	}

	depJobIds <- do.call("c", lapply(getDepJobs(job), getId))
	depStr <- NULL
	if (length(depJobIds)>0){
		depStr <- paste("--dependency", paste0("afterany:", paste(depJobIds, collapse=":")), collapse="=")
	}
	nameStr <- NULL
	if (!is.null(jobName)) nameStr <- paste("--job-name", jobName, collapse="=")

	reqLines <- c(nameStr, depStr, reqStrs)
	reqLines <- paste("#SBATCH", reqLines)

	runCmd <- paste("srun", getCallString(job))
	scrptFn <- NULL
	if (is.null(batchScriptDir) || !dir.exists(batchScriptDir)){
		logger.warning(c("No directory for batch script submission has been specified or it does not exist."))
	} else {
		if (!is.null(jobName)) {
			scrptFn <- file.path(batchScriptDir, paste0(jobName,".sh"))
		} else {
			scrptFn <- tempfile(pattern="scrpt", tmpdir=batchScriptDir, fileext=".sh")
		}
		scrptLines <- c(
			"#!/bin/bash",
			reqLines,
			"\n",
			runCmd
		)
		fileConn <- file(scrptFn)
		writeLines(, fileConn)
		close(fileConn)
		Sys.chmod(scrptFn, mode = "0755")
		runCmd <- scrptFn
	}
	}
	qsubCmd <- "sbatch"
	args <- c(
		scrptFn
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
#' Instantiates the method \code{\link{exec,CommandR-method}} for \code{\linkS4class{CommandRslurm}} objects.
#'
#' @param object   \code{\linkS4class{CommandRslurm}} object
#' @param job      \code{\linkS4class{Job}} object
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @param req      Resource requirements to the slurm engine when submitting the job. Must be a named character vector.
#' @return \code{\linkS4class{JobResult}} object
#'
#' @rdname exec-CommandRslurm-method
#' @docType methods
#' @aliases exec,CommandRslurm-method
#' @author Fabian Mueller
#' @export
setMethod("exec",
	signature(
		object="CommandRslurm"
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

		subRes <- doSbatch(job, logFile, errFile, jobName=jid, req=req, batchScriptDir=scrptDir)

		if (wait){
			waitForSlurmJobsToTerminate(jid)
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
#' Instantiates the method \code{\link{lexec,CommandR-method}} for \code{\linkS4class{CommandRslurm}} objects.
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param jobList  a list of \code{\linkS4class{Job}} objects
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @param req      Resource requirements to the grid engine when submitting the job. Must be a named character vector.
#' @return a list of \code{\linkS4class{JobResult}} objects
#'
#' @rdname lexec-CommandRslurm-method
#' @docType methods
#' @aliases lexec,CommandRslurm-method
#' @author Fabian Mueller
#' @export
setMethod("lexec",
	signature(
		object="CommandRslurm"
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
			subRes <- doSbatch(jj, logFile, errFile, jobName=jid, req=req, batchScriptDir=scrptDir)
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
			waitForSlurmJobsToTerminate(jids)
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