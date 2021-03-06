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
		req   = "characterOrNULL",
		scriptDir   = "characterOrNULL",
		user = "character"
	),
	contains="CommandR",
	package = "muPipeR"
)
setMethod("initialize","CommandRslurm",
	function(
		.Object,
		logDir=NULL,
		scriptDir=NULL,
		req=NULL,
		user=""
	) {
		if (is.null(logDir)){
			logDir <- tempdir()
			logger.warning(c("Did not specify directory for logging --> using directory:", logDir))
		}
		if (is.null(scriptDir)){
			scriptDir <- tempdir()
			logger.warning(c("Did not specify directory for scripts. --> using directory:", scriptDir))
		}
		if (!is.null(req)){
			if (any(is.na(names(req))) || is.null(names(req))){
				logger.error("Invalid resource requirement specification. Need names for all requirements")
			}
		}
		.Object <- callNextMethod(.Object)
		.Object@logDir <- logDir
		.Object@scriptDir <- scriptDir
		.Object@handlesJobDeps <- TRUE
		.Object@req <- req
		.Object@user <- user
		.Object
	}
)
#' @param logDir log directory
#' @param scriptDir script directory
#' @param req default resource requirements. Will be specified for each submitted job, if no
#'            other requirements are specified. Must be a named character vector.
#' @param user   optional user name for the slurm cluster (for performance improvements in querying the cluster)
#' 
#' @name CommandRslurm
#' @rdname CommandRslurm-class
#' @aliases intialize,CommandRslurm-method
#' @export
CommandRslurm <- function(logDir=NULL, scriptDir=NULL, req=NULL, user=""){
	obj <- new("CommandRslurm",
		logDir, scriptDir, req, user
	)
	return(obj)
}
################################################################################
# Helpers
################################################################################
#' getSlurmJobStatusTab
#' 
#' Query the slurm engine using retrieve a table containing job states for the given
#' job ids
#' @param jids   job id(s)
#' @param user   optional user name for the slurm cluster (for performance improvements in retrieving the job status table)
#' @return a table containing job names, slurm IDs, and states
#' @author Fabian Mueller
#' @noRd
getSlurmJobStatusTab <- function(jids, user=""){
	tmpFn <- tempfile(pattern="squeue", fileext=".tsv")
	stateTab <- NULL
	fail <- TRUE
	squeueArgs <- c("-o", paste0('"', paste(c("%j", "%i", "%T", "%R"), collapse="\t"), '"'))
	if (nchar(user) > 0) squeueArgs <- c("-u", user, squeueArgs)
	while (fail){
		stateStr <- system2("squeue", squeueArgs, stdout=tmpFn, stderr=tmpFn)
		# catch "busy cluster" errors
		stateTab <- tryCatch({
				read.table(tmpFn, header=TRUE, sep="\t", comment.char="", stringsAsFactors=FALSE)
			}, error = function(ee) {
				ss <- readLines(tmpFn)
				clusterBusy <- any(grepl("slurm_load_jobs error: Socket timed out", ss))
				if (clusterBusy){
					return(NULL)
				} else {
					logger.error(c("Could not get SLURM job status table because:", ee$message))
				}
			}
		)
		unlink(tmpFn)
		fail <- is.null(stateTab) || !is.data.frame(stateTab) || !all(c("NAME", "JOBID", "STATE", "NODELIST.REASON.") %in% colnames(stateTab))
		if (fail){
			logger.info(c("Cluster is busy. Retrying to get job status table ..."))
			Sys.sleep(5)
		}
	}
	colnames(stateTab)[colnames(stateTab)=="NODELIST.REASON."] <- "REASON"

	stateTab <- stateTab[stateTab[,"NAME"] %in% jids,,drop=FALSE]
	return(stateTab)
}
#' getSlurmJobStatus
#' 
#' Query the slurm engine using retrieve the status (known or unknown) 
#' of the job for a given job id
#' @param jids   job id(s)
#' @param user   optional user name for the slurm cluster (for performance improvements in retrieving the job status table)
#' @return a character string: "known" if the job is still known to the grid engine
#'         (running or scheduled). "unknown" if not.
#' @author Fabian Mueller
#' @noRd
getSlurmJobStatus <- function(jids, user=""){
	stateTab <- getSlurmJobStatusTab(jids, user=user)

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
#' @param initRelease should held up jobs be released after the initial check?
#' @param user   optional user name for the slurm cluster (for performance improvements in retrieving the job status table)
#' @return nothing of particular interest
#' @author Fabian Mueller
#' @noRd
waitForSlurmJobsToTerminate <- function(jids, initRelease=FALSE, user=""){
	lag <- .config$waitLag
	initialize.lag  <- 10
	initialize.iter <- 20

	# initial check if all jobs are registered
	jids.invalid <- jids
	while (initialize.iter > 0 && length(jids.invalid) > 0){
		Sys.sleep(initialize.lag)
		jobStatus <- getSlurmJobStatus(jids.invalid, user=user)
		jids.valid <- jids.invalid[jobStatus %in% c("active")]
		jids.invalid <- setdiff(jids.invalid, jids.valid)
		initialize.iter <- initialize.iter - 1
	}
	if (length(jids.invalid) > 0){
		logger.error(c("Could not retrieve status for the following jobs (already finished?):", paste(jids.invalid, collapse=",")))
	}
	if (initRelease){
		logger.status("Releasing held jobs...")
		statTab <- getSlurmJobStatusTab(jids, user=user)
		statTab <- statTab[grepl("JobHeld", statTab[,"REASON"]),,drop=FALSE]
		# for some reason only half the jobs get released in one go (strangely every other job) --> iterate until all jobs have been released
		while (nrow(statTab) > 0){
			heldJobNames <- statTab[,"NAME"]
			heldJobIds <- statTab[,"JOBID"]
			# split the arguments to the release command, so that the string does not get too long
			chunks <- split(heldJobIds, ceiling(1:length(heldJobIds)/20))
			relCmd <- "scontrol"
			relRes <- lapply(chunks, FUN=function(x){
				args <- c("release", paste0('"', paste(x, collapse=","), '"'))
				return(system2(relCmd, args))
			})
			# check which jobs have not been released
			Sys.sleep(2)
			statTab <- getSlurmJobStatusTab(heldJobNames, user=user)
			# print(statTab)
			statTab <- statTab[grepl("JobHeld", statTab[,"REASON"]),,drop=FALSE]
		}
	}
	logger.status("Waiting for jobs to complete...")
	# wait for jobs to complete:
	# check status of jobs until they become inactive in the slurm engine
	jids.incomplete <- jids
	while (length(jids.incomplete) > 0){
		Sys.sleep(lag)
		jobStatus <- getSlurmJobStatus(jids.incomplete, user=user)
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
#' @param hold    should the job be submitted in a held state?
#' @param array   Only if submitting array jobs: integer vector specifying indices for the submitted jobs
#' @return the result of the system call using \link{system2}
#' @author Fabian Mueller
#' @noRd
doSbatch <- function(job, logFile, errFile, jobName=getId(job), req=NULL, batchScriptDir=NULL, hold=FALSE, user="", array=NULL){
	reqStrs <- NULL
	if (length(req) > 0) {
		if (any(is.na(names(req))) || is.null(names(req))){
			logger.error("Invalid resource requirement specification. Need names for all requirements")
		}
		reqStrs <- paste0(names(req),"=",req)
	}

	depJobIds <- do.call("c", lapply(getDepJobs(job), getId))
	depStr <- NULL
	if (length(depJobIds)>0){
		jTab <- getSlurmJobStatusTab(depJobIds, user=user)
		slurmIdStr <- paste(unique(jTab[,"JOBID"]), collapse=":")
		depStr <- paste(c("--dependency", paste0("afterany:", slurmIdStr)), collapse="=")
	}
	nameStr <- NULL
	if (!is.null(jobName)) nameStr <- paste(c("--job-name", jobName), collapse="=")

	outFnStr <- paste(c("--output", logFile), collapse="=")
	errFnStr <- paste(c("--error", errFile), collapse="=")

	reqLines <- c(nameStr, outFnStr, errFnStr, depStr, reqStrs)
	reqLines <- paste("#SBATCH", reqLines)

	runCmd <- getCallString(job)
	# if (is.null(array)){
	# 	runCmd <- paste("srun", runCmd)
	# }
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
			runCmd
		)
		fileConn <- file(scrptFn)
		writeLines(scrptLines, fileConn)
		close(fileConn)
		Sys.chmod(scrptFn, mode = "0755")
		runCmd <- scrptFn
	}
	subCmd <- "sbatch"
	args <- c(
		scrptFn
	)
	if (!is.null(array)){
		if (all(array == array[1]:array[length(array)])) {
			aidxStr <- paste0(array[1], "-", array[length(array)])
		} else {
			aidxStr <- paste(array, collapse=",")
		}
		args <- c(paste0("--array=", aidxStr), args)
	}
	if (hold){
		args <- c("--hold", args)
	}
	subRes <- system2(subCmd, args)
	cmd <- paste(subCmd, paste(args, collapse=" "), sep=" ")
	# print(cmd)
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
		scrptDir  <- object@scriptDir

		sysOut <- character()
		sysErr <- character()
		sysStatus <- 0L

		subRes <- doSbatch(job, logFile, errFile, jobName=jid, req=req, batchScriptDir=scrptDir, hold=wait, user=object@user)

		if (wait){
			waitForSlurmJobsToTerminate(jid, initRelease=TRUE, user=object@user)
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
#' @param array    Submit array jobs: integer vector specifying indices for the submitted jobs
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
		req=NULL,
		array=NULL
	) {
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		if (is.null(req)) req <- object@req

		arrayJob <- !is.null(array)

		if (length(jobList) == 1 && arrayJob && !is.list(array)){
			array <- list(array)
		}

		scrptDir  <- object@scriptDir

		if (arrayJob){
			subJobList <- list()
			jids <- c()
			for (k in seq_along(jobList)){
				logStruct <- getLoggingStruct(object, jobList[[k]])
				logFile   <- logStruct$logFile
				logFile.base <- gsub("\\.log$", "_", logFile)
				logFile <- paste0(logFile.base, "%a.log")
				errFile <- paste0(logFile.base, "%a.log.err")

				jid_batch <- logStruct$jobId
				subRes <- doSbatch(jobList[[k]], logFile, errFile, jobName=jid_batch, req=req, batchScriptDir=scrptDir, hold=wait, user=object@user, array=array[[k]])

				subJobList <- c(subJobList, lapply(array, FUN=function(i){
					list(
						jobId=jid_batch,
						logFile=paste0(logFile.base, i, ".log"),
						errFile=paste0(logFile.base, i, ".log.err"),
						subRes=subRes
					)
				}))
				jids <- c(jids, jid_batch)
			}

		} else {
			subJobList <- lapply(jobList, FUN=function(jj){
				logStruct <- getLoggingStruct(object, jj)
				logFile   <- logStruct$logFile
				errFile   <- logStruct$errFile
				jid       <- logStruct$jobId
				subRes <- doSbatch(jj, logFile, errFile, jobName=jid, req=req, batchScriptDir=scrptDir, hold=wait, user=object@user)
				rr <- list(
					jobId=jid,
					logFile=logFile,
					errFile=errFile,
					subRes=subRes
				)
				return(rr)
			})
			
			jids <- sapply(subJobList, FUN=function(x){x[["jobId"]]})
		}
		
		if (wait){
			waitForSlurmJobsToTerminate(jids, initRelease=TRUE, user=object@user)
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
