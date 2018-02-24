################################################################################
# Job submission class
################################################################################
# General Virtual class template
################################################################################
#' CommandR
#'
#' A virtual class for submitting command line calls
#' 
#' @details
#' For a concrete child class employing system calls see \code{\linkS4class{CommandRsystem}}.
#' For a concrete child class employing the sun grid engine calls see \code{\linkS4class{CommandRsge}}.
#' If you want to implement your own child class be sure to at least implement the following functions:
#' \code{\link{exec,CommandR-method}}.
#'
#' @section Slots:
#' \describe{
#'   \item{\code{logDir}}{A directory where log and error files are written to}
#'   \item{\code{doRun}}{Flag indicating whether the analysis should actually run.}
#'   \item{\code{handlesJobDeps}}{Flag indicating whether the class object can
#' 		actually handle job dependencies on its own
#'   }
#' }
#'
#' @section Methods:
#' \describe{
#'   \item{\code{\link{getLogDir,CommandR-method}}}{Get the logging directory.}
#'   \item{\code{\link{exec,CommandR-method}}}{Execute a command/job.}
#'   \item{\code{\link{lexec,CommandR-method}}}{Execute an array of commands/jobs.}
#' }
#'
#' @name CommandR-class
#' @rdname CommandR-class
#' @author Fabian Mueller
#' @exportClass CommandR
setClass("CommandR",
	slots = list(
		logDir = "characterOrNULL",
		doRun  = "logical",
		handlesJobDeps  = "logical"
	),
	contains = "VIRTUAL",
	package = "muPipeR"
)

#' initialize.CommandR
#'
#' Initialize a CommandR object
#' 
#' @param .Object New instance of \code{CommandR}.
#'
#' @author Fabian Mueller
#' @docType methods
setMethod("initialize","CommandR",
	function(
		.Object
	) {
		.Object@logDir <- NULL
		.Object@doRun  <- TRUE
		.Object@handlesJobDeps <- FALSE
		.Object
	}
)
################################################################################
# Getters
################################################################################
if (!isGeneric("getLogDir")) {
	setGeneric(
		"getLogDir",
		function(object) standardGeneric("getLogDir"),
		signature=c("object")
	)
}
#' getLogDir-methods
#'
#' Return the logging directory of an object
#'
#' @param object \code{\linkS4class{CommandR}} object
#' @return Character specifying the logging directory of the object.
#'
#' @rdname getLogDir-CommandR-method
#' @docType methods
#' @aliases getLogDir
#' @aliases getLogDir,CommandR-method
#' @author Fabian Mueller
#' @export
setMethod("getLogDir",
	signature(
		object="CommandR"
	),
	function(
		object
	) {
		return(object@logDir)
	}
)
################################################################################
# Helpers
################################################################################
if (!isGeneric("getLoggingStruct")) {
	setGeneric(
		"getLoggingStruct",
		function(object, job) standardGeneric("getLoggingStruct"),
		signature=c("object","job")
	)
}
#' getLoggingStruct-methods
#'
#' Helper function for getting the logging structure of a CommandR object and a Job object
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param job      \code{\linkS4class{Job}} object
#' @return a list with the following entries:
#' \describe{
#'   \item{\code{logDir}}{logging directory from \code{object}. If this is NULL, \code{tempdir()} is returned}
#'   \item{\code{logFile}}{a log file in the logging directory based on the job id}
#'   \item{\code{errFile}}{an error log file in the logging directory based on the job id} 
#'   \item{\code{jobId}}{id of the job. If this is NULL, a string is returned, that is unique in the logfile.
#'         since this is a random string that contains the current date this should be nearly always unique across calls.} 
#' }
#'
#' @rdname getLoggingStruct-CommandR-method
#' @docType methods
#' @aliases getLoggingStruct
#' @aliases getLoggingStruct,CommandR-method
#' @author Fabian Mueller
#' @noRd
setMethod("getLoggingStruct",
	signature(
		object="CommandR"
	),
	function(
		object,
		job
	) {
		logDir <- getLogDir(object)
		if (is.null(logDir)){
			logDir <- tempdir()
		}
		jid <- getId(job)
		if (is.null(jid)){
			jid <- getHashString("j", useDate=TRUE)
		}
		logFile <- file.path(logDir, paste0(jid, ".log"))
		errFile <- file.path(logDir, paste0(jid, ".log.err"))
		res <- list(
			logDir=logDir,
			logFile=logFile,
			errFile=errFile,
			jobId=jid
		)
		class(res) <- "LoggingStruct"
		return(res)
	}
)
################################################################################
# Execution of jobs
################################################################################
#implement for each inheriting class
if (!isGeneric("exec")) {
	setGeneric(
		"exec",
		function(object, job, result=FALSE, wait=TRUE, ...) standardGeneric("exec"),
		signature=c("object","job", "result", "wait")
	)
}
#' exec-methods
#'
#' Executes a job given the job specifics in the job
#' @details
#' For a concrete child class implementations see \code{\link{exec,CommandRsystem-method}} and \code{\link{exec,CommandRsge-method}}
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param job      \code{\linkS4class{Job}} object
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @return \code{\linkS4class{JobResult}} object
#'
#' @rdname exec-CommandR-method
#' @docType methods
#' @aliases exec
#' @aliases exec,CommandR-method
#' @author Fabian Mueller
#' @export
setMethod("exec",
	signature(
		object="CommandR"
	),
	function(
		object,
		job,
		result=FALSE,
		wait=TRUE
	) {
		if (result && !wait) logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
		return(JobResult())
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("lexec")) {
	setGeneric(
		"lexec",
		function(object, jobList, result=FALSE, wait=TRUE, ...) standardGeneric("lexec"),
		signature=c("object","jobList", "result", "wait")
	)
}
#' lexec-methods
#'
#' executes an array of jobs (list execute --> lexec) given the job specifics in the input list of jobs
#' @details
#' For a concrete child class implementation for a sun grid architecture specification see \code{\link{lexec,CommandRsystem-method}}
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param jobList  a list of \code{\linkS4class{Job}} objects
#' @param result   Flag indicating whether the result should be returned
#' @param wait     Flag indicating whether the session should wait for the job to finish before continuing. Will be set to \code{TRUE}
#'                 if \code{result} is \code{TRUE}
#' @return a list of \code{\linkS4class{JobResult}} objects
#'
#' @rdname lexec-CommandR-methods
#' @docType methods
#' @aliases lexec
#' @aliases lexec,CommandR-method
#' @author Fabian Mueller
#' @export
setMethod("lexec",
	signature(
		object="CommandR"
	),
	function(
		object,
		jobList,
		result=FALSE,
		wait=TRUE
	) {
		if (result && !wait){
			logger.warning("wait cannot be FALSE when result is TRUE. --> setting wait<-TRUE")
			wait <- TRUE
		}
		res <- lapply(jobList, FUN=function(jj){
			exec(object, jj, wait=wait, result=result)
		})
		return(res)
	}
)
#-------------------------------------------------------------------------------
# TODO: rethink this:
# instead of executing just one set of R commands do something like lapply for
# R scripts
if (!isGeneric("lapplyExec")) {
	setGeneric(
		"lapplyExec",
		function(object, X, ...) standardGeneric("lapplyExec"),
		signature=c("object")
	)
}
#' lapplyExec-methods
#'
#' lapply for \code{\linkS4class{CommandR}} objects using \code{\link{lexec,CommandR-method}}
#'
#' @param object   \code{\linkS4class{CommandR}} object
#' @param X		   the object to iterate over. Currently only \code{list} objects are supported
#' @param FUN      the R function to be run
#' @param env      R environment or list storing variables that will be exported and might be used in the function call
#' @param loadPackages character vector of packages to load before executing the function
#' @param Rexec    the command that is used to run the R script that is generated
#' @param name     a name for the execution that will be used as identifier and prefix for the jobs that are run
#' @param ...      optional arguments to \code{FUN}
#' @param cleanUp  should the directory structure created for running the jobs be deleted when completed
#' 
#' @return a list containing the results of \code{FUN} for each element in \code{X}
#' 
#' @details
#' It will create an R script for the commands
#'
#' @examples
#' \donttest{
#' ll <- lapply(1:20,  identity)
#' cmdr <- CommandRsystem("partest")
#' rr <- lapplyExec(cmdr, ll, function(i, b){Sys.sleep(1); print(a); print(b); return(paste("success on job", i, "- status:", b))}, env=list(a="success"), cleanUp=FALSE, b="superduper")
#' }
#' @rdname lapplyExec-CommandR-method
#' @docType methods
#' @aliases lapplyExec
#' @aliases lapplyExec,CommandR-method
#' @author Fabian Mueller
#' @export
setMethod("lapplyExec",
	signature(
		object="CommandR"
	),
	function(
		object,
		X,
		FUN,
		env=new.env(parent=emptyenv()),
		loadPackages=.packages(),
		Rexec="Rscript",
		name="run",
		cleanUp=TRUE,
		...
	) {
		if (is.list(env)){
			if (length(names(env)) != length(env)) logger.error("if env is a list, it must have names")
			env <- list2env(env, parent=emptyenv())
		}
		logger.status("Preparing infrastructure...")
		lDir <- getLogDir(object)
		if (is.null(lDir)){
			lDir <- tempdir()
		}
		if (!dir.exists(lDir)) dir.create(lDir)
		eid <- getHashString(name, useDate=TRUE)
		baseDir <- file.path(lDir, eid)
		if (!dir.exists(baseDir)){
			logger.info(c("running R command using files in directory:", baseDir))
			dir.create(baseDir)
		} else {
			logger.error(c("directory", baseDir, "already exists"))
		}
		object@logDir <- file.path(baseDir, "log") #set the logging directory to a subdirectory
		dir.create(object@logDir)
		dataDir <- file.path(baseDir, "data")
		dir.create(dataDir)
		outDir <- file.path(baseDir, "output")
		dir.create(outDir)

		# Work in progress
		fFn <- file.path(dataDir, "fun.rds")
		saveRDS(FUN, fFn)
		dFn <- file.path(dataDir, "dotArgs.rds")
		dotArgs <- list(...)
		saveRDS(dotArgs, dFn)
		for (i in seq_along(X)){
			saveRDS(X[[i]], file.path(dataDir, paste0("x", i, ".rds")))
		}

		rdFn <- file.path(dataDir, "envir.RData")
		save(list=ls(envir=env), file=rdFn, envir=env)

		scrptFn <- file.path(baseDir, "run.R")
		loadPackages <- c(loadPackages, "argparse")
		scrptLines <- c(
			paste0("library(", loadPackages, ")"),
			"",
			"ap <- ArgumentParser()",
			"ap$add_argument('-x', action='store', dest='xFile', help='an RDS file storing the object to call the function on')",
			"ap$add_argument('-f', '--fun', action='store', dest='fFile', help='an RDS file storing the function to run')",
			"ap$add_argument('-d', '--dots', action='store', dest='dFile', help='an RDS file storing a list of dot arguments (...) to function')",
			"ap$add_argument('-e', '--envir', action='store', dest='envirFile', help='file containing saved R variables')",
			"ap$add_argument('-o', '--out', action='store', dest='outFile', help='filename for the output (RDS file)')",
			"cmdArgs <- ap$parse_args()",
			"",
			"load(cmdArgs$envirFile)",
			"x <- readRDS(cmdArgs$xFile)",
			"dotList <- readRDS(cmdArgs$dFile)",
			".f <- readRDS(cmdArgs$fFile)",
			"",
			"result <- do.call('.f', c(list(x), dotList))",
			"saveRDS(result, cmdArgs$outFile)"
		)
		writeLines(scrptLines, scrptFn)
		
		jobList <- list()
		for (i in seq_along(X)){
			jid <- paste0(eid, "_j", i)
			args <- c(
				scrptFn,
				paste("-x", file.path(dataDir, paste0("x", i, ".rds"))),
				paste("-f", fFn),
				paste("-d", dFn),
				paste("-e", rdFn),
				paste("-o", file.path(outDir, paste0("o", i, ".rds")))
			)
			jj <- Job(Rexec, args=args, id=jid)
			jobList <- c(jobList, list(jj))
		}

		logger.status("Running command...")
		execRes <- lexec(object, jobList)

		logger.status("Collecting output...")
		res <- lapply(seq_along(X), FUN=function(i){
			readRDS(file.path(outDir, paste0("o", i, ".rds")))
		})

		if (cleanUp) {
			unlink(baseDir, recursive=TRUE)
		}
		return(res)
	}
)
# library(devtools)
# load_all("muResearchCode/muPipeR")
# ll <- lapply(1:20,  identity)
# cmdr <- CommandRsystem("partest")
# rr <- lapplyExec(cmdr, ll, function(i, b){Sys.sleep(1); print(a); print(b); return(paste("success on job", i, "- status:", b))}, env=list(a="success"), cleanUp=FALSE, b="superduper")

