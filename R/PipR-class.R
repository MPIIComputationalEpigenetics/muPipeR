#' PipR
#'
#' A class for managing analysis pipelines
#' 
#' @details
#' When constructing \code{PipR} objects, a base directory will be created that
#' contains subdirectories for the pipeline status, log files, analysis results
#' and temporary files.
#' Internally a graph representation of the pipeline is used for handling
#' dependencies between jobs.
#' See the package vignette for examples and more details.
#'
#' @section Slots:
#' \describe{
#'   \item{\code{baseDir}}{
#'		Base directory for the analysis pipeline
#'   }
#'   \item{\code{logDir}}{
#'		Directory containing log files orginating from executing the analysis pipeline
#'   }
#'   \item{\code{srcDir}}{
#'		Directory containing source files for executing the analysis pipeline
#'   }
#'   \item{\code{resultDir}}{
#'		Directory containing results orginating from executing the analysis pipeline.
#'      This directory will contain subdirectories for each analysis step
#'   }
#'   \item{\code{statusDir}}{
#'		Directory where status information on the execution of the pipeline will
#'      be stored. This includes the \code{PipR} object itself and a graphical overview
#'   }
#'   \item{\code{tempDir}}{
#'		Directory containing temporary files orginating from executing the analysis pipeline
#'   }
#'   \item{\code{idHash}}{
#'		A (most likely) unique hash string identifying the anlysis pipeline
#'   }
#'   \item{\code{steps}}{
#'		A list containing details on the steps registered for the analysis pipeline.
#'      This list contains list with elements corresponding to job details, i.e.
#'      job name, status, result directory and associated \code{\linkS4class{Job}}
#'      and \code{\linkS4class{JobResult}} objects.
#'   }
#'   \item{\code{graph}}{
#'		\code{igraph} graph representation storing all steps in the pipeline as nodes
#'      and step dependencies as edges.
#'   }
#' }
#'
#' @section Methods:
#' \describe{
#'    \item{\code{\link{getDir,PipR-method}}}{
#'      Getter for the base and subdirectories
#'    }
#'    \item{\code{\link{addStep,PipR-method}}}{
#'      Add an analysis step to the pipeline and register it for being executed.
#'    }
#'    \item{\code{\link{getSteps,PipR-method}}}{
#'      Get the names of analysis steps in the pipeline.
#'    }
#'    \item{\code{\link{run,PipR-method}}}{
#'      Run the analysis pipeline executing all registered, incomplete steps.
#'    }
#'    \item{\code{\link{getGraph,PipR-method}}}{
#'      Get a graph representation storing all steps in the pipeline as nodes
#'      and step dependencies as edges.
#'    }
#'    \item{\code{\link{getGraph,PipR-method}}}{
#'      Plot the graph representation of the pipeline.
#'    }
#'    \item{\code{\link{initializePipeDir,PipR-method}}}{
#'      Create the pipeline directory and it's subdirectories.
#'    }
#'    \item{\code{\link{changePipeDir,PipR-method}}}{
#'      Change the pipeline directory.
#'    }
#'    \item{\code{\link{cleanup,PipR-method}}}{
#'      Remove temproary and uninformative files.
#'    }
#'    \item{\code{\link{resetStep,PipR-method}}}{
#'      Undo a pipeline step and its dependend steps resetting its results.
#'    }
#' }
#'
#' @name PipR-class
#' @rdname PipR-class
#' @author Fabian Mueller
#' @exportClass PipR
setClass("PipR",
	slots = list(
		baseDir   = "character",
		logDir    = "characterOrNULL",
		srcDir    = "characterOrNULL",
		statusDir = "characterOrNULL",
		resultDir = "characterOrNULL",
		tempDir   = "characterOrNULL",
		idHash    = "character",
		steps     = "list",
		graph     = "igraph"
	),
	package = "muPipeR"
)

setMethod("initialize","PipR",
	function(
		.Object,
		baseDir
	) {
		require(igraph)
		.Object@baseDir   <- baseDir
		.Object@logDir    <- NULL
		.Object@srcDir    <- NULL
		.Object@statusDir <- NULL
		.Object@resultDir <- NULL
		.Object@tempDir   <- NULL
		.Object@idHash    <- getHashString("p", useDate=TRUE)
		.Object@steps     <- list()
		.Object@graph     <- graph.empty(directed=TRUE)
		.Object <- initializePipeDir(.Object)
		.Object
	}
)
#' @param baseDir base directory for the pipeline
#' @name PipR
#' @rdname PipR-class
#' @aliases intialize,PipR-method
#' @export
PipR <- function(baseDir){
	obj <- new("PipR",
		baseDir
	)
	return(obj)
}
################################################################################
# Getters
################################################################################
if (!isGeneric("getDir")) {
	setGeneric(
		"getDir",
		function(object, ...) standardGeneric("getDir"),
		signature=c("object")
	)
}
#' getDir-methods
#'
#' Return the pipeline base directory or one of it's subdirectories
#'
#' @param object \code{\linkS4class{PipR}} object
#' @param type   Optionally specify the subdirectory. Possible values are
#'               \code{"base", "log", "status", "result", "temp"}
#' @return Character string specifying the path of the directory.
#'
#' @rdname getDir-PipR-method
#' @docType methods
#' @aliases getDir
#' @aliases getDir,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("getDir",
	signature(
		object="PipR"
	),
	function(
		object,
		type="base"
	) {
		res <- NULL
		if (type == "base"){
			res <- object@baseDir
		} else if (type == "log"){
			res <- object@logDir
		} else if (type == "src"){
			res <- object@srcDir
		} else if (type == "status"){
			res <- object@statusDir
		} else if (type == "result"){
			res <- object@resultDir
		} else if (type == "temp"){
			res <- object@tempDir
		} else {
			logger.error("Unknown pipeline directory type")
		}
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getSteps")) {
	setGeneric(
		"getSteps",
		function(object, ...) standardGeneric("getSteps"),
		signature=c("object")
	)
}
#' getSteps-methods
#'
#' Return the names of pipeline steps
#'
#' @param object \code{\linkS4class{PipR}} object
#' @param status Optionally only include complete or incomplete steps in the output.
#'               Possible values are \code{"all", "complete", "incomplete"}.
#' @param graphOrder Logical indicating whether the steps should be returned in
#'               graph order, i.e. in the order of a DFS in the pipeline graph.
#' @return Character vector of pipeline step names.
#'
#' @rdname getSteps-PipR-method
#' @docType methods
#' @aliases getSteps
#' @aliases getSteps,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("getSteps",
	signature(
		object="PipR"
	),
	function(
		object,
		status="all",
		graphOrder=FALSE
	) {
		res <- names(object@steps)
		if (graphOrder){
			g <- object@graph
			allSteps <- V(g)$name
			res <- c()
			hasUnvisitedSteps <- length(setdiff(allSteps, res)) > 0
			while (hasUnvisitedSteps){
			#find the sources in the pipeline graph
				isSourceNode <- sapply(V(g)$name, FUN=function(v){
					length(neighbors(g, v, mode="in")) == 0
				})
				sourceNodes <- V(g)$name[isSourceNode]
				if (length(sourceNodes) < 1){
					logger.error("No sources found in pipeline graph. Is the graph really a DAG?")
				}
				res <- c(res, sourceNodes)
				g <- delete_vertices(g, sourceNodes)
				hasUnvisitedSteps <- length(setdiff(allSteps, res)) > 0
			}
		}
		stepStates <- vapply(object@steps, FUN=function(x){x[["status"]]}, character(1))
		names(stepStates) <- names(object@steps)
		stepStates <- stepStates[res]
		if (status == "complete"){
			res <- res[stepStates == "complete"]
		} else if (status == "incomplete"){
			res <- res[stepStates != "complete"]
		} 
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("getGraph")) {
	setGeneric(
		"getGraph",
		function(object) standardGeneric("getGraph"),
		signature=c("object")
	)
}
#' getGraph-methods
#'
#' Get a graph representation storing all steps in the pipeline as nodes
#' and step dependencies as edges.
#'
#' @param object \code{\linkS4class{PipR}} object
#' @return An \code{igraph} object containing the graph
#' @seealso \code{\link{plotGraph,PipR-method}} for plotting the graph
#' @rdname getGraph-PipR-method
#' @docType methods
#' @aliases getGraph
#' @aliases getGraph,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("getGraph",
	signature(
		object="PipR"
	),
	function(
		object
	) {
		require(igraph)
		return(object@graph)
	}
)
################################################################################
# Utility functions
################################################################################
if (!isGeneric("plotGraph")) {
	setGeneric(
		"plotGraph",
		function(object, ...) standardGeneric("plotGraph"),
		signature=c("object")
	)
}
#' plotGraph-methods
#'
#' Plot the pipeline graph representation.
#' @details
#' Registered steps will appear in yellow, completed steps in green and active steps in orange.
#'
#' @param object \code{\linkS4class{PipR}} object
#' @param activeSteps Names of active steps to be highlighted in the plot.
#' @return the result of \code{plot}
#' @seealso \code{\link{getGraph,PipR-method}} for getting the graph as \code{igraph} object.
#' @rdname plotGraph-PipR-method
#' @docType methods
#' @aliases plotGraph
#' @aliases plotGraph,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("plotGraph",
	signature(
		object="PipR"
	),
	function(
		object,
		activeSteps=character()
	) {
		require(igraph)
		if (!all(activeSteps %in% getSteps(object))){
			logger.error("Unknown analysis steps")
		}
		colScheme <- c(incomplete="#FFF7BC", complete="#31A354", active="#FD8D3C")

		steps.active <- activeSteps
		steps.complete  <- getSteps(object, status="complete")
		steps.scheduled <- getSteps(object, status="incomplete")
		g <- object@graph
		V(g)$color[V(g)$name %in% steps.scheduled] <- colScheme["incomplete"]
		V(g)$color[V(g)$name %in% steps.complete]  <- colScheme["complete"]
		V(g)$color[V(g)$name %in% steps.active]    <- colScheme["active"]
		# plot(g, layout=layout_as_tree(g))
		# plot(g, layout=layout_as_tree(g, mode="all")) #TODO: mode="all" does not work yet. igraph is currently fixing this
		plot(g)
	}
)
################################################################################
# Handling the analysis directory
################################################################################
if (!isGeneric("initializePipeDir")) {
	setGeneric(
		"initializePipeDir",
		function(object) standardGeneric("initializePipeDir"),
		signature=c("object")
	)
}
#' initializePipeDir-methods
#'
#' Create the pipeline directory and it's subdirectories.
#' @details
#' Will result in an error if the pipeline directory is already existing.
#'
#' @param object \code{\linkS4class{PipR}} object
#' @return the modified object with changed directories in the object slots
#' @rdname initializePipeDir-PipR-method
#' @docType methods
#' @aliases initializePipeDir
#' @aliases initializePipeDir,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("initializePipeDir",
	signature(
		object="PipR"
	),
	function(
		object
	) {
		bd <- object@baseDir
		if (dir.exists(bd)){
			logger.error("Pipeline directory already exists")
		}
		dir.create(bd)
		logDir <- file.path(bd, "log")
		dir.create(logDir)
		srcDir <- file.path(bd, "src")
		dir.create(srcDir)
		statusDir <- file.path(bd, "status")
		dir.create(statusDir)
		resultDir <- file.path(bd, "results")
		dir.create(resultDir)
		tempDir <- file.path(bd, "temp")
		dir.create(tempDir)

		object@logDir    <- logDir
		object@srcDir    <- srcDir
		object@statusDir <- statusDir
		object@resultDir <- resultDir
		object@tempDir   <- tempDir

		return(object)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("changePipeDir")) {
	setGeneric(
		"changePipeDir",
		function(object, ...) standardGeneric("changePipeDir"),
		signature=c("object")
	)
}
#' changePipeDir-methods
#'
#' Change the pipeline directory and it's subdirectories.
#' @details
#' Will result in an error if the new pipeline directory is already existing.
#'
#' @param object \code{\linkS4class{PipR}} object
#' @param dir    the new pipeline directory
#' @return the modified object with changed directories in the object slots
#' @rdname changePipeDir-PipR-method
#' @docType methods
#' @aliases changePipeDir
#' @aliases changePipeDir,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("changePipeDir",
	signature(
		object="PipR"
	),
	function(
		object,
		dir
	) {
		object@baseDir <- dir
		object <- initializePipeDir(object)
		return(object)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("cleanup")) {
	setGeneric(
		"cleanup",
		function(object) standardGeneric("cleanup"),
		signature=c("object")
	)
}
#' cleanup-methods
#'
#' Remove temporary and uninformative files.
#' @details
#' Everything in the pipelines temporary will be deleted. Additionally, empty files
#' from the log directoy will be deleted.
#'
#' @param object \code{\linkS4class{PipR}} object
#' @return the unmodified object (invisible)
#' @rdname cleanup-PipR-method
#' @docType methods
#' @aliases cleanup
#' @aliases cleanup,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("cleanup",
	signature(
		object="PipR"
	),
	function(
		object
	) {
		tempDir <- object@tempDir
		if (dir.exists(tempDir)){
			unlink(file.path(tempDir,"*"), recursive=TRUE)
		}
		if (dir.exists(object@logDir)){
			ffs <- list.files(object@logDir, full.names=TRUE)
			ffs.del <- ffs[file.size(ffs)==0]
			unlink(ffs.del)
		}
		invisible(object)
	}
)
################################################################################
# Analysis steps
################################################################################
if (!isGeneric("parseJobStrings")) {
	setGeneric(
		"parseJobStrings",
		function(object, ...) standardGeneric("parseJobStrings"),
		signature=c("object")
	)
}
#' parseJobStrings-methods
#'
#' Instantiatiate pipeline variables in a vector of strings
#'
#' @param object     \code{\linkS4class{PipR}} object
#' @param jobStrings character vector containing strings in which variables will
#'                   be instantiated
#' @param step       the active pipeline step
#' @return the instantiated strings as character vector
#' @rdname parseJobStrings-PipR-method
#' @docType methods
#' @aliases parseJobStrings
#' @aliases parseJobStrings,PipR-method
#' @author Fabian Mueller
#' @noRd
setMethod("parseJobStrings",
	signature(
		object="PipR"
	),
	function(
		object,
		jobStrings,
		step=NULL
	) {
		res <- jobStrings
		if (!is.null(step)){
			pattern.stepdir <- "\\$\\{STEPDIR\\}"
			stepDir <- file.path(getDir(object, "result"), step)
			res <- gsub(pattern.stepdir, stepDir, res)
		}
		#specific step directories
		for (ss in getSteps(object)){
			stepDir.cur <- file.path(getDir(object, "result"), ss)
			pattern.stepdir.cur <- paste0("\\$\\{STEPDIR:", ss, "\\}")
			res <- gsub(pattern.stepdir.cur, stepDir.cur, res)
		}
		#base directory
		pattern.basedir <- "\\$\\{BASEDIR\\}"
		baseDir <- getDir(object, "base")
		res <- gsub(pattern.basedir, baseDir, res)
		#temp directory
		pattern.tempdir <- "\\$\\{TEMPDIR\\}"
		tempDir <- getDir(object, "temp")
		res <- gsub(pattern.tempdir, tempDir, res)
		return(res)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("addStep")) {
	setGeneric(
		"addStep",
		function(object, name, cmd, args=NULL, parents=character()) standardGeneric("addStep"),
		signature=c("object", "name", "cmd", "args", "parents")
	)
}
#' addStep-methods
#'
#' Add an analysis step to the pipeline
#' @details
#' The new analysis step will be registered and scheduled for running using
#' the \code{\link{run,PipR-method}} command.
#'
#' @param object  \code{\linkS4class{PipR}} object
#' @param name    A name for the analysis step. will be used as an identifier
#' @param cmd     Command(s)/Tool(s) pertaining to the analysis step as character vector. 
#'                If the vector is of length 1 (single string), the command will be recycled
#'                for all \code{args} elements
#' @param args    Command/Tool arguments pertaining to the analysis step as list of
#'                character vectors. There should be one list element for each command.
#' @param parents Character vector specifying the names of other analysis steps the
#'                new step depends on. These steps must already be in the pipeline object.
#' @return the modified pipeline object containing the added analysis step
#' @rdname addStep-PipR-method
#' @docType methods
#' @aliases addStep
#' @aliases addStep,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("addStep",
	signature(
		object="PipR"
	),
	function(
		object,
		name,
		cmd,
		args=NULL,
		parents=character()
	) {
		Njobs <- length(cmd)
		if (!is.character(cmd) || Njobs < 1){
			logger.error("[addStep,PipR] Invalid argument: cmd")
		}
		if (length(args) > 0){
			if (is.list(args)){
				if (length(args) != length(cmd)){
					if (length(cmd) == 1){
						Njobs <- length(args)
						cmd <- rep(cmd, Njobs)
					} else {
						logger.error("[addStep,PipR] Invalid argument: args: length does not match")
					}
				}
				validArgs <- sapply(args, is.character)
				if (!all(validArgs)){
					logger.error("[addStep,PipR] Invalid argument: args: list contains non-character elements")
				}
			} else {
				if (!is.character(args)){
					logger.error("[addStep,PipR] Invalid argument: args: is non-character")
				}
				args <- rep(list(args), Njobs)
			}	
		}
		stepExists <- is.element(name, getSteps(object))
		if (stepExists){
			logger.warning("Step already exists. --> Replacing current step setting")
		}
		if (!stepExists) object@graph <- add_vertices(object@graph, 1, name=name)

		name.hash <- paste(name, object@idHash, sep="_")

		# parent steps
		parentJobs <- list()
		if (length(parents) > 0){
			parents.exist <- parents %in% getSteps(object)
			if (!all(parents.exist)){
				logger.error(c("The following parent steps do not exist:", paste(parents[!parents.exist],collapse=",")))
			}
			edges <- as.vector(rbind(parents, name))
			object@graph <- add_edges(object@graph, edges)

			#get all jobs of parent steps
			parentJobs <- do.call("c", lapply(parents, FUN=function(parName){
				object@steps[[parName]][["jobs"]]
			}))
		}

		jobIds <- paste0(name.hash, "_j", 1:Njobs)
		jobs <- lapply(1:Njobs, FUN=function(i){
			res <- Job(cmd[i], args[[i]], jobIds[i], dependsOn=parentJobs)
			return(res)
		})
		names(jobs) <- jobIds

		stepDetails <- list(
			name=name,
			name.hash=name.hash,
			status="scheduled",
			dir=file.path(getDir(object, "result"), name),
			jobs=jobs,
			jobResults=list()
		)
		object@steps[[name]] <- stepDetails
		return(object)
	}
)
#-------------------------------------------------------------------------------
if (!isGeneric("resetStep")) {
	setGeneric(
		"resetStep",
		function(object, name, ...) standardGeneric("resetStep"),
		signature=c("object", "name")
	)
}
#' resetStep-methods
#'
#' Reset/undo an analysis step and all of it's dependent steps.
#' @details
#' The analysis step and potentially all it's dependent steps will be marked as
#' 'reset' in their status and associated result directories will be deleted.
#'
#' @param object  \code{\linkS4class{PipR}} object
#' @param name    Name for the analysis step to be reset/undone.
#' @param delete  Logical Indicating whether the step's result directory should be deleted
#' @param archiveLogs  Logical Indicating whether the step's log files should be put in an archive folder
#' 	              (carrying the timestamp of the execution of the reset step in its name)
#' @param resetDependendSteps Logical indicating whether subsequent steps that depend on the
#'                reset step should also be reset.
#' @return the modified pipeline object with the reset anlysis step(s)
#' @rdname resetStep-PipR-method
#' @docType methods
#' @aliases resetStep
#' @aliases resetStep,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("resetStep",
	signature(
		object="PipR"
	),
	function(
		object,
		name,
		delete=TRUE,
		archiveLogs=TRUE,
		resetDependendSteps=TRUE
	) {
		if (!is.element(name, getSteps(object))){
			logger.error(c("Unknown analysis step:", name))
		}
		stepDetails <- object@steps[[name]]
		object@steps[[name]][["status"]]     <- "reset"
		object@steps[[name]][["jobResults"]] <- list()
		if (delete && dir.exists(stepDetails[["dir"]])){
			unlink(stepDetails[["dir"]], recursive=TRUE)
		}
		# archive log files
		if (archiveLogs){
			logDir <- getDir(object, "log")
			stepJobs <- stepDetails[["jobs"]]
			files2move <- c()
			for (jj in stepJobs){
				jid <- getId(jj)
				jobFiles <- list.files(logDir, pattern=jid)
				if (length(jobFiles) > 0 && jobFiles != ""){
					files2move <- c(files2move, jobFiles)
				}
			}
			if (length(files2move) > 0){
				archDir <- file.path(logDir, format(Sys.time(), "archive_%Y%m%d_%H%M%S"))
				if (!dir.exists(archDir)) dir.create(archDir) #the directory could already exists, if the step is reset as a consequence of a recursive call
				file.rename(file.path(logDir, files2move), file.path(archDir, files2move))
			}
		}
		# reset dependend steps
		if (resetDependendSteps){
			depSteps <- subcomponent(object@graph, name, mode="out")$name
			depSteps <- setdiff(depSteps, name)
			for (ss in depSteps){
				object <- resetStep(object, ss, delete=delete, archiveLogs=archiveLogs, resetDependendSteps=FALSE)
			}
		}
		return(object)
	}
)
################################################################################
# Run the pipeline
################################################################################
# TODO: comment back in once RnBeads generic Definition of "run" has been updated
# if (!isGeneric("run")) {
	setGeneric(
		"run",
		function(object, ...) standardGeneric("run"),
		signature=c("object")
	)
# }
#' run-methods
#'
#' Run all incomplete analysis steps in the pipeline
#' @details
#' The method will run all incomplete analysis steps given a \code{\linkS4class{CommandR}}
#' object for executing the jobs pertaining to each step
#'
#' @param object      \code{\linkS4class{PipR}} object
#' @param cmdr        \code{\linkS4class{CommandR}} object that will execute all jobs
#'                    pertaining to each pipeline step.
#' @param createDirs  Logical Indicating whether subdirectories for each pipeline step should
#'                    be created.
#' @param saveStatus  Logical indicating whether the \code{\linkS4class{PipR}} object should
#'                    save itself and the current pipeline graph with annotated steps
#'                    in the pipeline's status directory.
#' @param logCommands Logical indicating whether the commands that have been used should be
#'                    written to a log file.
#' @return the modified pipeline object with updated step states.
#' @rdname run-PipR-method
#' @docType methods
#' @aliases run
#' @aliases run,PipR-method
#' @author Fabian Mueller
#' @export
setMethod("run",
	signature(
		object="PipR"
	),
	function(
		object,
		cmdr=CommandRsystem(logDir=getDir(object, "log")),
		createDirs=TRUE,
		saveStatus=TRUE,
		logCommands=FALSE,
		...
	) {
		if (!inherits(cmdr, "CommandR")){
			logger.error("[run,PipR] Invalid argument: cmdr")
		}
		if (saveStatus){
			fn <- file.path(getDir(object, "status"), "pipr.rds")
			saveRDS(object, fn)
		}
		stepsOrdered <- getSteps(object, graphOrder=TRUE)

		logDir <- getDir(object, "log")
		stepRes <- list()
		for (step in stepsOrdered){
			stepDetails <- object@steps[[step]]
			if (is.element(step, getSteps(object, status="incomplete"))){
				logger.status(c("Executing step:", stepDetails[["name"]]))
				fn.pipeGraph <- file.path(getDir(object, "status"), "pipe_status.pdf")
				if (saveStatus){
					pdf(fn.pipeGraph, width=10, height=10)
						plotGraph(object, activeSteps=step)
					dev.off()
				}

				jobList <- stepDetails[["jobs"]]
				unsetDeps <- FALSE
				# unsetDeps <- !missing(wait) && wait # unset job dependencies if waiting for the previous step anyways
				for (i in 1:length(jobList)){
					#parse variables in args
					if (length(jobList[[i]]@args > 0)){
						jobList[[i]]@args <- parseJobStrings(object, jobList[[i]]@args, step=step)
					}
					if (unsetDeps && length(getDepJobs(jobList[[i]])) > 0){
						jobList[[i]]@dependsOn <- list()
					}
				}

				if (createDirs){
					if (dir.exists(stepDetails[["dir"]])){
						logger.warning(c("Result directory already exists for step", step))
					} else {
						dir.create(stepDetails[["dir"]])
					}
				}

				rr <- lexec(cmdr, jobList, ...)
				object@steps[[step]][["jobResults"]] <- rr
				object@steps[[step]][["status"]] <- "complete"

				if (logCommands){
					cmds <- unlist(lapply(rr, FUN=function(x){getCommand(x)}))
					if (length(cmds) > 0){
						fn <- file.path(getDir(object, "src"), paste0(step, "_jobCommands.log"))
						writeLines(cmds, fn)
					}
				}
				if (saveStatus){
					fn <- file.path(getDir(object, "status"), "pipr.rds")
					saveRDS(object, fn)
					pdf(fn.pipeGraph, width=10, height=10)
						plotGraph(object)
					dev.off()
				}
			} else {
				logger.status(c("Skipping already completed step:", stepDetails[["name"]]))
			}
		}

		return(object)
	}
)
