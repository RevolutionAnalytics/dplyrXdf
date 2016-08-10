.onLoad <- function(libname, pkgname)
{
    # patched version of rxGetInfo for RRE <= 7.3
    f73 <- function (data, getVarInfo = FALSE, getBlockSizes = FALSE, getValueLabels = NULL,
        varsToKeep = NULL, varsToDrop = NULL, startRow = 1, numRows = 0,
        computeInfo = FALSE, allNodes = TRUE, verbose = 0)
    {
        currentComputeContext <- rxGetComputeContext()
        if (rxuIsDistributedHpa() && !is.data.frame(data)) {
            if (rxIsListOfDataSources(data)) {
                stop("rxGetInfo does not support a list of data sources in a distributed compute context.")
            }
            matchCall <- match.call()
            if ((!is.null(allNodes) && allNodes == FALSE) || rxIsDistributedFileSystem(currentComputeContext)) {
                if (inherits(currentComputeContext, "RxInTeradata")) {
                    computeContextName <- rxGetOptionComputeContextName()
                    rxSetComputeContext("local")
                    if (!is.null(computeContextName)) {
                      on.exit(rxSetComputeContext(computeContextName),
                        add = TRUE)
                    }
                    else {
                      on.exit(rxSetComputeContext(currentComputeContext),
                        add = TRUE)
                    }
                    if (inherits(data, "RxTeradata")) {
                      data <- RxDataSource(dataSource = data, class = "RxOdbcData")
                    }
                }
                else {
                    return(rxExecJob(rxCallInfo(matchCall), matchCall = matchCall))
                }
            }
            else {
                matchCallList <- as.list(matchCall)
                matchCallList[["allNodes"]] <- NULL
                if (length(matchCallList) > 1) {
                    for (i in 2:length(matchCallList)) {
                      if (!is.null(matchCallList[[i]])) {
                        matchCallList[[i]] <- eval.parent(matchCallList[[i]])
                      }
                    }
                }
                return(do.call("rxExec", matchCallList))
            }
        }
        if (rxIsListOfDataSources(data)) {
            return(rxRecallDataList(data))
        }
        bIsNonXdfDataSource <- FALSE
        bIsNonXdfDataSource <- rxIsNonXdfDataSource(data)
        if (is.data.frame(data)) {
            infoList <- list(objName = as.character(substitute(data)),
                description = attr(data, ".rxDescription"), class = "data.frame",
                numRows = nrow(data), numVars = ncol(data), numBlocks = 0)
            if (!is.null(infoList) && getVarInfo == TRUE) {
                if (is.null(getValueLabels)) {
                    getValueLabels <- TRUE
                }
                infoList$varInfo <- rxGetVarInfoInternal(data = data,
                    varsToKeep = varsToKeep, varsToDrop = varsToDrop,
                    getValueLabels = getValueLabels)
            }
            if (!is.null(infoList) && infoList$numRows > 0 && numRows >
                0) {
                if (startRow <= 0) {
                    stop("startRow must be greater than 0")
                }
                varIndices <- rxGetVarsToKeepIndices(names(data),
                    varsToKeep = varsToKeep, varsToDrop = varsToDrop)
                infoList$data <- data[startRow:(startRow + numRows -
                    1), varIndices, drop = FALSE]
                row.names(infoList$data) <- NULL
                infoList$startRow <- startRow
            }
            oldClass(infoList) <- "rxGetInfo"
            return(infoList)
        }
        else if (rxIsCharacterScalarNonEmpty(data) || inherits(data,
            "RxXdfData") || bIsNonXdfDataSource) {
            rxGetInfoXdf(file = data, getVarInfo = getVarInfo, getBlockSizes = getBlockSizes,
                getValueLabels = getValueLabels, varsToKeep = varsToKeep,
                varsToDrop = varsToDrop, startRow = startRow, numRows = numRows,
                computeInfo = computeInfo, verbose = verbose)
        }
        else if (rxIsCharacterScalarNonEmpty(data) || is(data, "RxDistributedHpa")) {
            rxGetNodeInfo(data, nodes = NULL, getWorkersOnly = TRUE)
        }
        else {
            infoList <- list(objName = as.character(substitute(data)),
                description = attr(data, ".rxDescription"), class = class(data),
                numBlocks = 0)
            if (!is.null(nrow(data))) {
                infoList$numRows <- nrow(data)
            }
            if (!is.null(ncol(data))) {
                infoList$numVars <- ncol(data)
            }
            else if (!is.null(length(data))) {
                infoList$length <- length(data)
            }
            oldClass(infoList) <- "rxGetInfo"
            return(infoList)
        }
    }
    
    # patched version of rxGetInfo for RRE 7.4
    f74 <- function (data, getVarInfo = FALSE, getBlockSizes = FALSE, getValueLabels = NULL, 
        varsToKeep = NULL, varsToDrop = NULL, startRow = 1, numRows = 0, 
        computeInfo = FALSE, allNodes = TRUE, verbose = 0) 
    {
        currentComputeContext <- rxGetComputeContext()
        if (rxuIsDistributedHpa() && !is.data.frame(data)) {
            if (rxIsListOfDataSources(data)) {
                stop("rxGetInfo does not support a list of data sources in a distributed compute context.")
            }
            matchCall <- match.call()
            if ((!is.null(allNodes) && allNodes == FALSE) || rxIsDistributedFileSystem(currentComputeContext)) {
                if (inherits(currentComputeContext, "RxInTeradata")) {
                    computeContextName <- rxGetOptionComputeContextName()
                    rxSetComputeContext("local")
                    if (!is.null(computeContextName)) {
                      on.exit(rxSetComputeContext(computeContextName), 
                        add = TRUE)
                    }
                    else {
                      on.exit(rxSetComputeContext(currentComputeContext), 
                        add = TRUE)
                    }
                    if (inherits(data, "RxTeradata")) {
                      data <- RxDataSource(dataSource = data, class = "RxOdbcData")
                    }
                }
                else {
                    return(rxExecJob(rxCallInfo(matchCall), matchCall = matchCall))
                }
            }
            else {
                matchCallList <- as.list(matchCall)
                matchCallList[["allNodes"]] <- NULL
                if (length(matchCallList) > 1) {
                    for (i in 2:length(matchCallList)) {
                      if (!is.null(matchCallList[[i]])) {
                        matchCallList[[i]] <- eval.parent(matchCallList[[i]])
                      }
                    }
                }
                return(do.call("rxExec", matchCallList))
            }
        }
        if (rxIsListOfDataSources(data)) {
            return(rxRecallDataList(data))
        }
        bIsNonXdfDataSource <- FALSE
        bIsNonXdfDataSource <- rxIsNonXdfDataSource(data)
        if (is.data.frame(data)) {
            infoList <- list(objName = as.character(substitute(data)), 
                description = attr(data, ".rxDescription"), class = "data.frame", 
                numRows = nrow(data), numVars = ncol(data), numBlocks = 0)
            if (!is.null(infoList) && getVarInfo == TRUE) {
                if (is.null(getValueLabels)) {
                    getValueLabels <- TRUE
                }
                infoList$varInfo <- rxGetVarInfoInternal(data = data, 
                    varsToKeep = varsToKeep, varsToDrop = varsToDrop, 
                    getValueLabels = getValueLabels)
            }
            if (!is.null(infoList) && infoList$numRows > 0 && numRows > 
                0) {
                if (startRow <= 0) {
                    stop("startRow must be greater than 0")
                }
                varIndices <- rxGetVarsToKeepIndices(names(data), 
                    varsToKeep = varsToKeep, varsToDrop = varsToDrop)
                infoList$data <- data[startRow:(startRow + numRows - 
                    1), varIndices, drop = FALSE]
                row.names(infoList$data) <- NULL
                infoList$startRow <- startRow
            }
            oldClass(infoList) <- "rxGetInfo"
            return(infoList)
        }
        else if (rxIsCharacterScalarNonEmpty(data) || inherits(data, 
            "RxXdfData") || bIsNonXdfDataSource) {
            rxGetInfoXdfInternal(file = data, getVarInfo = getVarInfo, 
                getBlockSizes = getBlockSizes, getValueLabels = getValueLabels, 
                varsToKeep = varsToKeep, varsToDrop = varsToDrop, 
                startRow = startRow, numRows = numRows, computeInfo = computeInfo, 
                verbose = verbose)
        }
        else if (rxIsCharacterScalarNonEmpty(data) || is(data, "RxDistributedHpa")) {
            rxGetNodeInfo(data, nodes = NULL, getWorkersOnly = TRUE)
        }
        else {
            infoList <- list(objName = as.character(substitute(data)), 
                description = attr(data, ".rxDescription"), class = class(data), 
                numBlocks = 0)
            if (!is.null(nrow(data))) {
                infoList$numRows <- nrow(data)
            }
            if (!is.null(ncol(data))) {
                infoList$numVars <- ncol(data)
            }
            else if (!is.null(length(data))) {
                infoList$length <- length(data)
            }
            oldClass(infoList) <- "rxGetInfo"
            return(infoList)
        }
    }

    # patched version of rxExecInDataHadoop for MRS 8
    h <- function (callInfo, hpaCall) 
    {
        rxRuntimeTraceIn("rxExecInDataHadoop()")
        computeContext <- rxGetOption("computeContext")
        if (!inherits(computeContext, "RxHadoopMR")) {
            stop("Internal error: Not an in-Hadoop compute context.")
        }
        if (!rxHadoopIsBackendReady()) {
            stop("Hadoop components have not been configured, aborting")
        }
        nameNode <- computeContext@nameNode
        port <- computeContext@port
        hdfsFS <- RxHdfsFileSystem(hostName = nameNode, port = port)
        data <- callInfo$ARGS$data
        if (is.null(data)) {
            data <- callInfo$ARGS$inData
            if (is.null(data)) {
                data <- callInfo$ARGS$inFile
                if (is.null(data)) {
                    data <- callInfo$ARGS$file
                }
            }
        }
        if (is.null(data)) {
            stop("A 'data' argument must be specified.")
        }
        if (!is.null(attr(data, "fileSystem")) && !is.null(data@fileSystem) && 
            data@fileSystem$fileSystemType == "hdfs" && (grepl("^wasb://", 
            nameNode) || grepl("^adl://", nameNode) || grepl("^wasb://", 
            data@fileSystem$hostName) || grepl("^adl://", data@fileSystem$hostName))) {
            if (nameNode != data@fileSystem$hostName) {
                stop(paste("Data source and spark compute context have different storage (azure blob/azure data lake) namenodes:", 
                    data@fileSystem$hostName, "and", nameNode, sep = " "))
            }
        }
        outData <- callInfo$ARGS$outFile
        if (!is.null(outData)) {
            if (!is.null(attr(outData, "fileSystem")) && !is.null(outData@fileSystem) && 
                outData@fileSystem$fileSystemType == "hdfs" && (grepl("^wasb://", 
                nameNode) || grepl("^adl://", nameNode) || grepl("^wasb://", 
                outData@fileSystem$hostName) || grepl("^adl://", 
                outData@fileSystem$hostName))) {
                if (nameNode != outData@fileSystem$hostName) {
                    stop(paste("OutFile Data and spark compute context have different storage (azure blob/azure data lake) namenodes:", 
                      outData@fileSystem$hostName, "and", nameNode, 
                      sep = " "))
                }
            }
        }
        data <- rxGetInDataSource(data, fileSystem = hdfsFS)
#        dataClass <- class(data)
#        if ((dataClass != "RxTextData") && (dataClass != "RxXdfData") && 
#            (dataClass != "data.frame")) {
        if(!inherits(data, c("RxTextData", "RxXdfData", "data.frame"))) {
            stop("Data must be an RxTextData or RxXdfData data source.")
        }
        inDataFile <- "Temp"
#        if (dataClass != "data.frame") {
        if (!inherits(data, "data.frame")) {
            if (!is.null(data@fileSystem) && (rxGetFileSystemType(data@fileSystem) != 
                "hdfs")) {
                stop("Data source does not have an hdfs file system type.")
            }
            inDataFile <- data@file
        }
        guid <- rxCreateGuid()
        if (inherits(computeContext, "RxSpark")) {
            pathBase <- paste(computeContext@shareDir, "/", guid, 
                "/", sep = "")
            inputInHdfs <- FALSE
            outputInHdfs <- FALSE
            dir.create(pathBase, recursive = TRUE, mode = "0700")
            on.exit(unlink(pathBase, recursive = TRUE), add = TRUE)
        }
        else {
            pathBase <- paste(computeContext@hdfsShareDir, "/", guid, 
                "/", sep = "")
            inputInHdfs <- TRUE
            outputInHdfs <- TRUE
        }
        hdfsShareDirPath <- paste(computeContext@hdfsShareDir, "/", 
            guid, sep = "")
        prefix <- ""
        if (length(grep("://", nameNode, fixed = TRUE)) > 0) {
            prefix <- nameNode
        }
        on.exit(rxDeleteHelperGuidDirFromHdfs(paste(prefix, hdfsShareDirPath, 
            sep = "")), add = TRUE)
        inputObjectExt <- ".input"
        ouputObjectExt <- ".iro"
        inputObjectPath <- paste(pathBase, inputObjectExt, sep = "")
        finalIROPath <- paste(pathBase, "IRO", ouputObjectExt, sep = "")
#        isInputXdf <- ((dataClass == "RxXdfData") || (dataClass == 
#            "data.frame"))
        isInputXdf <- inherits(data, c("data.frame", "RxXdfData"))
        if (isInputXdf) {
            minSplitSizeSwitch <- "-Dmapred.min.split.size=9223372036854775807"
        }
        else {
            minSplitSizeSwitch <- ""
        }
        if (inherits(computeContext, "RxSpark")) {
            fifoToSpark <- paste(computeContext@shareDir, paste("RevoSparkFifoToSpark", 
                computeContext@user, computeContext@rSessionPid, 
                computeContext@appId, sep = "-"), sep = "/")
            fifoFromSpark <- paste(computeContext@shareDir, paste("RevoSparkFifoFromSpark", 
                computeContext@user, computeContext@rSessionPid, 
                computeContext@appId, sep = "-"), sep = "/")
            if (!computeContext@wait) {
                fifoToSpark <- paste(fifoToSpark, computeContext@jobId, 
                    sep = "-")
                fifoFromSpark <- paste(fifoFromSpark, computeContext@jobId, 
                    sep = "-")
            }
            hadoopCmdToLaunchJob <- paste(system.file("utils/spark/RevoSparkHPALauncher", 
                package = "RevoScaleR"), fifoToSpark, fifoFromSpark, 
                isInputXdf, inputObjectPath, inDataFile, finalIROPath, 
                computeContext@hadoopRPath, nameNode, as.character(port), 
                callInfo$FUN, "spark-no-header-xUwb1", "spark-create-xdf-rdd", 
                hdfsShareDirPath, computeContext@user, computeContext@rSessionPid, 
                computeContext@appId, computeContext@jobId, computeContext@wait, 
                computeContext@idleTimeout, computeContext@executorCores, 
                tolower(computeContext@executorMem), tolower(computeContext@driverMem), 
                RevoScaleR:::countMb(computeContext@executorOverheadMem), 
                computeContext@numExecutors, clusterGUIDDir, paste0("\"", 
                    gsub("\"", "\\\\\"", computeContext@extraSparkConfig), 
                    "\""))
            revoScaleRPath <- system.file(package = "RevoScaleR")
            sparkSubmitKillerPath <- paste(revoScaleRPath, "utils/spark/RevoSparkSubmitKiller", 
                sep = "/")
            if (computeContext@wait) {
                system(paste(sparkSubmitKillerPath, computeContext@user, 
                    computeContext@rSessionPid, computeContext@appId, 
                    computeContext@jobId, computeContext@wait, "clean", 
                    "session", sep = " "))
            }
            if (!computeContext@persistentRun) {
                on.exit(system(paste(sparkSubmitKillerPath, computeContext@user, 
                    computeContext@rSessionPid, computeContext@appId, 
                    computeContext@jobId, computeContext@wait, "stop", 
                    fifoToSpark, sep = " ")), add = TRUE)
            }
        }
        else {
            if (.rxIsWindows()) {
                computeContext@hadoopRPath <- "RScript.exe"
            }
            hadoopSwitchesFromEnv <- "${REVOHADOOPSWITCHES}"
            hadoopCmdToLaunchJob <- paste(rxHadoopBin(), "RevoScaleR", 
                "-Dmapred.reduce.tasks=1", hadoopSwitchesFromEnv, 
                computeContext@hadoopSwitches, minSplitSizeSwitch, 
                inputObjectPath, finalIROPath, inDataFile, nameNode, 
                as.character(port), computeContext@hadoopRPath)
        }
        rxRuntimeTrace(paste("Hadoop/Spark system command:", hadoopCmdToLaunchJob))
        localComputeContext <- RxLocalSeq()
        if (!is.null(computeContext@dataPath)) {
            localComputeContext@dataPath <- computeContext@dataPath
        }
        if (!is.null(computeContext@outDataPath)) {
            localComputeContext@outDataPath <- computeContext@outDataPath
        }
        oldOpt <- rxOptions(computeContext = localComputeContext)
        on.exit(rxOptions(computeContext = computeContext), add = TRUE)
        blocksPerRead <- rxGetOption("blocksPerRead")
        oldOptions <- rxSetInternalPathAndBlockOptions(blocksPerRead)
        on.exit(rxResetInternalPathAndBlockOptions(oldOptions), add = TRUE)
        rxCall("RxUtil", list(USE = "InitializeHadoop", inputObjectPath = inputObjectPath, 
            inputInHdfs = inputInHdfs, outputObjectPath = finalIROPath, 
            outputInHdfs = outputInHdfs, hdfsShareDirPath = hdfsShareDirPath, 
            systemCommand = hadoopCmdToLaunchJob, runningInSpark = inherits(computeContext, 
                "RxSpark")))
        on.exit(rxCall("RxUtil", list(USE = "UninitializeHadoop")), 
            add = TRUE)
        options(mds.inHadoop = TRUE)
        on.exit(options(mds.inHadoop = FALSE), add = TRUE)
        retObj <- eval(hpaCall)
        rxRuntimeTraceOut("rxExecInDataHadoop()")
        return(retObj)
    }

    environment(f73) <- environment(rxGetInfo)
    environment(f74) <- environment(rxGetInfo)
    environment(h) <- environment(rxGetInfo)

    rxver <- packageVersion("RevoScaleR")
    suppressWarnings(if(rxver < package_version("7.5.0"))  # 7.5 corrects bug in rxGetInfo
    {
        f <- if(rxver >= package_version("7.4.0")) f74 else f73
        unlockBinding("rxGetInfo", as.environment("package:RevoScaleR"))
        assignInNamespace("rxGetInfo", value=f, ns="RevoScaleR", pos="package:RevoScaleR")
        assign("rxGetInfo", f, pos="package:RevoScaleR")
        lockBinding("rxGetInfo", as.environment("package:RevoScaleR"))
    })
    suppressWarnings({
        assignInNamespace("rxExecInDataHadoop", value=h, ns="RevoScaleR", pos="package:RevoScaleR")
    })
    
    # turn progress reporting off
    rxOptions(reportProgress=0)

    # run auxiliary initialisation code, set finaliser
    .dxInit()
    reg.finalizer(.dxOptions, .dxFinal, onexit=TRUE)

    # warn about warning
    if(.dxOptions$dplyrVersion >= package_version("0.5"))
        message("dplyrXdf defines the union_all generic to allow\n",
                "interoperability with versions of dplyr < 0.5.\n",
                "Ignore the message about masking the same object from dplyr.")

    invisible(NULL)
}


# environment for storing options
.dxOptions <- new.env(parent=emptyenv())


.dxInit <- function()
{
    .dxOptions$rowsPerRead <- 500000

    # set the HDFS temporary directory
    hdfsTempDir <- gsub("\\", "/", tempfile(pattern="dxTmp", tmpdir="/tmp"), fixed=TRUE)
    .dxOptions$hdfsTempDir <- hdfsTempDir
    .dxOptions$hdfsTempDirCreated <- FALSE

    # store dplyr version
    .dxOptions$dplyrVersion <- packageVersion("dplyr")

    NULL
}


.dxFinal <- function(e)
{
    # remove the HDFS temporary directory
    if(e$hdfsTempDirCreated)
        rxHadoopRemoveDir(e$hdfsTempDir, skipTrash=TRUE, intern=TRUE)
    NULL
}


dxOptions <- function(...)
{
    if(nargs() == 0)
        return(as.list(.dxOptions))
    dots <- list(...)
    nams <- names(dots)
    opts <- names(.dxOptions)
    if(!all(nams %in% opts))
    {
        badOpts <- nams[!(nams %in% opts)]
        stop("invalid dplyrXdf option(s): ", paste(badOpts, collapse=", "))
    }
    for(o in nams)
        .dxOptions[[o]] <- dots[[o]]
    as.list(.dxOptions)
}
