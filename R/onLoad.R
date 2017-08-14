.onLoad <- function(libname, pkgname)
{
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

    # patched version of rxCheckSupportForDataSource for MRS 9.1
    sup <- function(computeContext, jobType, data, isOutData=FALSE)
    {
        if(is.null(data))
        {
            return(TRUE)
        }
        if(isOutData)
        {
            inout.datasource <- "output data source"
        }
        else
            {
            inout.datasource <- "input data source"
        }
        if(class(computeContext) == "RxHadoopMR")
        {
            if(inherits(data, "RxSparkData"))
            {
                msg <- paste(class(data), "is supported in RxSpark compute context only")
                stop(msg, call.=FALSE)
            }
        }
        else if(class(computeContext) == "RxSpark")
        {
            #if(class(data) == "character")
            if(is.character(data))
            {
                stop(paste(data, "is considered as local file. Data source used in RxSpark compute context must be in hdfs file system."),
                call.=FALSE)
            }
            #else if(class(data) %in% c("RxTextData", "RxXdfData",
            #"RxParquetData", "RxOrcData"))
            else if(inherits(data, c("RxTextData", "RxXdfData", "RxParquetData", "RxOrcData")))
            {
                if(data@fileSystem$fileSystemType != "hdfs")
                {
                    if(grepl("://", data@file))
                    {
                        message(class(data), "specifying a hdfs fileSystem is recommended")
                    }
                    else
                    {
                        msg <- paste(class(data), "as", inout.datasource,
                    "in RxSpark compute context must be in hdfs file system")
                        stop(msg, call.=FALSE)
                    }
                }
                if(is(data, "RxXdfData") && isTRUE(isOutData) &&
                    tolower(rxFileExtension(data@file)) == "xdf")
                {
                    msg <- paste(data@file, "has extension '.xdf', which is considered as single XDF and not supported in RxHadoopMR and RxSpark compute context")
                    stop(msg, call.=FALSE)
                }
                if(is(data, "RxXdfData") && isTRUE(isOutData) &&
                    is.logical(data@createCompositeSet) && data@createCompositeSet == FALSE)
                {
                    stop("The `createCompositeSet` argument cannot be set to FALSE in RxHadoopMR and RxSpark compute context.",
                  call.=FALSE)
                }
                if(!is(data, "RxParquetData") && !is(data, "RxOrcData"))
                {
                    cc.nameNode <- computeContext@nameNode
                    data.hostName <- data@fileSystem$hostName
                    if(grepl("://", cc.nameNode) || grepl("://", data.hostName))
                    {
                        if(cc.nameNode != data.hostName)
                        {
                            msg <- paste(class(data), "data source and RxSpark compute context have different hdfs (default/azure blob/azure data lake). data source:",
                                cc.nameNode, ", compute context:", data.hostName)
                                stop(msg, call.=FALSE)
                        }
                    }
                }
            }
            #else if(class(data) == "RxHiveData")
            else if(inherits(data, "RxHiveData"))
            {
                if(isOutData && data@dfType == "hive")
                {
                    msg <- paste("Cannot use RxHiveData with query as",
                  inout.datasource, ". Please use RxHiveData with table.")
                    stop(msg, call.=FALSE)
                }
            }
            else
            {
                msg <- paste(class(data), "as", inout.datasource,
                "in RxSpark compute context is not supported")
                stop(msg, call.=FALSE)
            }
            if(inherits(data, "RxSparkData") && identical(jobType, "hpc"))
            {
                stop("RxSparkData is not supported in HPC (rxExec) mode",
                call.=FALSE)
            }
        }
        else
        {
            if(inherits(data, "RxSparkData"))
            {
                msg <- paste(class(data), "is supported in RxSpark compute context only")
                stop(msg, call.=FALSE)
            }
            return(TRUE)
        }
        TRUE
    }

    environment(h) <- environment(sup) <- environment(rxGetInfo)

    rxver <- packageVersion("RevoScaleR")
    suppressWarnings({
        #if(rxver < package_version("9.0"))
            #assignInNamespace("rxExecInDataHadoop", value=h, ns="RevoScaleR", pos="package:RevoScaleR")
        #if(rxver <= package_version("9.1"))
            #assignInNamespace("rxCheckSupportForDataSource", value=sup, ns="RevoScaleR", pos="package:RevoScaleR")
    })
    
    # turn progress reporting off
    rxOptions(reportProgress=0)

    # run auxiliary initialisation code, set finaliser
    .dxInit()
    reg.finalizer(.dxOptions, .dxFinal, onexit=TRUE)

    invisible(NULL)
}


# environment for storing options
.dxOptions <- new.env(parent=emptyenv())


.dxInit <- function()
{
    .dxOptions$rowsPerRead <- 500000
    .dxOptions$dplyrVersion <- packageVersion("dplyr")

    rxver <- packageVersion("RevoScaleR")
    .dxOptions$useExecBy <- (rxver >= package_version("9.1"))

    set_dplyrxdf_dir(fileSystem="native")
    set_dplyrxdf_dir(fileSystem="hdfs")
    NULL
}


.dxFinal <- function(e)
{
    # remove the HDFS working directory, if we are still connected
    if(e$hdfsWorkDirCreated && !is.na(detectHdfsConnection(stopIfNotConnected=FALSE)))
    {
        message("Removing HDFS working directory")
        rxHadoopRemoveDir(e$hdfsWorkDir, skipTrash=TRUE, intern=TRUE)
    }

    # remove local working directory
    if(tempdir() != .dxOptions$localWorkDir)
        unlink(.dxOptions$localWorkDir, recursive=TRUE)

    NULL
}
