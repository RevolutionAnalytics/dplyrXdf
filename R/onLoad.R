.onLoad <- function(libname, pkgname)
{
    # patched version of rxGetInfo for <= 7.3
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
    
    # patched version of rxGetInfo for >= 7.4
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

    environment(f73) <- environment(rxGetInfo)
    environment(f74) <- environment(rxGetInfo)

    rxver <- packageVersion("RevoScaleR")
    suppressWarnings(if(rxver <= package_version("7.5.0"))
    {
        f <- if(rxver >= package_version("7.4.0")) f74 else f73
        unlockBinding("rxGetInfo", as.environment("package:RevoScaleR"))
        assignInNamespace("rxGetInfo", value=f, ns="RevoScaleR", pos="package:RevoScaleR")
        assign("rxGetInfo", f, pos="package:RevoScaleR")
        lockBinding("rxGetInfo", as.environment("package:RevoScaleR"))
    })
    
    # turn progress reporting off
    rxOptions(reportProgress=0)

    invisible(NULL)
}

