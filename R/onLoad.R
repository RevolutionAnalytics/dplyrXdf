.onLoad <- function(libname, pkgname)
{
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
        hdfs_dir_remove(e$hdfsWorkDir, skipTrash=TRUE, host=e$hdfsHost)
    }

    # remove local working directory
    if(tempdir() != .dxOptions$localWorkDir)
        unlink(.dxOptions$localWorkDir, recursive=TRUE)

    NULL
}
