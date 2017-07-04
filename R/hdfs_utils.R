# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(inherits(rxGetFileSystem(.data), "RxHdfsFileSystem"))
        stop(..., call.=FALSE)
}


# create the temporary directory in HDFS
# must run this every time we create a new tbl, because tempdir in HDFS is not guaranteed to exist
makeHdfsWorkDir <- function()
{
    if(.dxOptions$hdfsWorkDirCreated)  # quit if already created
        return()
    rxHadoopMakeDir(.dxOptions$hdfsWorkDir)
    .dxOptions$hdfsWorkDirCreated <- TRUE
    NULL
}


isHdfs <- function(fs)
{
    (fs == "hdfs") || inherits(fs, "RxHdfsFileSystem")
}
