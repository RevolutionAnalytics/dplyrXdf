# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(inherits(rxGetFileSystem(.data), "RxHdfsFileSystem"))
        stop(..., call.=FALSE)
}


# create the temporary directory in HDFS
# must run this every time we create a new tbl, because tempdir in HDFS is not guaranteed to exist
makeHdfsTempDir <- function()
{
    if(.dxOptions$hdfsTempDirCreated)  # quit if already created
        return()
    rxHadoopMakeDir(.dxOptions$hdfsTempDir)
    .dxOptions$hdfsTempDirCreated <- TRUE
    NULL
}
