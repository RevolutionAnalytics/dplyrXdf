#' @export
dir_hdfs <- function(path=NULL, full_path=FALSE, dirs_only=FALSE, ...)
{
    cc <- rxGetComputeContext()
    if(!inherits(cc, "RxHadoopMR"))
        stop("Must be in HadoopMR or Spark compute context")

    user <- cc@sshUsername
    if(is.null(path))
        path <- file.path("/user", user, fsep="/")

    output <- capture.output(res <- rxHadoopListFiles(path, ...))
    if(!res)
        stop("Unable to get directory listing")

    # some people, when confronted with a problem, think "I know, I'll use regular expressions"
    output <- sub('^.*\\[[^]]*\\][^"+]"([^"]*)".*$', '\\1', output)[-1]

    if(dirs_only)
        output <- output[substr(output, 1, 1) == "d"]
    output <- gsub("^[^/]*(/.*)$", "\\1", output)
    if(!full_path)
        output <- basename(output)
    message("Directory listing of ", path)
    output
}


# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(isHdfs(rxGetFileSystem(.data)))
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
    inherits(fs, "RxHdfsFileSystem") || (fs == "hdfs")
}
