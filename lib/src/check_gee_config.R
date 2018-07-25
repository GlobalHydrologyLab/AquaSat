check_gee_config <- function() {
  cfg_file <- 'lib/cfg/gee_config.yml'
  if(!file.exists(cfg_file)) {
    txt <- "RETICULATE_PYTHON: /usr/local/bin/python2"
    stop(sprintf(
      "%s missing. Create a file called %s with the following text, modified to point to your python installation that is GEE-authorized: %s",
      cfg_file, cfg_file,
      txt))
  } else {
    return(TRUE)
  }
}
