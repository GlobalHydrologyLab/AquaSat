check_gd_config <- function() {
  cfg_file <- 'lib/cfg/gd_config.yml'
  if(!file.exists(cfg_file)) {
    cmd <- "gd_config(config_file='lib/cfg/gd_config.yml', folder=I('your_file_id_goes_here'))"
    stop(sprintf(
      "%s missing. Create a folder on Google Drive and then call %s with that folder ID (last piece of the URL)",
      cfg_file,
      cmd))
  } else {
    return(TRUE)
  }
}
