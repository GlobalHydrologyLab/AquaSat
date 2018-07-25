#' Render an Rmd file to html, with the key side effect of probably producing a
#' data file
#' @param input name of the Rmd file
#' @param output_file name of the desired file (probably .html)
#' @param ... other arguments passed to rmarkdown::render
render_rmd <- function(input, output_file, ...) {
  rmarkdown::render(input=input, output_file=basename(output_file), ...)
  rmd_out <- file.path(dirname(input), basename(output_file)) 
  file.copy(rmd_out, output_file)
  file.remove(rmd_out)
}
