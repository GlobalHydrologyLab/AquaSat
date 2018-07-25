#' Render an Rmd file to html, with the key side effect of probably producing a
#' data file
#' @param input name of the Rmd file
#' @param output_file name of the desired file (probably .html)
#' @param ... other arguments passed to rmarkdown::render
render_rmd <- function(input, output_file, ...) {
  rmarkdown::render(input=input, output_file=output_file, ...)
}
