ft.describetable <- function(auth, table_id) {
  url <- "https://www.googleapis.com/fusiontables/v2/query"
  params <- list(sql = paste("DESCRIBE", table))
  result <- POST(url, config(token=auth), query = params)
  contents <- content(result)
  table_data <- unlist(contents$rows)
  tlength <- length(table_data)
  fields <- data.frame(column = table_data[seq(1, tlength, 3)],
                       field = table_data[seq(2, tlength, 3)],
                       type = table_data[seq(3, tlength, 3)])
  return(fields)
}

