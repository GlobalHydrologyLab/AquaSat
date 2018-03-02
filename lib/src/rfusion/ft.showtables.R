ft.showtables <- function(auth) {
  url <- "https://www.googleapis.com/fusiontables/v2/query"
  params <- list(sql = "SHOW TABLES")
  
  result <- POST(url, config(token=auth), query = params)
  
  contents <- content(result)
  ids_names <- unlist(contents$rows)
  tables <- data.frame(names = ids_names[seq(2, length(ids_names), 2)],
                       ids = ids_names[seq(1, length(ids_names), 2)])
  
  return(tables)
}

