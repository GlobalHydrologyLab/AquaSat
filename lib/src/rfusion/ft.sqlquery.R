ft.sqlquery <- function(auth, sqlquery) {
  url <- "https://www.googleapis.com/fusiontables/v2/query"
  params <- list(sql = sqlquery)
  
  result <- POST(url, config(token=auth), query = params)
  
  contents <- content(result)
  
  return(contents)
}

