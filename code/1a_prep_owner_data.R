
###############################################
# Load needed packages 
###############################################

suppressMessages({
  library(DBI)
  library(dplyr)
})


###############################################
# Define functions and parameters to access parquet files 
###############################################

# Set parquet folder
PARQUET_FOLDER <- "data/pulled"

# Define some functions to access parquet files
pdata <- function(pfile) {
  file.path(PARQUET_FOLDER, pfile) 
} 

connect_duckdb <- function(dbase) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con){
  dbDisconnect(con, shutdown = TRUE)
}


###############################################
# Get a history od GUO 50
###############################################

get_guo_data <- function(ctry, y){
  
  file <- paste0("links_", y, ".parquet")
  
  guo_info <- pdata(file)
  
  guo_info_cols <- c("Subsidiary BvD ID",
                     "Shareholder BvD ID",
                     "Type of relation",
                     "Information date",
                     "GUO 50")
  
  query_str <- sprintf(
    paste0(
      "SELECT \"%s\" FROM '%s' WHERE \"Subsidiary BvD ID\" LIKE '", ctry, "%%' AND \"Type of relation\" = 'GUO 50'"
    ),
    paste(guo_info_cols, collapse = '", "'),
    guo_info
  )
  
  
  con <- connect_duckdb(":memory:")
  guo_info <- dbGetQuery(con, query_str)
  disconnect_duckdb(con)
  
  
  guo_info <- guo_info %>%
    filter(!is.na(`GUO 50`)) %>%
    distinct()
  
  # If there are multiple observations keep most recent
  guo_info  <- guo_info  %>%
    group_by(`Subsidiary BvD ID`) %>%
    filter(`Information date` == max(`Information date`)) %>%
    ungroup()
  
  guo_info$year <- substr(guo_info$`Information date`,1,4)
  
  guo_info <- guo_info %>% filter(year %in% y)
  
  guo_info
  
}


for (y in 2010:2022) {
  
  var_name <- paste0("guo_info_", y)
  
  assign(var_name, get_guo_data(ctry = "DE", y = y))
  
  print(paste("Finished processing year:", y))
}



guo_data <- bind_rows(guo_info_2010,
                      guo_info_2011,
                      guo_info_2012,
                      guo_info_2013,
                      guo_info_2014,
                      guo_info_2015,
                      guo_info_2016,
                      guo_info_2017,
                      guo_info_2018,
                      guo_info_2019,
                      guo_info_2020,
                      guo_info_2021,
                      guo_info_2022)

saveRDS(guo_data, "data/pulled/guo_data.rds")
