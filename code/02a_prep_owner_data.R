
##################################################
# This code loads historic ownership data for specified countries for the years 2007 to 2022 an generates a panel data set of ownership
# The resulting panel data consists of owners with a share of more than 50% in the firm, irrespective of the owners country of origin 
# Warning: The resulting owner datasets are large; only apply to countries needed
##################################################

suppressPackageStartupMessages(suppressWarnings({
  library(dplyr)
  library(duckdb)
  library(glue)
  library(fs)
  library(arrow)  
}))

source("code/config.R")

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

# Generate ouput folder if it does not exist
dir_create(glue("data/generated/owner_data"))

get_owner_panel <- function(ctry){
  
  # Connect to DuckDB
  con <- connect_duckdb()
  
  # Set years
  years <- 2007:2024
  
  # Build a query across all historic parquets. Only load data where the owner is available and the information is recent 
  select_blocks <- sapply(years, function(y) {
    glue(
      "SELECT ",
      "\"Subsidiary BvD ID\" AS bvd_id, ",
      "\"GUO 50\" AS owner, ",
      "\"Information date\" AS information_date, ",
      "SUBSTR(CAST(\"Information date\" AS VARCHAR), 1, 4) AS year ",
      "FROM read_parquet('data/pulled/links_{y}.parquet') ",
      "WHERE \"GUO 50\" IS NOT NULL ",
      "AND \"Subsidiary BvD ID\" LIKE '{ctry}%' ",
      "AND SUBSTR(CAST(\"Information date\" AS TEXT), 1, 4) = '{y}'"
    )
  })
  
  # Combine the data
  sql_query <- glue("
    CREATE OR REPLACE TABLE owner_data AS
    {paste(select_blocks, collapse = '\nUNION ALL\n')}
  ")
  
  dbExecute(con, sql_query)
  
  # Remove duplicates: latest information per firm-year, then first if still more than one per firm year
  dbExecute(con, "
    CREATE OR REPLACE TABLE owner_data_no_dup AS
    SELECT *
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY bvd_id, year ORDER BY information_date DESC) AS row_num
      FROM owner_data
    )
    WHERE row_num = 1
  ")
  
  # Export the cleaned data to parquet
  dbExecute(con, glue("
    COPY (SELECT bvd_id, year, owner FROM owner_data_no_dup)
    TO 'data/generated/owner_data/{ctry}_owner_data.parquet'
    (FORMAT 'parquet')
  "))
  
  # Clean and disconnect
  dbExecute(con, "DROP TABLE owner_data")
  dbExecute(con, "DROP TABLE owner_data_no_dup")
  gc()
  disconnect_duckdb(con)
}

########################################
# Run function
########################################

# Set Countries
ctries <- c("AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "GB")

# Check if some countries are alredy done
done <- list.files(path = "data/generated/owner_data")
done <- substr(done, 1, 2)
ctries <- ctries[!ctries %in% done]


for (ctry in ctries) {
  message(glue("Processing {ctry}..."))
  get_owner_panel(ctry)
}
