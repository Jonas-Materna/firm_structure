
###############################################
# This code filters Orbis financial data similar to Beuselinck et al. (2023): 
# Keep only financial statements with 12-month reporting period and where the reporting type is either consolidated  or unconsolidated
# If both consolidated and unconsolidated statements are available, the consolidated statement is used
# The resulting parquet file comprises not more than one statement per entity-year
###############################################



###############################################
# Load needed packages 
###############################################

suppressMessages({
  library(DBI)
  library(dplyr)
  library(glue)
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
# Load and clean data
###############################################


# Connect to DuckDB
con <- connect_duckdb()

# File paths
FIN_DATA <- pdata("industry_global_financials_and_ratios_eur.parquet")

dbExecute(con, glue("
  CREATE OR REPLACE VIEW financial_data AS
  SELECT
    \"BvD ID number\" AS bvd_id,
    SUBSTR(CAST(\"Closing date\" AS VARCHAR), 1, 4) AS year,
    \"Consolidation code\" AS conscode,
    \"Closing date\" AS fye,
    \"Number of months\" AS nr_month,
    \"Total assets\" AS total_assets,
    \"Number of employees\" AS employees,
    \"Operating revenue (Turnover)\" AS revenue,
    \"Sales\" AS sales,
    \"P/L for period [=Net income]\" AS income
  FROM read_parquet('{FIN_DATA}')
"))


# Filtering and make sure to have one statement per firm year only 
df <- tbl(con, "financial_data") %>%
  filter(nr_month == "12") %>%
  filter(conscode %in% c("C1", "C2", "U1", "U2")) %>%
  mutate(
    cc_rank = ifelse(
      conscode == "C1", 4,
      ifelse(
        conscode == "C2", 3,
        ifelse(conscode == "U1", 2, 1)
      )
    )
  ) %>%
  group_by(bvd_id, year) %>%
  #Take most aggregated statement (highest cc_rank)
  filter(cc_rank == max(cc_rank, na.rm = TRUE)) %>%
  #Among those, take the most recent closing date
  filter(fye == max(fye, na.rm = TRUE)) %>%
  #If still more than one observation, take first
  filter(row_number() == 1) %>%
  ungroup() %>%
  select(-cc_rank)


# Export to parquet
rv <- DBI::dbExecute(
  con, paste0(
    "COPY (", dbplyr::sql_render(df), 
    ") TO 'data/generated/financial_data_clean.parquet' (FORMAT 'parquet');"
  ))

disconnect_duckdb(con)