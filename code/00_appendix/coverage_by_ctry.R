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
connect_duckdb <- function(dbase) {
  con <- dbConnect(duckdb::duckdb(), ":memory:")
  return(con)
}

disconnect_duckdb <- function(con){
  dbDisconnect(con, shutdown = TRUE)
}



###############################################
# Define function to get sample
###############################################

# Create output folder
dir.create("data/generated/owner_coverage", recursive = TRUE, showWarnings = FALSE)

get_coverage <- function(ctry){
  
  
  ###############################################
  # Load legal data
  ###############################################
  legal_data <- file.path("data/pulled/legal_info.parquet") 
  
  query_str <- sprintf(
    paste0("SELECT \"BvD ID number\" AS bvd_id, ",
           "\"Standardised legal form\" AS legal_form ",
           "FROM '%s' WHERE \"BvD ID number\" LIKE '", ctry, "%%'"),
    legal_data
  )
  
  
  con <- connect_duckdb(":memory:")
  legal_data <- dbGetQuery(con, query_str)
  disconnect_duckdb(con)
  
  
  
  ###############################################
  # Load financial data
  ###############################################
  financial_data <- file.path("data/generated/financial_data_clean.parquet") 
  
  query_str <- sprintf(
    paste0("SELECT * FROM '%s' WHERE \"bvd_id\" LIKE '", ctry, "%%'"),
    financial_data
  )
  
  con <- connect_duckdb(":memory:")
  financial_data <- dbGetQuery(con, query_str)
  disconnect_duckdb(con)
  
  
  ###############################################
  # Load owner data
  ###############################################
  owner_data <- file.path(paste0("data/generated/owner_data/",ctry,"_owner_data.parquet")) 
  
  query_str <- sprintf(
    paste0("SELECT * FROM '%s'"),
    owner_data
  )
  
  con <- connect_duckdb(":memory:")
  owner_data  <- dbGetQuery(con, query_str)
  disconnect_duckdb(con)
  
  
  ###############################################
  # Filter
  ###############################################
  
  # Keep only limited liability entities for the years 2007 to 2022
  llc <- legal_data$bvd_id[legal_data$legal_form %in% "Private limited companies"]
  smp <- financial_data %>% 
    filter(!is.na(total_assets)) %>%
    filter(bvd_id %in% llc & year %in% c(2007:2024))
  
  
  # Merge to owner data
  smp <- left_join(smp, owner_data, by = c("bvd_id", "year"))
  
  
  
  # Remove all owners if one of their entities discloses a consolidated financial statement 
  bad_owner <- unique(smp$owner[smp$conscode %in% c("C1", "C2")])
  bad_owner <- bad_owner[!is.na(bad_owner)]
  smp <- smp %>% filter(!owner %in% bad_owner)
  
  
  # Get share of years with available owner data
  dt <- smp %>%
    group_by(year) %>%
    summarise("entities" = n(),
              "owner_available" = sum(!is.na(owner)),
              "share" = owner_available / entities)
  
  # Save it
  write.csv(dt, paste0("data/generated/owner_coverage/", ctry, "_owner_coverage.csv"), row.names = FALSE)
  
}




# Get all files 
files <- list.files(path = "data/generated/owner_data")
countries <- substr(files, 1, 2)


for(ctry in countries){
  message(glue("Processing {ctry}..."))
  get_coverage(ctry)
}
