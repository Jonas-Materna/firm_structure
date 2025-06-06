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
dir.create("data/generated/sample_data", recursive = TRUE, showWarnings = FALSE)


get_sample <- function(ctry, years){
  
  
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
  # Population limited liability entities with financial data for the years specified
  ###############################################
  llc <- legal_data$bvd_id[legal_data$legal_form %in% "Private limited companies"]
  smp <- financial_data %>% 
    filter(!is.na(total_assets)) %>%
    filter(bvd_id %in% llc & year %in% years) 
  
  # Count number of observations
  population <- nrow(smp)
  
  ###############################################
  # Filter 2: Remove all owners if one of their entities discloses a consolidated financial statement 
  ###############################################
  # Merge to owner data
  smp <- left_join(smp, owner_data, by = c("bvd_id", "year"))
  bad_owner <- unique(smp$owner[smp$conscode %in% c("C1", "C2")])
  bad_owner <- bad_owner[!is.na(bad_owner)]
  smp <- smp %>% filter(!owner %in% bad_owner)
  filter_1 <- nrow(smp)
  
  ###############################################
  # Filter 3: Remove all with missing owner data
  ###############################################
  smp <- smp %>% filter(!is.na(owner))
  filter_2 <- nrow(smp)

  
  ###############################################
  # Save it
  ###############################################
  sample_selection <- data.frame(population, filter_1 , filter_2)
  

  saveRDS(sample_selection, paste0("data/generated/sample_data/",ctry,"_sample_selection.rds"))
  if(nrow(smp > 0)){
    saveRDS(smp, paste0("data/generated/sample_data/",ctry,"_sample.rds"))
  }else{
    message(glue("No observations found for {ctry}"))
  }

  
}

# Get sample 
get_sample("DE", c(2015:2021))
get_sample("AT", c(2015:2021))
get_sample("DK", c(2015:2021))
get_sample("LU", c(2015:2021))
get_sample("LV", c(2015:2021))


