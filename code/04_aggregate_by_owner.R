
###############################################
# Load needed packagesand config
###############################################

suppressMessages({
  library(data.table)
  library(future.apply)
})

source("code/config.R")
run_parallel <- toupper(Sys.getenv("RUN_PARALLEL")) == "TRUE"


###############################################
# Define function
###############################################

# Create output folder
dir.create("data/generated/aggregated_data", recursive = TRUE, showWarnings = FALSE)

aggregate_by_owner <- function(ctry){
  
  
  ###############################################
  # Load data
  ###############################################
  smp <- readRDS(paste0("data/generated/sample_data/",ctry,"_sample.rds"))
  
  ###############################################
  # Aggregate data
  ###############################################
  
  # Convert to data.table
  setDT(smp)
  
  # Set up parallel processing
  if (run_parallel) {
    options(future.globals.maxSize = 4 * 1024^3) 
    plan(multisession, workers = max(1, parallel::detectCores() - 1))
  } else {
    plan(sequential)
  }
  
  # Split dataset by year (fixed)
  split_data <- split(smp, smp$year)
  
  # Process each year in parallel
  output_list <- future_lapply(split_data, function(df) {
    df[, list(
      n_entities       = uniqueN(bvd_id),
      sum_total_assets = sum(total_assets, na.rm = TRUE),
      sum_employees    = sum(employees, na.rm = TRUE),
      max_assets       = max(total_assets, na.rm = TRUE),
      max_employees    = max(employees, na.rm = TRUE),
      income_statements= sum(!is.na(income))
    ), by = list(owner, year)]
  })
  
  # Combine results
  aggregated_data <- rbindlist(output_list)
  
  # Reset to sequential processing after completion
  plan(sequential)
  
  
  # Save
  saveRDS(aggregated_data, paste0("data/generated/aggregated_data/",ctry,"_aggregated_data.rds"))
  rm(list = ls())
  gc()
  
  
}

# Aggregate the sample of entities by owner
aggregate_by_owner("DE")
aggregate_by_owner("AT")
aggregate_by_owner("DK")
aggregate_by_owner("LU")
aggregate_by_owner("LV")

