

###########################
# Load packages and define functions to open parquet file for contact information
###########################

library(dplyr)
library(DBI)
library(dplyr)
library(glue)
library(ggplot2)
library(purrr)
library(tibble)

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


###########################
# Load data 
###########################

# Connect to DuckDB
con <- connect_duckdb()

# File path
CONTACT_DATA <- pdata("contact_info.parquet")

dbExecute(con, glue("
  CREATE OR REPLACE VIEW contact_data AS
  SELECT
    \"BvD ID number\" AS bvd_id,
    \"NAME_NATIVE\" AS name,
    \"Telephone number\" AS phone,
    \"E-mail address\" AS mail,
    \"Website address\" AS website
  FROM read_parquet('{CONTACT_DATA}')
  WHERE \"BvD ID number\" LIKE 'DE%'
"))

dt_contact <- tbl(con, "contact_data") %>%
  collect()
disconnect_duckdb(con)



# File path
OWNER_DATA <- pdata("contact_info.parquet")



# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")


# Load data of entities owned by those owners
dt_entities  <- readRDS(paste0("data/generated/sample_data/DE_sample.rds"))

########################
# Find extreme cases not exceeding total assets threshold
########################

# Get those that do not exceed the threshold with any entity
dt <- dt_owner %>% 
  filter(year %in% 2021) %>%
  filter(sum_total_assets > 6000000) %>%
  filter(max_assets < 6000000) 


# Top 5 in assets
top5_assets <- dt %>%
  arrange(desc(sum_total_assets)) %>%
  slice_head(n = 5)

example_1_toas  <- dt_entities %>% filter(owner %in% top5_assets$owner[1]) %>% filter(year %in% 2021)
example_1_toas  <- left_join(example_1_toas, dt_contact, by = "bvd_id")

example_2_toas  <- dt_entities %>% filter(owner %in% top5_assets$owner[2]) %>% filter(year %in% 2021)
example_2_toas  <- left_join(example_2_toas, dt_contact, by = "bvd_id")

example_3_toas  <- dt_entities %>% filter(owner %in% top5_assets$owner[3]) %>% filter(year %in% 2021)
example_3_toas  <- left_join(example_3_toas, dt_contact, by = "bvd_id")





########################
# Find extreme cases not exceeding employee threshold
########################

# Get those that do not exceed the threshold with any entity
dt <- dt_owner %>% 
  filter(year %in% 2021) %>%
  filter(sum_employees > 50) %>%
  filter(max_employees < 50) 


# Top 5 in assets
top5_employees <- dt %>%
  arrange(desc(sum_employees)) %>%
  slice_head(n = 5)

example_1_emp  <- dt_entities %>% filter(owner %in% top5_employees$owner[1]) %>% filter(year %in% 2021)
example_1_emp  <- left_join(example_1_emp, dt_contact, by = "bvd_id")

example_2_emp  <- dt_entities %>% filter(owner %in% top5_employees$owner[2]) %>% filter(year %in% 2021)
example_2_emp  <- left_join(example_2_emp, dt_contact, by = "bvd_id")

example_3_emp  <- dt_entities %>% filter(owner %in% top5_employees$owner[3]) %>% filter(year %in% 2021)
example_3_emp  <- left_join(example_3_emp, dt_contact, by = "bvd_id")


