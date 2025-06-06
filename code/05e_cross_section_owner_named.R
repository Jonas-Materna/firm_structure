
##################
# Load packages
##################
library(dplyr)
library(modelsummary)
library(DBI)
library(data.table)


##################
# Prep data
##################

# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

lower_limit_employees <- 0.7 * 50
upper_limit_employees  <- 1.3 * 50

lower_limit_total_assets <- 0.7 * 6000000
upper_limit_total_assets  <- 1.3 * 6000000

smp_owner <- dt_owner %>% 
  filter(sum_employees >= lower_limit_employees & sum_employees <= upper_limit_employees) %>%
  filter(sum_total_assets >= lower_limit_total_assets & sum_total_assets <= upper_limit_total_assets)


# Get owners with multiple firms
smp_owner$multiple <- smp_owner$n_entities > 1

# Load data of entities owned by those owners
smp_entities  <- readRDS(paste0("data/generated/sample_data/DE_sample.rds"))

smp_entities <- left_join(smp_entities, smp_owner, by = c("owner", "year"))

smp_entities <- smp_entities %>% 
  filter(!is.na(n_entities)) %>% 
  select(bvd_id, year, total_assets, employees, income, owner, n_entities )

rm(dt_owner, lower_limit_employees, upper_limit_employees,  lower_limit_total_assets, upper_limit_total_assets)






##################
# Get owner named entities
##################


# Set parquet folder
PARQUET_FOLDER <- "data/pulled"

# Define access functions
pdata <- function(pfile) file.path(PARQUET_FOLDER, pfile)

connect_duckdb <- function(dbase = NULL) {
  dbConnect(duckdb::duckdb(), ":memory:")
}

disconnect_duckdb <- function(con) {
  dbDisconnect(con, shutdown = TRUE)
}

### Get names of all entities
entity_name <- pdata("contact_info.parquet")

entity_name_cols <- c("BvD ID number",
                      "NAME_NATIVE")


# Create DuckDB connection
con <- connect_duckdb()

query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" LIKE '","DE", "%%'"),
  paste(entity_name_cols, collapse = '", "'),
  entity_name
)

# Run query and collect result
con <- connect_duckdb(":memory:")
entity_name <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


entity_name <- entity_name %>% filter(!is.na(NAME_NATIVE))
colnames(entity_name) <- c("bvd_id", "entity_name")

# Merge
smp_entities <- left_join(smp_entities, entity_name, by = "bvd_id")




### Get names of owners 
owner_name <- pdata("controlling_shareholders.parquet")

owner_name_cols <- c("BvD ID number",
                     "CSH - BvD ID number",
                     "CSH - Last name",
                     "CSH - Total %")


query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" LIKE '","DE", "%%'"),
  paste(owner_name_cols, collapse = '", "'),
  owner_name
)

con <- connect_duckdb(":memory:")
owner_name <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


owner_name <- owner_name %>% 
  filter(!is.na(`CSH - Last name`)) %>% 
  select(`CSH - BvD ID number`, `CSH - Last name`)


colnames(owner_name) <- c("owner", "owner_name")



# Identify owners that have at least one owner named entity
smp_entities <- left_join(smp_entities, owner_name, by = "owner")

# Check whether entity is owner named 
smp_entities$owner_name  <- gsub("\\(|\\)","",as.character(smp_entities$owner_name))
smp_entities$owner_named  <- as.logical(mapply(grepl,smp_entities$owner_name, smp_entities$entity_name, ignore.case = T))
smp_entities$owner_named[is.na(smp_entities$owner_named)] <- FALSE
owner_named_entities <- smp_entities %>% filter(owner_named)
length(unique(owner_named_entities$bvd_id))


### Get owners that hold at least one owner named entity
owner_named <- unique(smp_entities$owner[smp_entities$bvd_id %in% owner_named_entities$bvd_id])
smp_owner$owner_named <- smp_owner$owner %in% owner_named

# Descriptive
sum(smp_owner$owner_named)
length(unique(smp_owner$owner[smp_owner$owner_named]))

##################
# Regression 
##################

smp_owner$above_toas_thresh <- smp_owner$sum_total_assets > 6000000
smp_owner$above_emp_thresh <- smp_owner$sum_employees > 50
smp_owner$above_both <- smp_owner$above_toas_thresh & smp_owner$above_emp_thresh

m1 <- list(
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + owner_named + above_toas_thresh + above_toas_thresh * owner_named, data = smp_owner),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + owner_named + above_emp_thresh +  above_emp_thresh * owner_named, data = smp_owner),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + owner_named + above_both + above_both * owner_named, data = smp_owner)
)


modelsummary(
  dvnames(m1),
  stars = TRUE
)

