

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
# Load data and restrict sample to owners close to the threshold
###########################

# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

lower_limit_employees <- 0.7 * 50
upper_limit_employees  <- 1.3 * 50

lower_limit_total_assets <- 0.7 * 6000000
upper_limit_total_assets  <- 1.3 * 6000000

smp_owner <- dt_owner %>% 
  filter(sum_employees >= lower_limit_employees & sum_employees <= upper_limit_employees) %>%
  filter(sum_total_assets >= lower_limit_total_assets & sum_total_assets <= upper_limit_total_assets)




# Load data of entities owned by those owners
dt_entities  <- readRDS(paste0("data/generated/sample_data/DE_sample.rds"))

smp_entities <- left_join(dt_entities, smp_owner, by = c("owner", "year"))

smp_entities <- smp_entities %>% 
  filter(!is.na(n_entities)) %>% 
  select(bvd_id, year, total_assets, employees, income, owner, n_entities )

rm(dt_owner, dt_entities, lower_limit_employees, upper_limit_employees,  lower_limit_total_assets, upper_limit_total_assets)




###########################
# Descriptive table: Entities
###########################

vars_to_summarise <- c("total_assets",
                       "employees",
                       "discloses_is")

smp_entities$discloses_is <- !is.na(smp_entities$income) 

# Create summary table
summary_table_entities <- map_dfr(vars_to_summarise, function(var) {
  vals <- smp_entities[[var]]
  tibble(
    variable = var,
    n = sum(!is.na(vals)),
    mean = mean(vals, na.rm = TRUE),
    min = min(vals, na.rm = TRUE),
    p25 = quantile(vals, 0.25, na.rm = TRUE),
    median = median(vals, na.rm = TRUE),
    p75 = quantile(vals, 0.75, na.rm = TRUE),
    max = max(vals, na.rm = TRUE)
  )
})

# Save
write.csv(summary_table_entities, "output/summary_table_entities.csv", row.names = FALSE)

###########################
# Descriptive table: Owner
###########################



vars_to_summarise <- c("n_entities",
                       "multiple",
                       "sum_total_assets",
                       "sum_employees")

smp_owner$multiple <- smp_owner$n_entities > 1

# Create summary table
summary_table_owner <- map_dfr(vars_to_summarise, function(var) {
  vals <- smp_owner[[var]]
  tibble(
    variable = var,
    n = sum(!is.na(vals)),
    mean = mean(vals, na.rm = TRUE),
    min = min(vals, na.rm = TRUE),
    p25 = quantile(vals, 0.25, na.rm = TRUE),
    median = median(vals, na.rm = TRUE),
    p75 = quantile(vals, 0.75, na.rm = TRUE),
    max = max(vals, na.rm = TRUE)
  )
})

# Save
write.csv(summary_table_owner, "output/summary_table_owner.csv", row.names = FALSE)





###########################
# Get contact data and investigate appearance to outsiders
###########################

# Connect to DuckDB
con <- connect_duckdb()

# File paths
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


smp_entities <- left_join(smp_entities, dt_contact, by= c("bvd_id"))


# Group by owner-year
dt <- smp_entities %>%
  filter(n_entities > 1) %>%
  group_by(owner, year) %>%
  summarise(n_entities = length(unique(bvd_id)),
            
            # Phone
            phone_available = sum(!is.na(phone)),
            unique_phone = length(unique(phone)),
            
            # Website
            website_available = sum(!is.na(website)),
            unique_website = length(unique(website)),
            
            # Mail
            mail_available = sum(!is.na(mail)),
            unique_mail = length(unique(mail)))


# Same phone number
dt_phone <- dt %>% filter(n_entities == phone_available)
sum(dt_phone$unique_phone < dt_phone$n_entities) /nrow(dt_phone)


# Same website number
dt_website <- dt %>% filter(n_entities == website_available)
sum(dt_website$unique_website < dt_website$n_entities) /nrow(dt_website)

# Same mail
dt_mail <- dt %>% filter(n_entities == mail_available)
sum(dt_mail$unique_mail < dt_mail$n_entities) /nrow(dt_mail)





###########################
# Investigate industries
###########################


# Connect to DuckDB
con <- connect_duckdb()

# File paths
INDUSTRY_DATA <- pdata("industry_classifications.parquet")

dbExecute(con, glue("
  CREATE OR REPLACE VIEW industry_data AS
  SELECT
    \"BvD ID number\" AS bvd_id,
    \"NACE Rev. 2 main section\" AS industry
  FROM read_parquet('{INDUSTRY_DATA}')
  WHERE \"BvD ID number\" LIKE 'DE%'
    AND \"NACE Rev. 2 main section\" IS NOT NULL
"))

dt_industry <- tbl(con, "industry_data") %>%
  collect()
disconnect_duckdb(con)


# Merge 
smp_entities <- left_join(smp_entities, dt_industry, by= c("bvd_id"))


# Industries in sample
total_industries <- data.frame(table(smp_entities$industry))
colnames(total_industries) <- c("industry", "total")

industries_single <- data.frame(table(smp_entities$industry[smp_entities$n_entities == 1]))
colnames(industries_single) <- c("industry", "total_single")

industries_multiple <- data.frame(table(smp_entities$industry[smp_entities$n_entities > 1]))
colnames(industries_multiple) <- c("industry", "total_multiple")

dt <- left_join(total_industries, industries_single, by = "industry")
dt <- left_join(dt, industries_multiple, by = "industry")

dt$share_multiple <- dt$total_multiple / dt$total

dt_total <- data.frame(
  industry = "Total",
  total = sum(dt$total, na.rm = TRUE),
  total_single = sum(dt$total_single, na.rm = TRUE),
  total_multiple = sum(dt$total_multiple, na.rm = TRUE),
  share_multiple  = sum(dt$total_multiple, na.rm = TRUE) / sum(dt$total, na.rm = TRUE)
)

# Bind to original data frame
dt <- rbind(dt, dt_total)



# Save
write.csv(dt, "output/industries.csv", row.names = FALSE)






###########################
# Get owner characteristics
###########################



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
owner_name_unique <- owner_name[!duplicated(owner_name$owner), ]

# Merge
smp_owner <- left_join(smp_owner, owner_name_unique, by = "owner")
sum(!is.na(smp_owner$owner_name)) / nrow(smp_owner)




# Get origin

origin <- substr(smp_owner$owner,1,2)
origin_count <- data.frame(table(origin))


sum(origin %in% "DE") / length(origin)
sum(origin %in% "US") / length(origin)
sum(origin %in% "CH") / length(origin)





###########################
# Get legal form of owner
###########################

bvd_ids_str <- paste(sprintf("'%s'", unique(smp_owner$owner)), collapse = ", ")

legal_info <- pdata("legal_info.parquet")


# Query string adjusted to filter by BvD ID number instead of country
query_str <- sprintf(
  paste0("SELECT * FROM '%s' WHERE \"BvD ID number\" IN (%s)"),
  legal_info,
  bvd_ids_str
)

con <- connect_duckdb(":memory:")
legal_info <- dbGetQuery(con, query_str)
disconnect_duckdb(con)





