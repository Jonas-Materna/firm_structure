
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
# Comparison on industries (is this particularly commin among real estate firms?)
##################


### Load industry data 

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



### Get names industries
industry_data <- pdata("industry_classifications.parquet")

industry_data_cols <- c("BvD ID number",
                     "NACE Rev. 2 main section")


query_str <- sprintf(
  paste0("SELECT \"%s\" FROM '%s' WHERE \"BvD ID number\" LIKE '","DE", "%%'"),
  paste(industry_data_cols, collapse = '", "'),
  industry_data
)

con <- connect_duckdb(":memory:")
industry_data <- dbGetQuery(con, query_str)
disconnect_duckdb(con)


industry_data <- industry_data %>% 
  filter(!is.na(`NACE Rev. 2 main section`))


colnames(industry_data) <- c("bvd_id", "industry")



### Get owners of entities in industries
bvd_ids <- unique(industry_data$bvd_id[industry_data$industry %in% c("L - Real estate activities")])
owners  <- unique(smp_entities$owner[smp_entities$bvd_id %in% bvd_ids])

smp_owner$real_estate_industry <- smp_owner$owner %in% owners


# Descriptive
sum(smp_owner$real_estate_industry)
length(unique(smp_owner$owner[smp_owner$real_estate_industry]))

##################
# Regression 
##################

smp_owner$above_toas_thresh <- smp_owner$sum_total_assets > 6000000
smp_owner$above_emp_thresh <- smp_owner$sum_employees > 50
smp_owner$above_both <- smp_owner$above_toas_thresh & smp_owner$above_emp_thresh

m1 <- list(
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + real_estate_industry + above_toas_thresh + above_toas_thresh * real_estate_industry, data = smp_owner),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + real_estate_industry + above_emp_thresh +  above_emp_thresh * real_estate_industry, data = smp_owner),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + real_estate_industry + above_both + above_both * real_estate_industry, data = smp_owner)
)


modelsummary(
  dvnames(m1),
  stars = TRUE
)

