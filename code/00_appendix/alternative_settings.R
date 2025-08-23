


# Medium-sized Threshold 

# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

lower_limit_employees <- 0.7 * 250
upper_limit_employees  <- 1.3 * 250

lower_limit_total_assets <- 0.7 * 20000000
upper_limit_total_assets  <- 1.3 * 20000000

smp_owner <- dt_owner %>% 
  filter(sum_employees >= lower_limit_employees & sum_employees <= upper_limit_employees) %>%
  filter(sum_total_assets >= lower_limit_total_assets & sum_total_assets <= upper_limit_total_assets)





# Drittelbeteiligungsgesetz


# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

lower_limit_employees <- 0.7 * 500
upper_limit_employees  <- 1.3 * 500

smp_owner <- dt_owner %>% 
  filter(sum_employees >= lower_limit_employees & sum_employees <= upper_limit_employees)




# Mitbestimmungsgesetz


# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

lower_limit_employees <- 0.7 * 2000
upper_limit_employees  <- 1.3 * 2000

smp_owner <- dt_owner %>% 
  filter(sum_employees >= lower_limit_employees & sum_employees <= upper_limit_employees)
