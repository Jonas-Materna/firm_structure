
##################
# Load packages
##################
library(dplyr)
library(DBI)
library(ggplot2)
library(ggpubr)
library(patchwork)
library(modelsummary)
library(extrafont)
loadfonts(device = "win")



##################
# Prep data
##################

# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

cutoff_assets <- 6000000
cutoff_employees <- 50

# Define bandwidths
lower_limit_employees <- 0.7 * cutoff_employees
upper_limit_employees <- 1.3 * cutoff_employees

lower_limit_total_assets <- 0.7 * cutoff_assets
upper_limit_total_assets <- 1.3 * cutoff_assets

# Filter data within bandwidths
dt <- dt_owner %>%
  filter(between(sum_employees, lower_limit_employees, upper_limit_employees)) %>%
  filter(between(sum_total_assets, lower_limit_total_assets, upper_limit_total_assets))

# Get owners with multiple firms
dt$multiple <- dt$n_entities > 1




##################
# Plot: Sum of Total Assets
##################

# Get buckets
bucket_size_assets <- (upper_limit_total_assets - lower_limit_total_assets) / 20
dt$toas_centered <- dt$sum_total_assets - cutoff_assets
dt$toas_bucket <- cut(dt$toas_centered, 
                      breaks = seq(-0.3 * cutoff_assets, 0.3 * cutoff_assets, by = bucket_size_assets), 
                      include.lowest = TRUE)



# Aggregate by bucket
dt_toas <- dt %>%
  group_by(toas_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(toas_centered, na.rm = TRUE)) 

# Plot
text_size  <- 14
point_size <- 2
line_size  <- 1
font_family <- "Times New Roman"

plot_toas <- ggplot(dt_toas, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_toas %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_toas %>% filter(bucket_mid > 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "#14676B", size = line_size) +
  scale_x_continuous(
    labels = comma,
    name = "Sum of Total Assets (Centered at Threshold)"
  ) +
  scale_y_continuous(
    labels = percent_format(accuracy = 1),
    name = "Share with Multiple Entities"
  ) +
  theme_minimal(base_family = font_family) +
  theme(
    axis.text = element_text(size = text_size),
    axis.title = element_text(size = text_size),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10))
  )
ggsave("output/plot_toas.png")

##################
# Plot: Sum of Employees
##################

# Get buckets
bucket_size_employees <- (upper_limit_employees - lower_limit_employees) / 20
dt$emp_centered <- dt$sum_employees - cutoff_employees
dt$emp_bucket <- cut(dt$emp_centered, 
                     breaks = seq(-0.3 * cutoff_employees, 0.3 * cutoff_employees, by = bucket_size_employees), 
                     include.lowest = TRUE)



# Aggregate by bucket
dt_emp <- dt %>%
  group_by(emp_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(emp_centered, na.rm = TRUE)) 


# Plot
text_size  <- 14
point_size <- 2
line_size  <- 1
font_family <- "Times New Roman"


plot_emp <- ggplot(dt_emp, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_emp %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_emp %>% filter(bucket_mid > 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "#14676B", size = line_size) +
  scale_x_continuous(
    labels = comma,
    name = "Sum of Employees (Centered at Threshold)"
  ) +
  scale_y_continuous(
    labels = percent_format(accuracy = 1),
    name = "Share with Multiple Entities"
  ) +
  theme_minimal(base_family = font_family) +
  theme(
    axis.text = element_text(size = text_size),
    axis.title = element_text(size = text_size),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10))
  )
ggsave("output/plot_emp.png")



combined_plot <- plot_toas / plot_emp + 
  plot_annotation(tag_levels = "A")

# Save combined plot
ggsave("output/combined_plot.png", combined_plot, width = 8, height = 10, dpi = 300)


##################
# Regression 1
##################


dt$above_toas_thresh <- dt$sum_total_assets > 6000000
dt$above_emp_thresh <- dt$sum_employees > 50
dt$above_both <- dt$above_toas_thresh & dt$above_emp_thresh

m1 <- list(
  fixest::feols(
    fml = multiple ~ sum_total_assets, data = dt),
  fixest::feols(
    fml = multiple ~ sum_employees, data = dt),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees, data = dt),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh, data = dt),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_emp_thresh, data = dt),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh + above_emp_thresh, data = dt),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_both, data = dt)
)

modelsummary(
  dvnames(m1),
  stars = TRUE
)



##################
# Regression 2
##################


# Delete singletons as suggested by Breuer & deHaan (2024)
singleton <- dt %>% 
  group_by(owner) %>%
  summarise(n = n()) %>%
  filter(n == 1)
dt_2 <- dt[!dt$owner %in% singleton$owner]

# Define some variables
dt_2$above_toas_thresh <- dt_2$sum_total_assets > 6000000
dt_2$above_emp_thresh <- dt_2$sum_employees > 50
dt_2$above_both <- dt_2$above_toas_thresh & dt_2$above_emp_thresh

# Get number of observation with within FE variation
within_fe_variation <- dt_2 %>%
  group_by(owner) %>%
  summarise(multiple = length(unique(multiple)) > 1,
            asset_tresh = length(unique(above_toas_thresh)) > 1,
            emp_tresh = length(unique(above_emp_thresh)) > 1,
            both_tresh = length(unique(above_both)) > 1)

sum(within_fe_variation$multiple)
sum(within_fe_variation$asset_tresh)
sum(within_fe_variation$emp_tresh)
sum(within_fe_variation$both_tresh)


m2 <- list(
  fixest::feols(
    fml = multiple ~ sum_total_assets | owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_employees| owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees| owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh| owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_emp_thresh| owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh + above_emp_thresh| owner, data = dt_2),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_both| owner, data = dt_2)
)

modelsummary(
  dvnames(m2),
  stars = TRUE
)






