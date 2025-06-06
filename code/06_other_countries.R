
##################
# Load packages
##################
library(dplyr)
library(DBI)
library(ggplot2)
library(ggpubr)
library(patchwork)
library(modelsummary)
library(scales)
library(extrafont)
loadfonts(device = "win")




# Plot specification
text_size  <- 14
point_size <- 2
line_size  <- 1
font_family <- "Times New Roman"


################################################
# Luxembourg
################################################

##################
# Prep data
##################

# Load aggregated owner data
dt_lu_owner <- readRDS("data/generated/aggregated_data/LU_aggregated_data.rds")

cutoff_assets <- 4400000
cutoff_employees <- 50

# Define bandwidt_aths
lower_limit_employees <- 0.7 * cutoff_employees
upper_limit_employees <- 1.3 * cutoff_employees

lower_limit_total_assets <- 0.7 * cutoff_assets
upper_limit_total_assets <- 1.3 * cutoff_assets

# Filter data within bandwidths
dt_lu <- dt_lu_owner %>%
  filter(between(sum_employees, lower_limit_employees, upper_limit_employees)) %>%
  filter(between(sum_total_assets, lower_limit_total_assets, upper_limit_total_assets))

# Discard due to few observations



################################################
# Denmark
################################################

# Discard due to change in threshold

################################################
# Austria
################################################

##################
# Prep data
##################

# Load aggregated owner data
dt_at_owner <- readRDS("data/generated/aggregated_data/AT_aggregated_data.rds")

cutoff_assets <- 5000000
cutoff_employees <- 50

# Define bandwidt_aths
lower_limit_employees <- 0.7 * cutoff_employees
upper_limit_employees <- 1.3 * cutoff_employees

lower_limit_total_assets <- 0.7 * cutoff_assets
upper_limit_total_assets <- 1.3 * cutoff_assets

# Filter data within bandwidths
dt_at <- dt_at_owner %>%
  filter(between(sum_employees, lower_limit_employees, upper_limit_employees)) %>%
  filter(between(sum_total_assets, lower_limit_total_assets, upper_limit_total_assets))

# Get owners with multiple firms
dt_at$multiple <- dt_at$n_entities > 1



# Load data of entities owned by those owners
dt_at_entities  <- readRDS(paste0("data/generated/sample_data/AT_sample.rds"))

dt_at_entities <- left_join(dt_at_entities, dt_at, by = c("owner", "year"))

dt_at_entities <- dt_at_entities %>% 
  filter(!is.na(n_entities)) %>% 
  select(bvd_id, year, total_assets, employees, income, owner, n_entities )




##################
# Plot: Sum of Total Assets
##################

# Get buckets
bucket_size_assets <- (upper_limit_total_assets - lower_limit_total_assets) / 20
dt_at$toas_centered <- dt_at$sum_total_assets - cutoff_assets
dt_at$toas_bucket <- cut(dt_at$toas_centered, 
                      breaks = seq(-0.3 * cutoff_assets, 0.3 * cutoff_assets, by = bucket_size_assets), 
                      include.lowest = TRUE)



# Aggregate by bucket
dt_at_toas <- dt_at %>%
  group_by(toas_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(toas_centered, na.rm = TRUE)) 


plot_toas_at <- ggplot(dt_at_toas, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_at_toas %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_at_toas %>% filter(bucket_mid > 0),
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







##################
# Plot: Sum of Employees
##################

# Get buckets
bucket_size_employees <- (upper_limit_employees - lower_limit_employees) / 20
dt_at$emp_centered <- dt_at$sum_employees - cutoff_employees
dt_at$emp_bucket <- cut(dt_at$emp_centered, 
                     breaks = seq(-0.3 * cutoff_employees, 0.3 * cutoff_employees, by = bucket_size_employees), 
                     include.lowest = TRUE)



# Aggregate by bucket
dt_at_emp <- dt_at %>%
  group_by(emp_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(emp_centered, na.rm = TRUE)) 



plot_emp_at <- ggplot(dt_at_emp, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_at_emp %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_at_emp %>% filter(bucket_mid > 0),
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





combined_plot_at <- plot_toas_at / plot_emp_at + 
  plot_annotation(
    title = "Austria",
    theme = theme(
      plot.title = element_text(
        family = "Times New Roman", 
        size = 16, 
        face = "bold", 
        hjust = 0.5
      )
    ),
    tag_levels = "A"
  )

# Save combined plot
ggsave("output/combined_plot_at.png", combined_plot_at, width = 8, height = 10, dpi = 300)




################################################
# Latvia
################################################



##################
# Prep data
##################

# Load aggregated owner data
dt_lv_owner <- readRDS("data/generated/aggregated_data/LV_aggregated_data.rds")

cutoff_assets <- 800000
cutoff_employees <- 50

# Define bandwidths
lower_limit_employees <- 0.7 * cutoff_employees
upper_limit_employees <- 1.3 * cutoff_employees

lower_limit_total_assets <- 0.7 * cutoff_assets
upper_limit_total_assets <- 1.3 * cutoff_assets

# Filter data within bandwidths
dt_lv <- dt_lv_owner %>%
  filter(between(sum_employees, lower_limit_employees, upper_limit_employees)) %>%
  filter(between(sum_total_assets, lower_limit_total_assets, upper_limit_total_assets))

# Get owners with multiple firms
dt_lv$multiple <- dt_lv$n_entities > 1



# Load data of entities owned by those owners
dt_lv_entities  <- readRDS(paste0("data/generated/sample_data/LV_sample.rds"))

dt_lv_entities <- left_join(dt_lv_entities, dt_lv, by = c("owner", "year"))

dt_lv_entities <- dt_lv_entities %>% 
  filter(!is.na(n_entities)) %>% 
  select(bvd_id, year, total_assets, employees, income, owner, n_entities )





##################
# Plot: Sum of Total Assets
##################

# Get buckets
bucket_size_assets <- (upper_limit_total_assets - lower_limit_total_assets) / 20
dt_lv$toas_centered <- dt_lv$sum_total_assets - cutoff_assets
dt_lv$toas_bucket <- cut(dt_lv$toas_centered, 
                      breaks = seq(-0.3 * cutoff_assets, 0.3 * cutoff_assets, by = bucket_size_assets), 
                      include.lowest = TRUE)



# Aggregate by bucket
dt_lv_toas <- dt_lv %>%
  group_by(toas_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(toas_centered, na.rm = TRUE)) 


plot_toas_lv <- ggplot(dt_lv_toas, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_lv_toas %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_lv_toas %>% filter(bucket_mid > 0),
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





##################
# Plot: Sum of Employees
##################

# Get buckets
bucket_size_employees <- (upper_limit_employees - lower_limit_employees) / 20
dt_lv$emp_centered <- dt_lv$sum_employees - cutoff_employees
dt_lv$emp_bucket <- cut(dt_lv$emp_centered, 
                     breaks = seq(-0.3 * cutoff_employees, 0.3 * cutoff_employees, by = bucket_size_employees), 
                     include.lowest = TRUE)



# Aggregate by bucket
dt_lv_emp <- dt_lv %>%
  group_by(emp_bucket) %>%
  summarise(mean_entities = mean(n_entities),
            share_multiple = mean(multiple),
            n = n(),
            bucket_mid = mean(emp_centered, na.rm = TRUE)) 


plot_emp_lv <- ggplot(dt_lv_emp, aes(x = bucket_mid, y = share_multiple)) +
  geom_point(size = point_size, color = "#14676B", alpha = 0.7) +
  geom_smooth(data = dt_lv_emp %>% filter(bucket_mid <= 0),
              method = "lm", se = FALSE, color = "#14676B", size = line_size) +
  geom_smooth(data = dt_lv_emp %>% filter(bucket_mid > 0),
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





# Combine plots

combined_plot_at <- plot_toas_at / plot_emp_at + 
  plot_annotation(
    title = "Austria",
    theme = theme(
      plot.title = element_text(
        family = "Times New Roman", 
        size = 16, 
        face = "bold", 
        hjust = 0.5
      )
    ),
    tag_levels = "A"
  )

# Save combined plot
ggsave("output/combined_plot_at.png", combined_plot_at, width = 8, height = 10, dpi = 300)


combined_plot_lv <- plot_toas_lv / plot_emp_lv + 
  plot_annotation(
    title = "Latvia",
    theme = theme(
      plot.title = element_text(
        family = "Times New Roman", 
        size = 16, 
        face = "bold", 
        hjust = 0.5
      )
    ),
    tag_levels = "A"
  )

# Save combined plot
ggsave("output/combined_plot_lv.png", combined_plot_lv, width = 8, height = 10, dpi = 300)





###############################
# Regressions
###############################

# Prep Austria
dt_at$above_toas_thresh <- dt_at$sum_total_assets > 5000000
dt_at$above_emp_thresh <- dt_at$sum_employees > 50
dt_at$above_both <- dt_at$above_toas_thresh & dt_at$above_emp_thresh

# Prep Latvia
dt_lv$above_toas_thresh <- dt_lv$sum_total_assets > 800000
dt_lv$above_emp_thresh <- dt_lv$sum_employees > 50
dt_lv$above_both <- dt_lv$above_toas_thresh & dt_lv$above_emp_thresh


m1 <- list(
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh, data = dt_at),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_emp_thresh, data = dt_at),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_both, data = dt_at), 
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_toas_thresh, data = dt_lv),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_emp_thresh, data = dt_lv),
  fixest::feols(
    fml = multiple ~ sum_total_assets + sum_employees + above_both, data = dt_lv)
)

modelsummary(
  dvnames(m1),
  stars = TRUE
)

