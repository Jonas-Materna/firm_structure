

###########################
# Load packages 
###########################

library(dplyr)
library(ggplot2)
library(scales)
library(extrafont)
loadfonts(device = "win")

###########################
# Load data and restrict sample to owners close to the threshold
###########################

# Load data of entities 
dt_entities  <- readRDS(paste0("data/generated/sample_data/DE_sample.rds"))

# Load aggregated owner data
dt_owner <- readRDS("data/generated/aggregated_data/DE_aggregated_data.rds")

# Merge
dt_entities <- left_join(dt_entities, dt_owner, by = c("owner", "year"))




##################
# Plot: Total Assets
##################

cutoff_assets <- 6000000

lower_limit_total_assets <- 0.7 * cutoff_assets
upper_limit_total_assets <- 1.3 * cutoff_assets

# Filter data within bandwidths
dt <- dt_entities %>%
  filter(between(total_assets, lower_limit_total_assets, upper_limit_total_assets))

# Get owners with multiple firms
dt$multiple <- dt$n_entities > 1


# Get buckets
bucket_size_assets <- (upper_limit_total_assets - lower_limit_total_assets) / 20
dt$toas_centered <- dt$total_assets - cutoff_assets
dt$toas_bucket <- cut(dt$toas_centered, 
                      breaks = seq(-0.3 * cutoff_assets, 0.3 * cutoff_assets, by = bucket_size_assets), 
                      include.lowest = TRUE)

bucket_breaks <- seq(-0.3 * cutoff_assets, 0.3 * cutoff_assets, by = bucket_size_assets)
bucket_mids <- head(bucket_breaks, -1) + diff(bucket_breaks) / 2

dt$toas_bucket <- cut(dt$toas_centered, breaks = bucket_breaks, include.lowest = TRUE)
dt$bucket_mid <- as.numeric(dt$toas_bucket)
levels(dt$toas_bucket) <- bucket_mids  
dt$bucket_mid <- as.numeric(as.character(dt$toas_bucket))  

# Aggregate by bucket
dt_plot <- dt %>%
  filter(!is.na(bucket_mid)) %>%
  group_by(bucket_mid, multiple) %>%
  summarise(is_share = mean(!is.na(income)), .groups = "drop")

# Make Plot

left_break   <- -1e6
center_break <- 0
right_break  <- 1e6

text_size  <- 14
point_size <- 2
line_size  <- 1

options(scipen = 999)


plot_is_share <- ggplot(dt_plot, aes(x = bucket_mid, y = is_share, fill = multiple)) +
  geom_bar(stat = "identity", position = position_dodge(), width = bucket_size_assets * 0.9) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "#14676B", size = line_size) +
  scale_fill_manual(values = c("FALSE" = "#AAAAAA", "TRUE" = "#14676B")) +
  scale_x_continuous(
    breaks = c(left_break, center_break, right_break),
    labels = scales::comma,
    name = "Total Assets (Centered at Threshold)"
  ) +
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 1),
    name = "Share of Entities Disclosing an Income Statement"
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text = element_text(size = text_size),
    axis.title = element_text(size = text_size),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    legend.position = "bottom"
  ) +
  guides(fill = guide_legend(title = "Organized in Multiple Entities"))
plot_is_share 
ggsave("output/plot_is_share.png")
