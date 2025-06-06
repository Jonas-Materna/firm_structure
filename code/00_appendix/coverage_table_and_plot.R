
library(tidyr)
library(dplyr)
library(ggplot2)
library(extrafont)
loadfonts(device = "win")



##############################
# Generate Table
##############################

# Get all files 
files <- list.files(path = "data/generated/owner_data")
countries <- substr(files, 1, 2)

output <- data.frame()
for(ctry in countries){
  
  # Load data
  dt <- read.csv(paste0("data/generated/owner_coverage/", ctry, "_owner_coverage.csv"))
  
  
  # Make sure to always have all years
  years <- data.frame(year = c(2007:2024))
  dt <- left_join(years, dt, by = "year")

  # Tanspose it
  dt <- dt %>%
    select(year, share) %>%
    mutate(ctry = ctry) %>%
    pivot_wider(names_from = year, values_from = share)

  
  
  # Append output
  output <- rbind(output, dt)
  message(paste0("Done: ", ctry))
}

# Clean and save
output[is.na(output)] <- 0
output$mean <- rowMeans(output[ , -1], na.rm = TRUE)
write.csv(output, paste0("output/coverage_by_country.csv"), row.names = FALSE)



##############################
# Make a plot fr Germany
##############################

# Load data
dt <- read.csv(paste0("data/generated/owner_coverage/DE_owner_coverage.csv"))
dt <- dt %>% filter(year < 2024)

text_size  <- 14
point_size <- 2
line_size  <- 1

coverage_plot <- ggplot(data = dt, aes(x = year, y = share)) +
  geom_point(color = "#14676B", alpha = 0.7, size = point_size) +
  geom_vline(xintercept = 2015, linetype = "dashed", color = "#6F354B", size = line_size) +
  geom_vline(xintercept = 2021, linetype = "dashed", color = "#6F354B", size = line_size) +
  annotate("text", x = 2015, y = 0.1, label = "Start of Sample Period", 
           angle = 90, vjust = -2, size = 4, family = "Times New Roman", color = "#6F354B") +
  annotate("text", x = 2021, y = 0.1, label = "End of Sample Period", 
           angle = 90, vjust = -2, size = 4, family = "Times New Roman", color = "#6F354B") +
  expand_limits(y = 0) +
  labs(
    x = "Year",
    y = "Share of Entities with Available Owner Data Over Time",
    title = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text = element_text(size = text_size, family = "Times New Roman"),
    axis.title = element_text(size = text_size, family = "Times New Roman")
  )
ggsave("output/coverage_plot.png")
