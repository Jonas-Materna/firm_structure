
###########################
# Load packages
###########################
library(readxl)
library(ggplot2)
library(scales) 
library(extrafont)
loadfonts(device = "win")


###########################
# Load data
###########################

dt <- read_excel("data/thresholds/threshold_data.xlsx")


dt$`Total Assets` <- dt$`Total Assets`*as.numeric(dt$exchg)
dt <- dt[!is.na(dt$`Total Assets`),]
dt <- dt[order(dt$`Total Assets`),]
# lock in factor level order
dt$ctry_name <- factor(dt$ctry_name, levels = dt$ctry_name)



# Make Plot
text_size  <- 16
point_size <- 4
line_size  <- 1

threshold_plot <- ggplot(dt, aes(x = ctry_name, y = `Total Assets`, shape = `Threshold Type`, color = `Threshold Type`)) +
  geom_point(size = point_size, alpha = 0.7) +
  geom_hline(yintercept = 4000000, linetype = "dashed", color = "#6F354B", size = line_size) +
  geom_hline(yintercept = 6000000, linetype = "dashed", color = "#6F354B", size = line_size) +
  annotate("text", x = "Portugal", y = 6200000, label = "Maximum permitted threshold", 
           color = "#6F354B", size = 4, family = "Times New Roman") +
  annotate("text", x = "Portugal", y = 4200000, label = "Threshold in EU directive", 
           color = "#6F354B", size = 4, family = "Times New Roman") +
  scale_shape_manual(values = c(16, 17)) +
  scale_color_manual(values = c("#EC9007", "#14676B")) +
  scale_y_continuous(labels = comma) +  # This line prevents scientific notation
  labs(
    x = NULL,
    y = "Total Assets Thresholds in Euro\n",
    title = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1, size = text_size, family = "Times New Roman"),
    axis.text.y = element_text(size = text_size, family = "Times New Roman"),
    axis.title.y = element_text(size = text_size, family = "Times New Roman"),
    legend.text = element_text(size = text_size, family = "Times New Roman"),
    legend.title = element_blank(),
    legend.position = "bottom"
  )

threshold_plot
ggsave("output/threshold_plot.png")