# firm_structure: Strategic Fragmentation Around Regulatory Thresholds

This repository contains the replication package for the paper *Strategic Fragmentation Around Regulatory Thresholds*. The study investigates whether threshold-based regulation leads to more fragmented firm structures.

---

## Requirements

This code uses Orbis data (version: December 2024), accessible via the API provided by the TRR 266 *Accounting for Transparency* collaborative research center.

- If you are a TRR 266 member, please insert your API credentials by creating a `firm_structure.env` file. Use `_firm_structure.env` as a template.
- If you are not a member of TRR 266, it is advisable to access the required Orbis data via Moody’s SFTP delivery system.

A detailed list of required Orbis files can be found in:  
`code/01_pull_bvd_api_data.py`

---

## Setup

To run the code, you will need the following software installed:

- Python  
- R  

### Required R Packages

The analysis relies on the following R packages:

- `arrow`  
- `data.table`  
- `DBI`  
- `dplyr`  
- `duckdb`  
- `extrafont`  
- `fs`  
- `future.apply`  
- `ggplot2`  
- `ggpubr`  
- `glue`  
- `modelsummary`  
- `patchwork`  
- `purrr`  
- `scales`  
- `tibble`  

---

## Output and Workflow

To replicate the results, follow the scripts in the `code/` folder in chronological order:

1. **`01_pull_bvd_api_data.py`**  
   Downloads the required Orbis data from the TRR 266 API.

2. **`02a_prep_owner_data.R`**  
   Constructs a panel dataset of owners from historic, time-static ownership files.

3. **`02b_prep_financial_data.R`**  
   Filters the financial data, following the approach of Beuselinck et al. (2023).

4. **`03_sample_selection.R`**  
   Selects the final sample, as described in the paper's sample selection section.

5. **`04_aggregate_by_owner.R`**  
   Aggregates firm-level statistics (e.g., total assets, employees) at the owner level.

6. **`05a_descriptives.R`**  
   Produces descriptive statistics and summary tables.

7. **`05b_plot_regression_germany.R`**  
   Generates the main regression results for the German setting.

8. **`05c_disclosure_behavior.R`**  
   Plots the share of firms disclosing income statements around the thresholds.

9. **`05d_multiple_splits.R`**  
   Identifies and presents case studies of owners who split firms multiple times to stay below thresholds.

10. **`05e_cross_section_owner_named.R`**  
    Tests whether owners of real estate firms are more likely to avoid thresholds.

11. **`05f_cross_section_real_estate.R`**  
    Tests whether owners of owner named firms are more likely to avoid thresholds.

12. **`06_other_countries.R`**  
    Investigates whether similar avoidance strategies are used in other countries.

---
