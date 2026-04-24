## Last updated: April 23rd, 2026
## By David Rios
# ──────────────────────────────────────────
# This cleaning script produces two Master files:
# 1. tech_2018_clean (Detailed 2018 AI data)
# 2. cs_panel_clean  (Multi-year 2017-2021 payroll data)

library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(readr)
library(here)

# Load raw data
tech_2018_raw <- read_csv(here::here("data", "tech_2018_raw.csv"))
cs_panel_raw  <- read_csv(here::here("data", "cs_panel_raw.csv"))

# Clean technology data
tech_2018_clean <- tech_2018_raw |>
  filter(TECHUSE_LABEL != "All firms") |>
  rename(naics_sector = NAICS2017) |>
  mutate(
    tech_type = str_to_lower(str_trim(str_extract(TECHUSE_LABEL, "^[^:]+"))),
    use_level = str_extract(TECHUSE_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP, RCPPDEMP, PAYANN), as.numeric),
    state = as.character(state)
  ) |>
  filter(!use_level %in% c("Total Reporting", "Total use")) |>
  mutate(
    used_tech = case_when(
      use_level %in% c("High use", "Moderate use", "Low use") ~ "Used",
      use_level == "Did not use" ~ "Did not use",
      use_level == "Tested, but did not use in production or service" ~ "Tested only",
      TRUE ~ "Unknown"
    ),
    # Standardize NAICS ranges
    naics_sector = case_when(
      naics_sector %in% c("31","32","33","31-33") ~ "31-33",
      naics_sector %in% c("44","45","44-45") ~ "44-45",
      naics_sector %in% c("48","49","48-49") ~ "48-49",
      TRUE ~ naics_sector
    )
  ) |>
  select(year, state, naics_sector, tech_type, use_level, used_tech,
         n_firms = FIRMPDEMP, n_emp = EMP, revenue = RCPPDEMP, payroll = PAYANN)

# cleaning industries
tech_2018_clean <- tech_2018_clean |>
  mutate(
    naics_label = case_when(
      naics_sector == "00"    ~ "Total",
      naics_sector == "11"    ~ "Agriculture",
      naics_sector == "21"    ~ "Mining",
      naics_sector == "22"    ~ "Utilities",
      naics_sector == "23"    ~ "Construction",
      naics_sector == "31-33" ~ "Manufacturing",
      naics_sector == "42"    ~ "Wholesale Trade",
      naics_sector == "44-45" ~ "Retail Trade",
      naics_sector == "48-49" ~ "Transportation",
      naics_sector == "51"    ~ "Information",
      naics_sector == "52"    ~ "Finance and Insurance",
      naics_sector == "53"    ~ "Real Estate",
      naics_sector == "54"    ~ "Professional Services",
      naics_sector == "55"    ~ "Management",
      naics_sector == "56"    ~ "Administrative Services",
      naics_sector == "61"    ~ "Educational Services",
      naics_sector == "62"    ~ "Health Care",
      naics_sector == "71"    ~ "Arts and Recreation",
      naics_sector == "72"    ~ "Food Services",
      naics_sector == "81"    ~ "Other Services",
      naics_sector == "99"    ~ "Unknown",
      TRUE                    ~ "Other"),
    naics_label = as.factor(naics_label))

#  AI-specific summary (Cross-section for merging)
ai_adoption_2018 <- tech_2018_clean |>
  filter(tech_type == "artificial intelligence", naics_sector != "00") |>
  group_by(state, naics_sector) |>
  summarise(
    ai_firms_any_use = sum(n_firms[used_tech == "Used"], na.rm = TRUE),
    ai_firms_total   = sum(n_firms[used_tech %in% c("Used", "Did not use")], na.rm = TRUE),
    ai_adoption_rate = if_else(ai_firms_total > 0, ai_firms_any_use / ai_firms_total, NA_real_),
    .groups = "drop"
  )

# company summary panel (2017-2021)
cs_panel_clean <- cs_panel_raw |>
  clean_names() |>
  rename(naics_sector = naics2017) |>
  mutate(
    state = as.character(state),
    naics_sector = as.character(naics_sector),
    # Standardize payroll to raw Dollars for easier plotting
    payroll_total_usd = as.numeric(payann) * 1000,
    n_emp = as.numeric(emp),
    n_firms = as.numeric(firmpdemp),
    payroll_per_emp = if_else(n_emp > 0, payroll_total_usd / n_emp, NA_real_)
  ) |>
  filter(n_firms > 0) |>
  select(-firmpdemp, -emp)

cs_panel_aggregate_clean <- cs_panel_clean |>
  filter(str_length(naics_sector) == 2,
         !naics_sector %in% c("00", "99")) |>
  rename(revenue = rcppdemp,
         payroll = payann) |>
  mutate(naics_label = case_when(
      naics_sector == "11"    ~ "Agriculture",
      naics_sector == "21"    ~ "Mining",
      naics_sector == "22"    ~ "Utilities",
      naics_sector == "23"    ~ "Construction",
      naics_sector == "31-33" ~ "Manufacturing",
      naics_sector == "42"    ~ "Wholesale Trade",
      naics_sector == "44-45" ~ "Retail Trade",
      naics_sector == "48-49" ~ "Transportation",
      naics_sector == "51"    ~ "Information",
      naics_sector == "52"    ~ "Finance and Insurance",
      naics_sector == "53"    ~ "Real Estate",
      naics_sector == "54"    ~ "Professional Services",
      naics_sector == "55"    ~ "Management",
      naics_sector == "56"    ~ "Administrative Services",
      naics_sector == "61"    ~ "Educational Services",
      naics_sector == "62"    ~ "Health Care",
      naics_sector == "71"    ~ "Arts and Recreation",
      naics_sector == "72"    ~ "Food Services",
      naics_sector == "81"    ~ "Other Services",
      TRUE                    ~ "Other"
    ),
    naics_label = as.factor(naics_label)
  )


# Save Master Files
write_csv(tech_2018_clean, here::here("data", "tech_2018_clean.csv"))
write_csv(cs_panel_clean,  here::here("data", "cs_panel_clean.csv"))
