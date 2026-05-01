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

# ──────────────────────────────────────────
### Company Summary Panel for the years 2017-2021
# ──────────────────────────────────────────
cs_panel_clean <- cs_panel_raw |>
  filter(
    !NAICS2017 %in% c("00", "99"),
    (nchar(as.character(NAICS2017)) == 2) | (NAICS2017 %in% c("31-33", "44-45", "48-49"))) |>
  mutate(across(c(EMP, FIRMPDEMP, PAYANN, RCPPDEMP),
                ~ as.numeric(gsub("[^0-9.-]", "", .)))) |>
  mutate(
    naics_sector = case_when(
      NAICS2017 %in% c("31", "32", "33", "31-33") ~ "31-33",
      NAICS2017 %in% c("44", "45", "44-45") ~ "44-45",
      NAICS2017 %in% c("48", "49", "48-49") ~ "48-49",
      TRUE ~ as.character(NAICS2017)
    ),
    naics_label = case_when(
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
      TRUE                    ~ "Other")) |>
  mutate(state_label = case_when(
    state == "01"      ~ "Alabama",
    state == "02"      ~ "Alaska",
    state == "04"      ~ "Arizona",
    state == "05"      ~ "Arkansas",
    state == "06"      ~ "California",
    state == "08"      ~ "Colorado",
    state == "09"      ~ "Connecticut",
    state == "10"      ~ "Delaware",
    state == "11"      ~ "District of Columbia",
    state == "12"      ~ "Florida",
    state == "13"      ~ "Georgia",
    state == "15"      ~ "Hawaii",
    state == "16"      ~ "Idaho",
    state == "17"      ~ "Illinois",
    state == "18"      ~ "Indiana",
    state == "19"      ~ "Iowa",
    state == "20"      ~ "Kansas",
    state == "21"      ~ "Kentucky",
    state == "22"      ~ "Louisiana",
    state == "23"      ~ "Maine",
    state == "24"      ~ "Maryland",
    state == "25"      ~ "Massachusetts",
    state == "26"      ~ "Michigan",
    state == "27"      ~ "Minnesota",
    state == "28"      ~ "Mississippi",
    state == "29"      ~ "Missouri",
    state == "30"      ~ "Montana",
    state == "31"      ~ "Nebraska",
    state == "32"      ~ "Nevada",
    state == "33"      ~ "New Hampshire",
    state == "34"      ~ "New Jersey",
    state == "35"      ~ "New Mexico",
    state == "36"      ~ "New York",
    state == "37"      ~ "North Carolina",
    state == "38"      ~ "North Dakota",
    state == "39"      ~ "Ohio",
    state == "40"      ~ "Oklahoma",
    state == "41"      ~ "Oregon",
    state == "42"      ~ "Pennsylvania",
    state == "72"      ~ "Puerto Rico",
    state == "44"      ~ "Rhode Island",
    state == "45"      ~ "South Carolina",
    state == "46"      ~ "South Dakota",
    state == "47"      ~ "Tennessee",
    state == "48"      ~ "Texas",
    state == "49"      ~ "Utah",
    state == "50"      ~ "Vermont",
    state == "51"      ~ "Virginia",
    state == "78"      ~ "Virgin Islands",
    state == "53"      ~ "Washington",
    state == "54"      ~ "West Virginia",
    state == "55"      ~ "Wisconsin",
    state == "56"      ~ "Wyoming"
  )) |>
  group_by(year, state, state_label, naics_sector, naics_label) |>
  summarise(
    EMP = sum(EMP, na.rm = TRUE),
    FIRMPDEMP = sum(FIRMPDEMP, na.rm = TRUE),
    PAYANN = sum(PAYANN, na.rm = TRUE),
    RCPPDEMP = sum(RCPPDEMP, na.rm = TRUE),
    .groups = "drop") |>
  mutate(
    avg_wage = (PAYANN * 1000) / EMP,
    revenue_per_firm = if_else(FIRMPDEMP > 0, RCPPDEMP / FIRMPDEMP, NA_real_)) |>
  filter(EMP > 0)

## renaming some key vars
cs_panel_clean <- cs_panel_clean |>
  rename(
    n_firms = FIRMPDEMP,
    n_emp   = EMP,
    payroll = PAYANN,
    revenue = RCPPDEMP)

# ──────────────────────────────────────────
### 2. Clean Technology Data (2018 Only)
# ──────────────────────────────────────────

tech_2018_clean <- tech_2018_raw |>
  select(-RCPPDEMP, -TECHSELL, -TECHUSE, -IMPACTWF_U, -IMPACTWK_U, -FACTORS_U, -MOTUSETECH) |> # Drop the useless 0s
  rename(
    naics_sector = NAICS2017,
    n_firms = FIRMPDEMP,
    n_emp = EMP,
    payroll = PAYANN) |>
  mutate(across(c(n_firms, n_emp, payroll), ~as.numeric(gsub("[^0-9.-]", "", .)))) |>
  mutate(across(c(n_firms, n_emp, payroll), ~na_if(., 0))) |> # Treat 0s as NA (Privacy suppression)
  mutate(
    active_label = case_when(
      TECHUSE_LABEL != "All firms" ~ TECHUSE_LABEL,
      TECHSELL_LABEL != "All firms" ~ TECHSELL_LABEL,
      IMPACTWF_U_LABEL != "All firms" ~ IMPACTWF_U_LABEL,
      IMPACTWK_U_LABEL != "All firms" ~ IMPACTWK_U_LABEL,
      FACTORS_U_LABEL != "All firms" ~ FACTORS_U_LABEL,
      MOTUSETECH_LABEL != "All firms" ~ MOTUSETECH_LABEL,
      TRUE ~ NA_character_),
    tech_type = str_to_lower(str_trim(str_extract(active_label, "^[^:]+"))),
    survey_response = str_trim(str_extract(active_label, "(?<=: ).+")),

    use_level = if_else(TECHUSE_LABEL != "All firms", survey_response, NA_character_),

    used_tech = case_when(
      use_level %in% c("High use", "Moderate use", "Low use") ~ "Used",
      use_level == "Did not use" ~ "Did not use",
      use_level == "Tested, but did not use in production or service" ~ "Tested",
      use_level == "Don't know" ~ "Unknown",
      is.na(use_level) ~ NA_character_,
      TRUE ~ NA_character_),

    naics_sector = case_when(
      naics_sector %in% c("31","32","33","31-33") ~ "31-33",
      naics_sector %in% c("44","45","44-45") ~ "44-45",
      naics_sector %in% c("48","49","48-49") ~ "48-49",
      TRUE ~ naics_sector
    ),
    state = as.character(state)
  ) |>
  filter(
    !is.na(active_label),
    !str_detect(survey_response, "^Total"), # Prevent double counting
    nchar(naics_sector) == 2 | naics_sector %in% c("31-33", "44-45", "48-49"),
    !naics_sector %in% c("00", "99")
  ) |>
  # Add labels back to the tech data
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
  )) |>

  # Add state labels to the tech data

  mutate(state_label = case_when(
    state == "01"      ~ "Alabama",
    state == "02"      ~ "Alaska",
    state == "04"      ~ "Arizona",
    state == "05"      ~ "Arkansas",
    state == "06"      ~ "California",
    state == "08"      ~ "Colorado",
    state == "09"      ~ "Connecticut",
    state == "10"      ~ "Delaware",
    state == "11"      ~ "District of Columbia",
    state == "12"      ~ "Florida",
    state == "13"      ~ "Georgia",
    state == "15"      ~ "Hawaii",
    state == "16"      ~ "Idaho",
    state == "17"      ~ "Illinois",
    state == "18"      ~ "Indiana",
    state == "19"      ~ "Iowa",
    state == "20"      ~ "Kansas",
    state == "21"      ~ "Kentucky",
    state == "22"      ~ "Louisiana",
    state == "23"      ~ "Maine",
    state == "24"      ~ "Maryland",
    state == "25"      ~ "Massachusetts",
    state == "26"      ~ "Michigan",
    state == "27"      ~ "Minnesota",
    state == "28"      ~ "Mississippi",
    state == "29"      ~ "Missouri",
    state == "30"      ~ "Montana",
    state == "31"      ~ "Nebraska",
    state == "32"      ~ "Nevada",
    state == "33"      ~ "New Hampshire",
    state == "34"      ~ "New Jersey",
    state == "35"      ~ "New Mexico",
    state == "36"      ~ "New York",
    state == "37"      ~ "North Carolina",
    state == "38"      ~ "North Dakota",
    state == "39"      ~ "Ohio",
    state == "40"      ~ "Oklahoma",
    state == "41"      ~ "Oregon",
    state == "42"      ~ "Pennsylvania",
    state == "72"      ~ "Puerto Rico",
    state == "44"      ~ "Rhode Island",
    state == "45"      ~ "South Carolina",
    state == "46"      ~ "South Dakota",
    state == "47"      ~ "Tennessee",
    state == "48"      ~ "Texas",
    state == "49"      ~ "Utah",
    state == "50"      ~ "Vermont",
    state == "51"      ~ "Virginia",
    state == "78"      ~ "Virgin Islands",
    state == "53"      ~ "Washington",
    state == "54"      ~ "West Virginia",
    state == "55"      ~ "Wisconsin",
    state == "56"      ~ "Wyoming"
  )) |>

  mutate(
    avg_wage = (payroll * 1000) / n_emp,
    emp_per_firm = n_emp / n_firms
  )




# ──────────────────────────────────────────
### Final Merge
# ──────────────────────────────────────────


# Save Master Files
write_csv(tech_2018_clean, here::here("data", "tech_2018_clean.csv"))
write_csv(cs_panel_clean,  here::here("data", "cs_panel_clean.csv"))






