## Last updated: April 16th, 2026
## By David Rios

# ───────────────────────────────────────────────────────

# This is our cleaning file

library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(readr)
library(here)

# Load raw data
tech_2018_raw <- readr::read_csv(
  here::here("data", "tech_2018_raw.csv"))

cs_panel_raw <- readr::read_csv(
  here::here("data", "cs_panel_raw.csv"))

#  Clean technology data
# The label columns follow the pattern "Technology Type: Description"
# We parse these into two clean columns

tech_2018_clean <- tech_2018_raw |>
  # drop the "All firms" aggregate rows — these are roll-ups, not categories
  filter(TECHUSE_LABEL != "All firms") |>
  # parse "Artificial Intelligence: High use" into two columns
  mutate(
    tech_type    = str_extract(TECHUSE_LABEL, "^[^:]+"),
    use_level    = str_extract(TECHUSE_LABEL, "(?<=: ).+"),
    # convert numeric columns
    across(c(FIRMPDEMP, EMP, RCPPDEMP, PAYANN), as.numeric),
    # recode state to state name later if needed
    state = as.character(state)
  ) |>
  # keep only the core use-level rows (drop Total Reporting/Total use —
  # these are just sums and will cause double-counting)
  filter(!use_level %in% c("Total Reporting", "Total use")) |>
  # create a binary: any use = High + Moderate + Low (not "Did not use")
  mutate(
    used_tech = case_when(
      use_level %in% c("High use", "Moderate use", "Low use") ~ "Used",
      use_level == "Did not use"                               ~ "Did not use",
      use_level == "Tested, but did not use in production or service" ~ "Tested only",
      use_level == "Don't know"                                ~ "Unknown",
      TRUE                                                     ~ NA_character_
    )
  ) |>
  select(
    year, state, NAICS2017,
    tech_type, use_level, used_tech,
    n_firms  = FIRMPDEMP,
    n_emp    = EMP,
    revenue  = RCPPDEMP,
    payroll  = PAYANN
  )

glimpse(tech_2018_clean)

## 2. Create AI-specific summary by state × NAICS

# This gives us an AI adoption profile for each state-industry cell
ai_adoption_2018 <- tech_2018_clean |>
  filter(tech_type == "Artificial Intelligence") |>
  # pivot use levels wide so each level is its own column
  select(state, NAICS2017, use_level, n_firms) |>
  pivot_wider(
    names_from  = use_level,
    values_from = n_firms,
    names_prefix = "ai_firms_"
  ) |>
  clean_names() |>
  # compute total firms reporting and adoption rate
  mutate(
    ai_firms_any_use = rowSums(
      across(c(ai_firms_high_use, ai_firms_moderate_use, ai_firms_low_use)),
      na.rm = TRUE
    ),
    ai_firms_total = rowSums(
      across(starts_with("ai_firms_")),
      na.rm = TRUE
    ),
    ai_adoption_rate = ai_firms_any_use / ai_firms_total
  )

glimpse(ai_adoption_2018)

# 3. Create comparison table: all tech types by state
tech_adoption_summary <- tech_2018_clean |>
  filter(used_tech == "Used") |>
  group_by(state, NAICS2017, tech_type) |>
  summarise(
    firms_using = sum(n_firms, na.rm = TRUE),
    emp_using   = sum(n_emp,   na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_wider(
    names_from  = tech_type,
    values_from = c(firms_using, emp_using),
    names_sep   = "_"
  ) |>
  clean_names()

glimpse(tech_adoption_summary)

## 3b KEY: to add more dimension to our study, we are obtaining smaller data sets from the raw

## extract each dimension from tech_2018_clean

# Why did firms adopt AI? (motivations)
ai_motivations <- tech_2018_raw |>
  filter(
    MOTUSETECH_LABEL != "All firms",
    str_starts(MOTUSETECH_LABEL, "Artificial Intelligence")
  ) |>
  mutate(
    motivation = str_extract(MOTUSETECH_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, NAICS2017, motivation, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(motivation, "Total Reporting"))

#  What barriers did firms face?
ai_barriers <- tech_2018_raw |>
  filter(
    FACTORS_U_LABEL != "All firms",
    str_starts(FACTORS_U_LABEL, "Artificial Intelligence")
  ) |>
  mutate(
    barrier = str_extract(FACTORS_U_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, NAICS2017, barrier, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(barrier, "Total Reporting"))

# How did AI impact the workforce?
ai_workforce_impact <- tech_2018_raw |>
  filter(
    IMPACTWF_U_LABEL != "All firms",
    str_starts(IMPACTWF_U_LABEL, "Artificial Intelligence")
  ) |>
  mutate(
    impact = str_extract(IMPACTWF_U_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, NAICS2017, impact, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(impact, "Total Reporting"))

# Did firms sell/produce AI?
ai_production <- tech_2018_raw |>
  filter(
    TECHSELL_LABEL != "All firms",
    str_starts(TECHSELL_LABEL, "Artificial Intelligence")
  ) |>
  mutate(
    produced = str_extract(TECHSELL_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, NAICS2017, produced, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(produced, "Total Reporting"))

# saving all documents as .csv
## had to change them cuz of a relative path issue.

# write_csv(ai_motivations,      "data/ai_motivations.csv")
# write_csv(ai_barriers,         "data/ai_barriers.csv")
# write_csv(ai_workforce_impact, "data/ai_workforce_impact.csv")
# write_csv(ai_production,       "data/ai_production.csv")

write_csv(ai_motivations, here::here("data", "ai_motivations.csv"))
write_csv(ai_barriers, here::here("data", "ai_barriers.csv"))
write_csv(ai_workforce_impact, here::here("data", "ai_workforce_impact.csv"))
write_csv(ai_production, here::here("data", "ai_production.csv"))



# Clean company summary panel
cs_panel_clean <- cs_panel_raw |>
  mutate(
    across(c(FIRMPDEMP, EMP, PAYANN, RCPPDEMP), as.numeric),
    state     = as.character(state),
    NAICS2017 = as.character(NAICS2017),
    year      = as.integer(year)
  ) |>
  filter(!(FIRMPDEMP == 0 & EMP == 0 & PAYANN == 0)) |>
  rename(
    n_firms = FIRMPDEMP,
    n_emp   = EMP,
    payroll = PAYANN,
    revenue = RCPPDEMP
  ) |>
  mutate(
    payroll_per_emp = if_else(n_emp > 0, payroll / n_emp, NA_real_)
  ) |>
  filter(nchar(NAICS2017) <= 2)   # keeping only 2-digit NAICS for clean merge

glimpse(cs_panel_clean)
count(cs_panel_clean, year)

# Lastly!!! Merging. Attach 2018 AI snapshot to multi-year panel
merged_panel <- cs_panel_clean |>
  left_join(
    ai_adoption_2018 |> rename(NAICS2017 = naics2017),
    by = c("state", "NAICS2017")
  ) |>
  mutate(
    high_ai = if_else(
      ai_adoption_rate >= median(ai_adoption_rate, na.rm = TRUE),
      "High AI adoption",
      "Low AI adoption"
    )
  )

glimpse(merged_panel)

# NAs should be gone or minimal now as we changed to 2-digits NAICS
## I think how they work is XX-XXXX, where the first two are agg industries (i.e. Healthcare)
merged_panel |> count(year, high_ai)

merged_panel |>
  group_by(year, high_ai) |>
  summarise(
    total_firms         = sum(n_firms, na.rm = TRUE),
    total_emp           = sum(n_emp,   na.rm = TRUE),
    avg_payroll_per_emp = mean(payroll_per_emp, na.rm = TRUE),
    .groups = "drop"
  )

# Saving cleaned data
#
# write_csv(tech_2018_clean,       "data/tech_2018_clean.csv")
# write_csv(ai_adoption_2018,      "data/ai_adoption_2018.csv")
# write_csv(tech_adoption_summary, "data/tech_adoption_summary.csv")
# write_csv(cs_panel_clean,        "data/cs_panel_clean.csv")
# write_csv(merged_panel,          "data/merged_panel.csv")



write_csv(tech_2018_clean, here::here("data", "tech_2018_clean.csv"))
write_csv(ai_adoption_2018, here::here("data", "ai_adoption_2018.csv"))
write_csv(tech_adoption_summary, here::here("data", "tech_adoption_summary.csv"))
write_csv(cs_panel_clean, here::here("data", "cs_panel_clean.csv"))
write_csv(merged_panel, here::here("data", "merged_panel.csv"))







