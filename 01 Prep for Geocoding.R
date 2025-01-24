## AUTHORS
# Lauren Woyczynski
# Lucas Owen
# Isaiah Wright
# Jessica Godwin

# Setup ####

## setwd() ####
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

## libPaths ####

#pull packages from the site_libs - also install packages here
.libPaths('R:/Project/UWED/Master Team Folder/Code/site_libs/')

## Packages ####
pacman::p_load(tidyverse, dplyr, readr, reticulate, parallel, sf, data.table, flextable, tidycensus,  haven, arsenal,tidycensus)
library(dplyr)
library(readr)
library(reticulate)
library(parallel)
library(sf)
library(tidyverse)
library(data.table)

## Source ####
source("00_helper_fns.R")

# Load data ####

## Issuances ####
# List all the .txt files in the Ballot_Issuances_Cures folder
# Specify the file path - only read in new files

issuances_path <- paste0("R:/Project/UWED/Master Team Folder/Data/Inputs/",
                         "SoS/Ballot_Issuances/")

old_files <- read_csv(paste0(issuances_path, "current_file_names.csv"))
issuances_txt_files <- list.files(issuances_path, pattern = "\\.txt$", full.names = TRUE)
new_files <- issuances_txt_files[!issuances_txt_files %in% old_files$issuances_txt_files]

## Initialize an empty list to store the data frames
issuances_list <- list()

## Loop over the file names
for (i in 1:length(new_files)) {
    # Get delimiter
    delim_guessed <- guessDelimiter(new_files[i])
    # Read the data from the file and create data frame
    issuances_df <- read.table(new_files[i], sep=delim_guessed, header=T,
                               na.strings = "", fill=T, quote = "")
    # issuances_df2 <- read.table(new_files[i], sep="|", header=T, na.strings = "", fill=T, quote = "", comment.char = "")

    # Add the data frame to the list
    issuances_list[[i]] <- issuances_df
}

## Convert every variable in each data frame to a character
issuances_list <- lapply(issuances_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

## rbind all data frames in the list
final_issuances <- do.call(rbind.data.frame, issuances_list)

## Columns we want to keep
col_names_keep <- c("Address", "City", "State", "Zip")

final_issuances <- select(final_issuances, all_of(col_names_keep))
final_issuances <- final_issuances %>%
    unite(col = "Address", Address, City, State, Zip, sep = ', ') %>%
    select(Address)
final_issuances <- final_issuances %>%
    mutate(Address = str_squish(Address))

## VRDB ####
vrdb_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/"
old_files <- read_csv(paste0(vrdb_path, "current_file_names.csv"))
vrdb_txt_files <- list.files(vrdb_path, pattern = "\\.txt$")
vrdb_txt_files <- vrdb_txt_files[grepl("VRDB", vrdb_txt_files)]

new_files <- vrdb_txt_files[!vrdb_txt_files %in% old_files$vrdb_txt_files]

# Initialize a data frame to store file names and their guessed delimiters
delimiters_df <- data.frame(FileName = character(),
                            GuessedDelimiter = character(),
                            stringsAsFactors = FALSE)

# Loop over the files and apply the guessDelimiter function
for(file in vrdb_txt_files) {
    guessed_delimiter <- guessDelimiter(paste0(vrdb_path, file))

    # Add the file name and its guessed delimiter to the data frame
    delimiters_df <- rbind(delimiters_df,
                           data.frame(FileName = basename(file),
                                      GuessedDelimiter = guessed_delimiter))
}

delimiters_df$GuessedDelimiter[delimiters_df$GuessedDelimiter == " "] <- "\t"
## NOTE FROM JG: WHAT IS THIS AND WHY
#if(nrow(delimiters_df) == 16){delimiters_df$GuessedDelimiter[4] <- "|"}

# Initialize an empty list to store the data frames
vrdb_list <- list()

#load the files
# Loop over the file names
for(i in 1:length(new_files)) {
    # Extract the file name
    file_name <- new_files[i]

    # Find the guessed delimiter for the current file
    guessed_delimiter <- delimiters_df$GuessedDelimiter[delimiters_df$FileName == file_name]

    # Check if a guessed delimiter was found, if not, default to "|"
    if (length(guessed_delimiter) == 0) {
        guessed_delimiter <- "|"
    }

    # Read the data from the file using the guessed delimiter
    vrdb_df <- read.table(paste0("R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/", file_name),
                          sep = guessed_delimiter, header = TRUE, na.strings = "", fill = TRUE, quote = "", comment.char = "")

    # Extract year from file name
    vrdb_df <- vrdb_df %>%
        mutate(Year = substr(file_name, 1, 4))
    # Add the data frame to the list
    vrdb_list[[i]] <- vrdb_df
}

# Convert every variable in each data frame to a character
vrdb_list <- lapply(vrdb_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

# rbind all data frames in the list
final_vrdb <- bind_rows(vrdb_list)

final_vrdb <- final_vrdb %>%
    unite(., col = "Address",  RegStNum, RegStPreDirection, RegStName,
          RegStType, RegStPostDirection, na.rm=TRUE, sep = " ") %>%
    mutate(Address = str_squish(Address))

# Columns we want to keep
col_names_keep <- c("Address", "RegCity", "RegState", "RegZipCode")

# Create final vrdb keeping only address information for geocoding
final_vrdb <- select(final_vrdb, all_of(col_names_keep))

# Combining Address columns for 1 single address
final_vrdb <- final_vrdb %>% unite(col = "Address", Address, RegCity,
                                   RegState, RegZipCode, sep = ', ') %>%
    select(Address)

# String squish here
final_vrdb <- final_vrdb %>% mutate(Address = str_squish(Address))

## Clean up environment ####
rm(vrdb_list)
rm(vrdb_df)
gc()

# Addresses for geocoding ####
## New addresses ####
addresses_for_geocoding <- final_vrdb %>%
    distinct(Address) #rbind(final_vrdb, final_issuances) %>% distinct(Address)

## Load old addresses ####
# Read in previous RDS file with all previously geocoded addresses
all_geocoded_addresses <- readRDS(paste0("R:/Project/UWED/Master Team Folder/",
                                         "Data/Outputs/",
                                         "all_geocoded_addresses.RDS"))

## Get remaining ####
remaining <- addresses_for_geocoding$Address[
    !addresses_for_geocoding$Address %in% all_geocoded_addresses$Address
    ]
length(remaining)

remaining <- data.frame(remaining)
colnames(remaining)[1] <- "Address"

# Apply the function to filter the data
remaining <- remaining[sapply(remaining$Address, remove_observations), ]
remaining <- as.data.frame(remaining)
colnames(remaining)[1] <- "Address"

## Eestimate cost ####
estimate <- (length(unique(remaining$Address))/1000)*0.5 #price

price_estimate <- paste("The estimated price for the current batch of geocoding is $", round(estimate, 0), " USD. This assumes AWS's pricing of $0.5 per 1000 cases as of February 2024.", sep="")
write.table(price_estimate,
            paste0("R:/Project/UWED/Master Team Folder/",
                   "Data/Geocoding/Price Estimate.txt"), row.names=FALSE)

#save to R: drive
saveRDS(remaining,
        paste0("R:/Project/UWED/Master Team Folder/",
               "Data/Outputs/",
               "addresses_for_geocoding.RDS"))

