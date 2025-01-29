### Combine the geocoded datasets to prepare to 04_Race_Imputation ###
### Removing observations with no lat/long###

#### SET LIBRARY PATH ####

# Note: file path should be "R:/Project/UWED.." if using Windows server
#pull packages from the project site_libs - also install packages here if needed
.libPaths('~/win/project/UWED/Master Team Folder/Code/site_libs/')

# install.packages("pacman")
pacman::p_load(tidyverse, # For data manipulation
               data.table) # For fast file operations and filtering

## set working directory and load helper file

source("00_helper_fns.R")

# combine and clean geocoded data

combine_and_clean_geocoded_data <- function(existing_data_file, new_data_file, output_file) {

    # Load the existing dataset (RDS file)
    existing_data <- readRDS(existing_data_file) %>%
        filter(!is.na(Longitude) & !is.na(Latitude))

    # Load the new dataset (CSV file)
    new_data <- fread(new_data_file, header = TRUE) # using fread for faster performance

    # Adjust columns based on the number of columns in the new data
    # Ideally, make sure new data doesn't contain the index column
    if (ncol(new_data) == 4) {
        new_data <- new_data[, 2:4]
        setnames(new_data, c("Address", "Longitude", "Latitude"))
    } else if (ncol(new_data) == 3) {
        setnames(new_data, c("Address", "Longitude", "Latitude"))
    } else {
        stop("Unexpected number of columns in the new data file")
    }

    # Combine datasets
    combined_data <- rbindlist(list(as.data.table(existing_data), new_data), use.names = TRUE, fill = TRUE) # using data.table for faster binding

    # Remove duplicates, filter rows with missing values
    combined_data <- combined_data[!duplicated(Address) & !is.na(Longitude) & !is.na(Latitude)]

    # Additional filtering, if invalid addresses CAN still have Lat and Long
    # combined_data <- combined_data[sapply(combined_data$Address, remove_observations)]

    # Save the final dataset to an RDS file
    saveRDS(combined_data, output_file)

    # Clean up memory
    rm(existing_data, new_data, combined_data)
    gc()

    # Return a message indicating success
    message("Data successfully combined and cleaned, saved to: ", output_file)
}

# 6 minutes with Jan 2025 data


combine_and_clean_geocoded_data("~/win/project/UWED/Master Team Folder/Data/Outputs/all_geocoded_addresses_Dec2024_batch.RDS",
                                "~/win/project/UWED/Master Team Folder/Data/Outputs/aws_geocoded_20250124_1.csv",
                                "~/win/project/UWED/Master Team Folder/Data/Outputs/all_geocoded_addresses_Jan2025_batch.RDS")
