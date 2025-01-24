## Authors
# Lucas Owen
# Jessica Godwin

# Setup ####
rm(list = ls())
## setwd() ####
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

## Parameters ####
write_date <- "20250124"
AWS_PlaceIndex <- "Ballots_20241206"
AWS_region <- "us-east-1"

## Libraries ####
library(dplyr)
library(readr)
library(reticulate)
library(parallel)
library(sf)
py_install("boto3")

## source ####
source("00_helper_fns.R")
source(paste0("R:/Project/UWED/Master Team Folder/",
              "Code/Godwin_AWS_keys.R"))

# #KEYS. REMOVE WHEN SHARING CODE
# Replaced with sourced Godwin_AWS_keys.R above
# ACCESS_KEY <- 'YOUR_ACCESS_KEY'
# SECRET_KEY <- 'YOUR_SECRET_KEY'
# api_key <- 'YOUR_API_KEY'

# Load Data ####

aws_addresses <- readRDS(paste0("R:/Project/UWED/Master Team Folder/",
                                "Data/Outputs/addresses_for_geocoding.RDS"))

# Set up AWS boto3 client for test ####
boto3 <- import('boto3')
session <- boto3$Session(aws_access_key_id = ACCESS_KEY,
                         aws_secret_access_key = SECRET_KEY,
                         region_name = AWS_region)
client <- boto3$client(
  service_name = 'location',
  region_name = AWS_region
)

# Prep output ####

end <- min(250000, nrow(aws_addresses))

addresses_to_geocode <- aws_addresses[1:end,]
# addresses_to_geocode <- aws_addresses[(end+1):nrow(aws_addresses),]
file_name <- paste0("R:/Project/UWED/Master Team Folder/Data/Outputs/",
                    "aws_geocoded_", write_date, "_", 1,".csv")

# ASCII Characters ####

# Apply the function to each address
non_ascii_indices <- which(sapply(addresses_to_geocode, contains_non_ascii))

# Extract addresses with non-ASCII characters
addresses_with_non_ascii <- addresses_to_geocode[non_ascii_indices]
addresses_with_non_ascii

#drop any addresses with non-ASCII characters
addresses_to_geocode <- addresses_to_geocode[!addresses_to_geocode %in% addresses_with_non_ascii]

# Parallel Implementation ####
## geocode fn ####
geocode <- function(address) {

    if (nchar(trimws(address)) == 0) {
        return(NA)
    } else {

        client <- boto3$client(
            service_name = 'location',
            region_name = AWS_region,
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY
        )
        response <- client$search_place_index_for_text(
            IndexName = AWS_PlaceIndex,
            Text = address
        )


        # Check if there are any results before accessing them
        if (length(response$Results) > 0 &&
            !is.null(response$Results[[1]]$Place$Geometry$Point)) {
            coordinates <- response$Results[[1]]$Place$Geometry$Point
            return(coordinates)
        } else {
            # Return NA or some other indicator for addresses with no result
            return(NA)
        }
    }
}

## start_time ####
start_time <- Sys.time()

## Set up cluster ####
num_cores <- min(detectCores() - 1, 10)
cluster <- makeCluster(num_cores)

## Export boto3 and the geocode fn ####
clusterEvalQ(cluster, {
 ## Packages + source ####
  library(reticulate)
  boto3 <- import('boto3')
  ## Need to load the credentials in each cluster node
  source(paste0("R:/Project/UWED/Master Team Folder/",
                "Code/Godwin_AWS_keys.R"))
  ## geocode fn ####
  geocode <- function(address) {

      if (nchar(trimws(address)) == 0) {
          return(NA)
      } else {

          client <- boto3$client(
              service_name = 'location',
              region_name = AWS_region,
              aws_access_key_id = ACCESS_KEY,
              aws_secret_access_key = SECRET_KEY
          )
          response <- client$search_place_index_for_text(
              IndexName = AWS_PlaceIndex,
              Text = address
          )


          # Check if there are any results before accessing them
          if (length(response$Results) > 0 &&
              !is.null(response$Results[[1]]$Place$Geometry$Point)) {
              coordinates <- response$Results[[1]]$Place$Geometry$Point
              return(coordinates)
          } else {
              # Return NA or some other indicator for addresses with no result
              return(NA)
          }
      }
  }
})

## parLapply ####
results <- tryCatch({
  parLapply(cluster, addresses_to_geocode, geocode)
}, error = function(e) e)

print(results)

## Stop the cluster ####
stopCluster(cluster)

## end_time ####
end_time <- Sys.time()
time_taken <- end_time - start_time
time_taken

## Clean up ####
df <- as.data.frame(do.call(rbind, results))
df <- cbind(addresses_to_geocode, df)

df$V1 <- unlist(df$V1)
df$V2 <- unlist(df$V2)

## Save ####
write.csv(df, file_name)

