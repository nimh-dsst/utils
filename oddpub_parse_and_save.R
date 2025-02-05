# Function to process PDFs in a directory
process_pdf_directory <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    warning("Directory does not exist: ", dir_path)
    return(NULL)
  }
  
  # Get output filename based on directory name
  dir_name <- basename(dir_path)
  # Using file.path() to properly handle path separators
  output_file <- file.path(dir_path, paste0("pdf_sentences_", dir_name, ".rds"))
  results_file <- file.path(dir_path, "oddpub_results.csv")
  
  tryCatch({
    # Add trailing separator to ensure proper path joining
    pdf_sentences <- oddpub::pdf_load(file.path(dir_path, ""))
    saveRDS(pdf_sentences, file = output_file)
    message("Successfully processed directory: ", dir_path)
    open_data_results <- oddpub::open_data_search(pdf_sentences)
    # Add directory name as a column to identify source
    open_data_results$source_dir <- dir_name
    write.csv(open_data_results, file = results_file)
    message("Open data results saved to ", results_file)
    return(TRUE)
  }, error = function(e) {
    warning("Error processing directory ", dir_path, ": ", e$message)
    return(FALSE)
  })
}

# Base directory containing subdirectories of PDFs
base_path <- file.path("/data/NIMH_scratch/lawrimorejg/pub_data/sample_pdfs/sample_extracts")

# Get all subdirectories
subdirs <- list.dirs(base_path, full.names = TRUE, recursive = FALSE)

if (length(subdirs) == 0) {
  stop("No subdirectories found in: ", base_path)
}

# Process each subdirectory
results <- sapply(subdirs, process_pdf_directory)

# Combine all results files
all_results <- data.frame()
for (subdir in subdirs) {
  results_file <- file.path(subdir, "oddpub_results.csv")
  if (file.exists(results_file)) {
    subdir_results <- read.csv(results_file)
    all_results <- rbind(all_results, subdir_results)
  }
}

# Save combined results to base directory
if (nrow(all_results) > 0) {
  total_results_file <- file.path(base_path, "total_oddpub_results.csv")
  write.csv(all_results, file = total_results_file, row.names = FALSE)
  message("Combined results saved to ", total_results_file)
}

# Summary of processing
successful <- sum(results, na.rm = TRUE)
failed <- sum(is.na(results) | !results)
message(sprintf("Processing complete. Successfully processed %d directories. Failed: %d", 
                successful, failed))
