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
  
  tryCatch({
    # Add trailing separator to ensure proper path joining
    pdf_sentences <- oddpub::pdf_load(file.path(dir_path, ""))
    saveRDS(pdf_sentences, file = output_file)
    message("Successfully processed directory: ", dir_path)
    return(TRUE)
  }, error = function(e) {
    warning("Error processing directory ", dir_path, ": ", e$message)
    return(FALSE)
  })
} 
