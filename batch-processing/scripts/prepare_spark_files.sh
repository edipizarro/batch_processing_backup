#!/bin/bash

# Function to clean the 'dist' directory if it exists
clean_dist () {
  if [ -d "dist" ]; then
    rm -rf "dist" || { echo "Failed to remove 'dist' directory."; exit 1; }
  fi
}

# Function to prepare Python dependencies with Poetry
prepare_poetry () {
  poetry install || { echo "Failed to install Poetry dependencies."; exit 1; }
  poetry build || { echo "Failed to build Poetry project."; exit 1; }
}

# Function to create zip archive of batch processing project
create_batch_processing_zip() {
  tar -xzf dist/batch_processing-0.1.0.tar.gz || { echo "Failed to extract batch_processing-0.1.0.tar.gz."; exit 1; }
  cd batch_processing-0.1.0 || { echo "Failed to change directory to batch_processing-0.1.0."; exit 1; }
  zip -r ../dist/batch_processing-0.1.0.zip * || { echo "Failed to create batch_processing-0.1.0.zip."; exit 1; }
  cd - >/dev/null || { echo "Failed to change back to previous directory."; exit 1; }
  rm -r batch_processing-0.1.0 || { echo "Failed to remove batch_processing-0.1.0 directory."; exit 1; }
}

# Function to create zip archive of Python virtual environment
create_venv_zip() {
  base_path=$(pwd)
  python_path=$(poetry run which python) || { echo "Failed to locate Python executable."; exit 1; }
  venv_path=$(dirname $(dirname "$python_path"))
  packages_path="${venv_path}/lib/python3.9/site-packages"
  cd "${packages_path}" || { echo "Failed to change directory to $packages_path."; exit 1; }
  zip "${base_path}/dist/batch_processing-env.zip" * || { echo "Failed to create batch_processing-env.zip."; exit 1; }
  cd - >/dev/null || { echo "Failed to change back to previous directory."; exit 1; }
}

# Main function to orchestrate the process
main () {
  clean_dist
  prepare_poetry
  create_batch_processing_zip
  create_venv_zip
}

# Execute the main function
main