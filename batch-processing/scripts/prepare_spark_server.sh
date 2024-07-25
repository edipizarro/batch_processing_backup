#!/bin/bash

validate_parameters () {
    if [ $# -lt 1 ] || [ $# -gt 2 ]; then
        echo "Usage: $0 <server_address> [<ssh_key>]"
        exit 1
    fi
}

ssh_options () {
    echo $ssh_key
    if [ -z "$ssh_key" ]; then
        echo "Using base SSH key."
        ssh_options=(-o "StrictHostKeyChecking=no" -o "UserKnownHostsFile=/dev/null")
    else
        echo "Using custom SSH key: $ssh_key"
        ssh_options=(-i "$ssh_key" -o "StrictHostKeyChecking=no" -o "UserKnownHostsFile=/dev/null")
    fi
}

scp_files () {
    local src_folder=$1
    shift  # Shift the arguments to remove the first argument (src_folder)
    local destination=$1  # First argument after src_folder is destination
    shift  # Shift again to remove destination from the file list
    local files=("$@")  # Use all remaining arguments as files array

    # Loop through the list of files and copy them
    for file in "${files[@]}"; do
        src_file="${src_folder}/${file}"
        dest_file="${destination}/${file}"

        # Perform SCP with error handling and logging
        if scp "${ssh_options[@]}" "$src_file" "$dest_file"; then
            echo "Copied $src_file to $dest_file successfully."
        else
            echo "Failed to copy $src_file to $dest_file."
            exit 1  # Exit the script if SCP fails
        fi
    done
}

main () {
    validate_parameters "$@"

    server_address=$1
    ssh_key=$2

    ssh_options
    destination="hadoop@${server_address}:/home/hadoop"

    files=("batch_processing-0.1.0.zip" "batch_processing-env.zip" "healpix-1.0.jar" "minimal_astroide-2.0.0.jar")
    scp_files dist $destination ${files[@]}

    scp_files scripts $destination "batch.py"
}

# Execute the main function with script parameters
main "$@"