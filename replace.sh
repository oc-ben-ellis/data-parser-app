#!/bin/bash

# Script to replace template placeholders in all text files
# Usage: ./replace.sh <app_name> <step_name> <stage_name>

set -e

# Check if all three arguments are provided
if [ $# -ne 3 ]; then
    echo "Usage: $0 <app_name> <step_name> <stage_name>"
    echo "Example: $0 myapp mystep mystage"
    exit 1
fi

APP_NAME="$1"
STEP_NAME="$2"
STAGE_NAME="$3"

# Convert to different cases
APP_NAME_LOWER=$(echo "$APP_NAME" | tr '[:upper:]' '[:lower:]')
APP_NAME_UPPER=$(echo "$APP_NAME" | tr '[:lower:]' '[:upper:]')
APP_NAME_CAPITAL=$(echo "$APP_NAME" | sed 's/./\U&/')

STEP_NAME_LOWER=$(echo "$STEP_NAME" | tr '[:upper:]' '[:lower:]')
STEP_NAME_UPPER=$(echo "$STEP_NAME" | tr '[:lower:]' '[:upper:]')
STEP_NAME_CAPITAL=$(echo "$STEP_NAME" | sed 's/./\U&/')

STAGE_NAME_LOWER=$(echo "$STAGE_NAME" | tr '[:upper:]' '[:lower:]')
STAGE_NAME_UPPER=$(echo "$STAGE_NAME" | tr '[:lower:]' '[:upper:]')
STAGE_NAME_CAPITAL=$(echo "$STAGE_NAME" | sed 's/./\U&/')

echo "Replacing template placeholders..."
echo "APP_NAME: $APP_NAME -> $APP_NAME_LOWER, $APP_NAME_UPPER, $APP_NAME_CAPITAL"
echo "STEP_NAME: $STEP_NAME -> $STEP_NAME_LOWER, $STEP_NAME_UPPER, $STEP_NAME_CAPITAL"
echo "STAGE_NAME: $STAGE_NAME -> $STAGE_NAME_LOWER, $STAGE_NAME_UPPER, $STAGE_NAME_CAPITAL"
echo

# Function to rename files and directories containing --APP_NAME_LOWER--
rename_files_and_dirs() {
    local old_pattern="$1"
    local new_name="$2"
    
    echo "Renaming files and directories containing '$old_pattern' to '$new_name'..."
    
    # Find all files and directories containing the pattern, sort by depth (deepest first)
    # This ensures we rename nested items before their parents
    find . -name "*$old_pattern*" | sort -r | while read -r item; do
        if [ -e "$item" ]; then
            # Get the directory and filename parts
            dir=$(dirname "$item")
            filename=$(basename "$item")
            
            # Replace the pattern in the filename
            new_filename=$(echo "$filename" | sed "s/$old_pattern/$new_name/g")
            
            # Create the new path
            new_path="$dir/$new_filename"
            
            # Only rename if the new name is different
            if [ "$item" != "$new_path" ]; then
                echo "  Renaming: $item -> $new_path"
                mv "$item" "$new_path"
            fi
        fi
    done
}

# Rename files and directories containing --APP_NAME_LOWER--
rename_files_and_dirs "--APP_NAME_LOWER--" "$APP_NAME_LOWER"

echo

# Find all text files and replace placeholders (excluding replace.sh to avoid self-modification)
find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<APP_NAME_LOWER>>/$APP_NAME_LOWER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<APP_NAME_UPPER>>/$APP_NAME_UPPER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<APP_NAME_CAPITAL>>/$APP_NAME_CAPITAL/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STEP_NAME_LOWER>>/$STEP_NAME_LOWER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STEP_NAME_UPPER>>/$STEP_NAME_UPPER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STEP_NAME_CAPITAL>>/$STEP_NAME_CAPITAL/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STAGE_NAME_LOWER>>/$STAGE_NAME_LOWER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STAGE_NAME_UPPER>>/$STAGE_NAME_UPPER/g" {} \;

find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.md" -o -name "*.txt" -o -name "*.sh" -o -name "*.tf" -o -name "*.toml" -o -name "*.cfg" -o -name "*.ini" -o -name "*.conf" -o -name "Makefile" -o -name "Dockerfile" \) ! -name "replace.sh" -exec sed -i "s/<<STAGE_NAME_CAPITAL>>/$STAGE_NAME_CAPITAL/g" {} \;

echo "Template replacement completed successfully!"
