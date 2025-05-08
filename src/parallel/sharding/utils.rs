use std::path::PathBuf;

use glob::glob;
use log::warn;

use crate::error::{DiskyError, Result};

/// Helper function to find shard paths matching a prefix in a directory
pub(crate) fn find_shard_paths(output_dir: &PathBuf, file_prefix: &str) -> Result<Vec<PathBuf>> {
    let glob_pattern = format!("{}_*", file_prefix);
    let pattern = output_dir.join(glob_pattern);
    let pattern_str = pattern.to_string_lossy();
    
    // Get a list of all matching files
    let mut shard_paths: Vec<PathBuf> = Vec::new();
    
    for entry in glob(&pattern_str)
        .map_err(|e| DiskyError::Other(format!("Invalid glob pattern: {}", e)))?
    {
        match entry {
            Ok(path) => shard_paths.push(path),
            Err(e) => warn!("Error with glob entry: {}", e),
        }
    }
    
    // Check if we found any shards
    if shard_paths.is_empty() {
        return Err(DiskyError::Other(format!(
            "No shards found with prefix '{}' in {:?}",
            file_prefix, output_dir
        )));
    }
    
    Ok(shard_paths)
}