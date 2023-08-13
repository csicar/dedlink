use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    process::id,
};

use anyhow::{anyhow, Context};
use clap::Parser;
use pathdiff::diff_paths;
use sha2::{Digest, Sha512};
use tokio::{
    fs::{self, File},
    io::{self, AsyncReadExt, BufReader},
};
use walkdir::{DirEntry, WalkDir};

/// Deduplicate files in a folder by symlinking to a central repository
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Files or folders to deduplicate
    #[arg(short, long)]
    files: String,

    #[arg(long, default_value = ".dedlink")]
    deduplication_folder: PathBuf,

    #[arg(short, default_value_t = false)]
    verbose: bool,

    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
struct Sha512Hash([u8; 64]);

impl From<[u8; 64]> for Sha512Hash {
    fn from(value: [u8; 64]) -> Self {
        Sha512Hash(value)
    }
}

// You can choose to implement multiple traits, like Lower and UpperHex
impl fmt::Display for Sha512Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:x}")?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut files_by_hashes = HashMap::<Sha512Hash, Vec<DirEntry>>::new();

    for entry in WalkDir::new(args.files).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let hash = hash_file(entry.path()).await?;

            files_by_hashes.entry(hash).or_default().push(entry);
        }
    }

    if args.verbose {
        println!("Found these files");
        for (hash, files) in files_by_hashes.iter() {
            println!("Hash {hash}:");
            for file in files.iter() {
                println!("|--  {}", file.path().display())
            }
        }
    }
    if args.dry_run {
        return Ok(());
    }

    let output_folder = args.deduplication_folder;
    fs::create_dir_all(&output_folder).await?;

    for (hash, files) in files_by_hashes.iter() {
        if let Some(file) = files.get(0) {
            let dedup_file = output_folder.join(format!("{hash}"));
            println!("copy file");
            if !fs::try_exists(&dedup_file).await? {
                println!("create new file");
                fs::rename(file.path(), &dedup_file).await?;
            }
            println!("symlink");
            for identical_file in files.iter() {
                println!("rm file {identical_file:?}");
                replace_with_symlink(identical_file.path(), &dedup_file).await?;
                let post_hash = hash_file(identical_file.path()).await?;
                assert_eq!(hash, &post_hash);
            }
        } else {
            // Do not create a link for a unique file
        }
    }
    files_by_hashes
        .iter()
        .for_each(|(hash, files)| println!("{hash} {files:#?}",));

    Ok(())
}

async fn replace_with_symlink(to_replace: &Path, destination: &Path) -> anyhow::Result<()> {
    println!("form {destination:?} to {to_replace:?}");
    let relative_path = diff_paths(
        destination,
        to_replace.parent().expect("File must have a parent"),
    )
    .ok_or(anyhow!("Could not calculate relative path"))?;
    println!("rel path {relative_path:?}");
    fs::remove_file(to_replace).await?;
    fs::symlink(relative_path, to_replace)
        .await
        .context("trying to symlink a file")?;
    Ok(())
}

async fn hash_file(path: &Path) -> anyhow::Result<Sha512Hash> {
    let mut hasher = Sha512::new();

    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);

    let mut buffer = [0; 4048];

    while let n_read @ 1.. = reader.read(&mut buffer).await? {
        hasher.update(&buffer[0..n_read]);
    }

    let hash = hasher.finalize();

    Ok(Sha512Hash(hash.into()))
}
