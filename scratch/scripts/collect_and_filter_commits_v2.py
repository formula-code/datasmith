"""Modernized commit collection script using SQLite storage."""
from __future__ import annotations

import argparse
import os
import platform
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from tqdm.auto import tqdm

from datasmith.execution.collect_commits_offline import collect_commits
from datasmith.execution.utils import _get_commit_info_offline, clone_repo, find_file_in_tree, has_core_file
from datasmith.logging_config import configure_logging
from datasmith.storage import (
    DataSmithDB, RepositoryStore, CommitStore, PipelineTracker,
    Repository, Commit
)

# Configure logging
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect and filter commits using SQLite storage.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--database", 
        type=str, 
        default="scratch/artifacts/datasmith.db",
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--max-repos", 
        type=int, 
        default=150, 
        help="Maximum number of repositories to process"
    )
    parser.add_argument(
        "--min-stars", 
        type=int, 
        default=100,
        help="Minimum stars for repository selection"
    )
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=16,
        help="Maximum number of worker threads/processes"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Show what would be processed without making changes"
    )
    parser.add_argument(
        "--resume-run-id", 
        type=int,
        help="Resume a previous pipeline run by ID"
    )
    
    return parser.parse_args()


def collect_commits_for_repository(repo: Repository, max_commits: int = 5000) -> list[Commit]:
    """Collect commits for a single repository.
    
    Args:
        repo: Repository to process
        max_commits: Maximum commits to collect per repository
        
    Returns:
        list[Commit]: Collected commits
    """
    logger.info(f"Collecting commits for {repo.full_name}")
    
    try:
        # Use existing collection logic
        raw_commits = collect_commits(
            repo_url=repo.url,
            max_pages=max_commits // 100,  # Assuming 100 commits per page
            query="state=closed&sort=popularity&direction=desc"
        )
        
        commits = []
        for raw_commit in raw_commits:
            # Filter and convert to our Commit model
            if is_performance_relevant_commit(raw_commit):
                commit = Commit(
                    id=None,
                    repository_id=repo.id,
                    sha=raw_commit.get('sha', ''),
                    commit_date=datetime.fromisoformat(raw_commit.get('commit_date', '').replace('Z', '+00:00')),
                    author_name=raw_commit.get('author_name'),
                    author_email=raw_commit.get('author_email'),
                    message=raw_commit.get('message'),
                    pr_number=raw_commit.get('number'),
                    is_merge=raw_commit.get('is_merge', False),
                    is_performance_relevant=True,  # Already filtered
                    metadata={
                        'labels': raw_commit.get('labels', []),
                        'changed_files': raw_commit.get('changed_files', []),
                        'additions': raw_commit.get('additions'),
                        'deletions': raw_commit.get('deletions')
                    }
                )
                commits.append(commit)
        
        logger.info(f"Collected {len(commits)} performance-relevant commits from {repo.full_name}")
        return commits
        
    except Exception as e:
        logger.error(f"Failed to collect commits for {repo.full_name}: {e}")
        raise


def is_performance_relevant_commit(commit_data: dict[str, Any]) -> bool:
    """Determine if a commit is performance-relevant.
    
    Args:
        commit_data: Raw commit data
        
    Returns:
        bool: True if commit is performance-relevant
    """
    # Use existing filtering logic
    message = commit_data.get('message', '').lower()
    labels = [label.get('name', '').lower() for label in commit_data.get('labels', [])]
    changed_files = commit_data.get('changed_files', [])
    
    # Performance keywords in commit message
    perf_keywords = ['performance', 'speed', 'optimization', 'faster', 'benchmark', 'efficient']
    if any(keyword in message for keyword in perf_keywords):
        return True
    
    # Performance-related labels
    perf_labels = ['performance', 'optimization', 'speed']
    if any(label in labels for label in perf_labels):
        return True
    
    # Check if core algorithmic files were changed
    core_extensions = ['.py', '.c', '.cpp', '.cxx', '.cc', '.h', '.hpp']
    exclude_paths = ['test', 'tests', 'doc', 'docs', 'example', 'examples', 'benchmark']
    
    for file_path in changed_files:
        if any(file_path.lower().endswith(ext) for ext in core_extensions):
            if not any(exclude in file_path.lower() for exclude in exclude_paths):
                return True
    
    return False


def process_repository_batch(batch: list[Repository], 
                            commit_store: CommitStore, 
                            tracker: PipelineTracker,
                            run_id: int,
                            dry_run: bool = False) -> dict[str, Any]:
    """Process a batch of repositories.
    
    Args:
        batch: List of repositories to process
        commit_store: Commit storage instance
        tracker: Pipeline tracker instance
        run_id: Pipeline run ID
        dry_run: If True, don't actually store commits
        
    Returns:
        dict: Processing results
    """
    results = {
        'processed': 0,
        'commits_collected': 0,
        'errors': []
    }
    
    for repo in batch:
        try:
            # Track repository processing
            tracker.track_item(run_id, "repository", repo.full_name, "processing")
            
            # Collect commits
            commits = collect_commits_for_repository(repo)
            
            if not dry_run and commits:
                # Store commits in database
                commit_store.insert_commits(commits)
            
            # Update tracking
            tracker.track_item(
                run_id, "repository", repo.full_name, "completed",
                output_data={'commits_collected': len(commits)}
            )
            
            results['processed'] += 1
            results['commits_collected'] += len(commits)
            
        except Exception as e:
            error_msg = str(e)
            results['errors'].append(f"{repo.full_name}: {error_msg}")
            
            # Track failure
            tracker.track_item(
                run_id, "repository", repo.full_name, "failed",
                error_message=error_msg
            )
            
            logger.error(f"Failed to process {repo.full_name}: {e}")
    
    return results


def get_environment_info() -> dict[str, Any]:
    """Get environment information for tracking.
    
    Returns:
        dict: Environment information
    """
    return {
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'hostname': platform.node(),
        'user': os.getenv('USER', 'unknown'),
        'cwd': str(Path.cwd())
    }


def main() -> None:
    """Main script entry point."""
    args = parse_args()
    
    # Initialize database and stores
    db = DataSmithDB(args.database)
    repo_store = RepositoryStore(db)
    commit_store = CommitStore(db)
    tracker = PipelineTracker(db)
    
    # Get or resume pipeline run
    if args.resume_run_id:
        run_id = args.resume_run_id
        existing_run = tracker.get_pipeline_run(run_id)
        if not existing_run:
            logger.error(f"Pipeline run {run_id} not found")
            return
        logger.info(f"Resuming pipeline run {run_id}")
    else:
        # Start new pipeline run
        run_name = f"collect_and_filter_commits_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_id = tracker.start_pipeline_run(
            run_name=run_name,
            script_name="collect_and_filter_commits_v2.py",
            config=vars(args),
            environment_info=get_environment_info()
        )
        logger.info(f"Started pipeline run '{run_name}' with ID {run_id}")
    
    try:
        # Get valid repositories
        repositories = repo_store.get_valid_repositories(
            limit=args.max_repos,
            min_stars=args.min_stars
        )
        
        if not repositories:
            logger.warning("No valid repositories found")
            tracker.complete_pipeline_run(
                run_id, "completed", 
                output_summary={'repositories_processed': 0, 'commits_collected': 0}
            )
            return
        
        logger.info(f"Processing {len(repositories)} repositories with {args.max_workers} workers")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
            for repo in repositories:
                logger.info(f"Would process: {repo.full_name} ({repo.stars} stars)")
            return
        
        # Process repositories in batches
        batch_size = max(1, len(repositories) // args.max_workers)
        batches = [repositories[i:i + batch_size] for i in range(0, len(repositories), batch_size)]
        
        total_results = {
            'processed': 0,
            'commits_collected': 0,
            'errors': []
        }
        
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            # Submit batch processing tasks
            future_to_batch = {
                executor.submit(
                    process_repository_batch, batch, commit_store, tracker, run_id, args.dry_run
                ): i for i, batch in enumerate(batches)
            }
            
            # Process results with progress bar
            with tqdm(total=len(repositories), desc="Processing repositories") as pbar:
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    try:
                        results = future.result()
                        
                        # Aggregate results
                        total_results['processed'] += results['processed']
                        total_results['commits_collected'] += results['commits_collected']
                        total_results['errors'].extend(results['errors'])
                        
                        pbar.update(results['processed'])
                        
                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {e}")
                        total_results['errors'].append(f"Batch {batch_idx}: {str(e)}")
        
        # Complete pipeline run
        status = "completed" if not total_results['errors'] else "failed"
        tracker.complete_pipeline_run(
            run_id, status,
            output_summary={
                'repositories_processed': total_results['processed'],
                'commits_collected': total_results['commits_collected'],
                'error_count': len(total_results['errors'])
            },
            error_message="\n".join(total_results['errors'][:5]) if total_results['errors'] else None
        )
        
        # Print summary
        logger.info(f"Pipeline run {run_id} completed")
        logger.info(f"Processed {total_results['processed']} repositories")
        logger.info(f"Collected {total_results['commits_collected']} commits")
        if total_results['errors']:
            logger.warning(f"Encountered {len(total_results['errors'])} errors")
            for error in total_results['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  {error}")
        
        # Print database stats
        stats = db.get_stats()
        logger.info(f"Database now contains {stats.get('commits_count', 0)} commits across {stats.get('repositories_count', 0)} repositories")
        
    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        tracker.complete_pipeline_run(
            run_id, "failed", 
            error_message=str(e)
        )
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    main()