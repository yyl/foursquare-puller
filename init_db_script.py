#!/usr/bin/env python3
"""
Enhanced Database Initialization Script for Foursquare Check-ins using Places API

This script creates and initializes the SQLite database schema for storing
Foursquare check-ins, places, and user data with proper indexing and constraints.

Author: Enhanced version of original script, migrated to Places API
"""

import sqlite3
import os
import sys
import argparse
import logging
from typing import Optional

def setup_logging() -> logging.Logger:
    """Setup logging configuration"""
    logger = logging.getLogger('init_db')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def create_foursquare_tables(db_path: str, force_recreate: bool = False) -> bool:
    """
    Creates the database tables for Foursquare data storage.
    
    Args:
        db_path: Path to the SQLite database file
        force_recreate: If True, drop existing tables before creating new ones
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('init_db')
    
    db_exists = os.path.exists(db_path)
    if db_exists and not force_recreate:
        logger.info(f"Database {db_path} already exists")
    elif db_exists and force_recreate:
        logger.info(f"Force recreate mode: will drop existing tables in {db_path}")
    else:
        logger.info(f"Creating new database: {db_path}")
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        logger.info(f"Connected to database: {db_path}")
        
        cursor.execute("PRAGMA foreign_keys = ON")
        
        if force_recreate:
            logger.info("Dropping existing tables...")
            cursor.execute("DROP TABLE IF EXISTS checkins")
            cursor.execute("DROP TABLE IF EXISTS places") 
            cursor.execute("DROP TABLE IF EXISTS users")
        
        logger.info("Creating 'users' table...")
        create_users_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            foursquare_user_id TEXT PRIMARY KEY NOT NULL,
            last_pulled_timestamp INTEGER DEFAULT 0,
            last_updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            CONSTRAINT users_timestamps_check CHECK (last_pulled_timestamp >= 0)
        )
        """
        cursor.execute(create_users_table_sql)
        
        logger.info("Creating 'places' table (for Places API)...")
        create_places_table_sql = """
        CREATE TABLE IF NOT EXISTS places (
            fsq_place_id TEXT PRIMARY KEY NOT NULL,
            name TEXT,
            latitude REAL,
            longitude REAL,
            address TEXT,
            locality TEXT,
            region TEXT,
            postcode TEXT,
            country TEXT,
            formatted_address TEXT,
            primary_category_fsq_id TEXT,
            primary_category_name TEXT,
            website TEXT,
            tel TEXT,
            email TEXT,
            price INTEGER,
            rating REAL,
            last_updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            CONSTRAINT places_coords_check CHECK (
                (latitude IS NULL AND longitude IS NULL) OR 
                (latitude IS NOT NULL AND longitude IS NOT NULL AND
                 latitude BETWEEN -90 AND 90 AND 
                 longitude BETWEEN -180 AND 180)
            )
        )
        """
        cursor.execute(create_places_table_sql)
        
        logger.info("Creating 'checkins' table...")
        create_checkins_table_sql = """
        CREATE TABLE IF NOT EXISTS checkins (
            checkin_id TEXT PRIMARY KEY NOT NULL,
            foursquare_user_id TEXT NOT NULL,
            place_fsq_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            type TEXT,
            shout TEXT,
            private BOOLEAN DEFAULT 0,
            visibility TEXT,
            is_mayor BOOLEAN DEFAULT 0,
            liked BOOLEAN DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            photos_count INTEGER DEFAULT 0,
            source_name TEXT,
            source_url TEXT,
            time_zone_offset INTEGER,
            imported_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (foursquare_user_id) REFERENCES users(foursquare_user_id) ON DELETE CASCADE,
            FOREIGN KEY (place_fsq_id) REFERENCES places(fsq_place_id) ON DELETE RESTRICT,
            CONSTRAINT checkins_counts_check CHECK (
                comments_count >= 0 AND 
                likes_count >= 0 AND 
                photos_count >= 0
            ),
            CONSTRAINT checkins_timestamps_check CHECK (created_at > 0)
        )
        """
        cursor.execute(create_checkins_table_sql)
        
        logger.info("Creating indexes...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_last_pulled ON users(last_pulled_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_places_name ON places(name)",
            "CREATE INDEX IF NOT EXISTS idx_places_locality ON places(locality)",
            "CREATE INDEX IF NOT EXISTS idx_places_location ON places(latitude, longitude)",
            "CREATE INDEX IF NOT EXISTS idx_places_category ON places(primary_category_fsq_id)",
            "CREATE INDEX IF NOT EXISTS idx_checkins_user ON checkins(foursquare_user_id)",
            "CREATE INDEX IF NOT EXISTS idx_checkins_place ON checkins(place_fsq_id)",
            "CREATE INDEX IF NOT EXISTS idx_checkins_created ON checkins(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_checkins_user_created ON checkins(foursquare_user_id, created_at)",
        ]
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        logger.info("Creating views...")
        
        create_checkins_with_places_view = """
        CREATE VIEW IF NOT EXISTS checkins_with_places AS
        SELECT 
            c.checkin_id, c.foursquare_user_id, c.created_at, c.type, c.shout, c.private,
            c.visibility, c.is_mayor, c.liked, c.comments_count, c.likes_count, c.photos_count,
            c.source_name, c.source_url, c.time_zone_offset, c.imported_at,
            p.fsq_place_id, p.name as place_name, p.latitude, p.longitude, p.address,
            p.locality, p.region, p.postcode, p.country, p.primary_category_name, p.rating
        FROM checkins c
        LEFT JOIN places p ON c.place_fsq_id = p.fsq_place_id
        """
        cursor.execute(create_checkins_with_places_view)
        
        create_user_stats_view = """
        CREATE VIEW IF NOT EXISTS user_stats AS
        SELECT 
            u.foursquare_user_id,
            u.last_pulled_timestamp,
            COUNT(c.checkin_id) as total_checkins,
            COUNT(DISTINCT c.place_fsq_id) as unique_places,
            MIN(c.created_at) as first_checkin_date,
            MAX(c.created_at) as last_checkin_date
        FROM users u
        LEFT JOIN checkins c ON u.foursquare_user_id = c.foursquare_user_id
        GROUP BY u.foursquare_user_id
        """
        cursor.execute(create_user_stats_view)
        
        conn.commit()
        logger.info("Database schema created successfully!")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Created tables: {', '.join(tables)}")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
        views = [row[0] for row in cursor.fetchall()]
        logger.info(f"Created views: {', '.join(views)}")
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

def verify_schema(db_path: str) -> bool:
    """Verify that the database schema is correct"""
    logger = logging.getLogger('init_db')
    
    if not os.path.exists(db_path):
        logger.error(f"Database file {db_path} does not exist")
        return False
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            required_tables = {'users', 'places', 'checkins'}
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = {row[0] for row in cursor.fetchall()}
            
            if not required_tables.issubset(existing_tables):
                logger.error(f"Missing required tables: {required_tables - existing_tables}")
                return False

            required_views = {'checkins_with_places', 'user_stats'}
            cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
            existing_views = {row[0] for row in cursor.fetchall()}

            if not required_views.issubset(existing_views):
                 logger.warning(f"Missing views: {required_views - existing_views}")

            logger.info("Database schema verification passed")
            return True
            
    except sqlite3.Error as e:
        logger.error(f"Error verifying schema: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Initialize SQLite database for Foursquare check-in storage using Places API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s f4q_places_data.db
  %(prog)s f4q_places_data.db --force
  %(prog)s f4q_places_data.db --verify-only
        """
    )
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument("--force", action="store_true", help="Force recreation of tables (WARNING: drops existing data)")
    parser.add_argument("--verify-only", action="store_true", help="Only verify the database schema")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    if args.verify_only:
        if not verify_schema(args.db_path):
            sys.exit(1)
        sys.exit(0)
    
    if args.force and os.path.exists(args.db_path):
        response = input("Are you sure you want to drop all existing data? (yes/no): ").lower()
        if response != 'yes':
            logger.info("Operation cancelled.")
            sys.exit(0)
    
    if not create_foursquare_tables(args.db_path, args.force):
        logger.error("Database initialization failed.")
        sys.exit(1)
    
    logger.info("Database initialization completed successfully.")
    verify_schema(args.db_path)

if __name__ == "__main__":
    main()