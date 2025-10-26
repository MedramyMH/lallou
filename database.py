import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

class PriceDatabase:
    def __init__(self, db_path: str = "price_data.db"):
        self.db_path = db_path
        self._init_database()
        logging.info("Database initialized successfully")
    
    def _init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create price data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT NOT NULL,
                    timeframe INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(asset, timeframe, timestamp)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_asset_timeframe 
                ON price_data(asset, timeframe, timestamp DESC)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON price_data(timestamp)
            ''')
            
            conn.commit()
    
    def save_price_data(self, asset: str, price_data: Dict, timeframe: int) -> bool:
        """
        Save price data to database
        
        Args:
            asset: Asset symbol (e.g., 'EURUSD')
            price_data: Dictionary with price information
            timeframe: Timeframe in minutes (1, 5, 15, etc.)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO price_data 
                    (asset, timeframe, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    asset,
                    timeframe,
                    price_data['time'],
                    price_data['open'],
                    price_data['max'],  # Using 'max' as high
                    price_data['min'],  # Using 'min' as low
                    price_data['close'],
                    price_data.get('volume', 0)
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logging.error(f"Error saving price data for {asset}: {e}")
            return False
    
    def save_candle(self, asset: str, timeframe: int, candle_data: Dict) -> bool:
        """
        Alternative method name for saving candle data
        This provides compatibility with different naming conventions
        """
        return self.save_price_data(asset, candle_data, timeframe)
    
    def get_latest_prices(self, asset: str, timeframe: int, limit: int = 100) -> List[Dict]:
        """
        Get latest price data for a specific asset and timeframe
        
        Args:
            asset: Asset symbol
            timeframe: Timeframe in minutes
            limit: Number of records to return
            
        Returns:
            List of price data dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT timestamp, open, high, low, close, volume
                    FROM price_data 
                    WHERE asset = ? AND timeframe = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', (asset, timeframe, limit))
                
                rows = cursor.fetchall()
                result = []
                
                for row in rows:
                    result.append({
                        'timestamp': row['timestamp'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                
                return result
                
        except Exception as e:
            logging.error(f"Error getting latest prices for {asset}: {e}")
            return []
    
    def get_price_data(self, asset: str, timeframe: int, start_time: int = None, end_time: int = None) -> List[Dict]:
        """
        Get price data for a specific asset and timeframe within a time range
        
        Args:
            asset: Asset symbol
            timeframe: Timeframe in minutes
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            
        Returns:
            List of price data dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = '''
                    SELECT timestamp, open, high, low, close, volume
                    FROM price_data 
                    WHERE asset = ? AND timeframe = ?
                '''
                params = [asset, timeframe]
                
                if start_time:
                    query += ' AND timestamp >= ?'
                    params.append(start_time)
                
                if end_time:
                    query += ' AND timestamp <= ?'
                    params.append(end_time)
                
                query += ' ORDER BY timestamp ASC'
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                result = []
                for row in rows:
                    result.append({
                        'timestamp': row['timestamp'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                
                return result
                
        except Exception as e:
            logging.error(f"Error getting price data for {asset}: {e}")
            return []
    
    def get_database_stats(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total records
                cursor.execute('SELECT COUNT(*) FROM price_data')
                total_records = cursor.fetchone()[0]
                
                # Unique assets
                cursor.execute('SELECT COUNT(DISTINCT asset) FROM price_data')
                unique_assets = cursor.fetchone()[0]
                
                # Date range
                cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data')
                min_max = cursor.fetchone()
                date_range = (min_max[0], min_max[1])
                
                # Asset counts
                cursor.execute('''
                    SELECT asset, COUNT(*) as count 
                    FROM price_data 
                    GROUP BY asset 
                    ORDER BY count DESC
                ''')
                asset_counts = [{'asset': row[0], 'count': row[1]} for row in cursor.fetchall()]
                
                return {
                    'total_records': total_records,
                    'unique_assets': unique_assets,
                    'date_range': date_range,
                    'asset_counts': asset_counts
                }
                
        except Exception as e:
            logging.error(f"Error getting database stats: {e}")
            return {
                'total_records': 0,
                'unique_assets': 0,
                'date_range': (None, None),
                'asset_counts': []
            }
    
    def cleanup_old_data(self, older_than_days: int = 30) -> int:
        """
        Clean up data older than specified days
        
        Args:
            older_than_days: Remove data older than this many days
            
        Returns:
            Number of records deleted
        """
        try:
            cutoff_time = int(datetime.now().timestamp()) - (older_than_days * 24 * 60 * 60)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    DELETE FROM price_data 
                    WHERE timestamp < ?
                ''', (cutoff_time,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                logging.info(f"Cleaned up {deleted_count} records older than {older_than_days} days")
                return deleted_count
                
        except Exception as e:
            logging.error(f"Error cleaning up old data: {e}")
            return 0