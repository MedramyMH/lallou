"""
Simple IQ Option Data Fetcher
Fetches price data and saves to database
"""

import os
import time
import logging
from threading import Thread, Event
from datetime import datetime
from iqoptionapi.stable_api import IQ_Option
from database import PriceDatabase

# Apply IQ Option API fixes first
try:
    from iqoption_fix import apply_iqoption_fixes
    apply_iqoption_fixes()
except Exception as e:
    print(f"Warning: Could not apply IQ Option fixes: {e}")

class DataFetcher:
    def __init__(self, email, password, account_type="PRACTICE"):
        self.email = email
        self.password = password
        self.account_type = account_type
        self.api = None
        self.db = PriceDatabase()
        self.stop_event = Event()
        
        # Setup logging
        self._setup_logging()
        
        # Assets to monitor
        self.monitored_assets = [
            'EURUSD', 'GBPUSD', 'USDJPY', 'USDCAD', 'EURCAD',
            'AUDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
        ]
        
        # Timeframes in minutes
        self.timeframes = [1, 5, 15, 60]
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_fetcher.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Connect to IQ Option API"""
        try:
            self.logger.info("Connecting to IQ Option...")
            self.api = IQ_Option(self.email, self.password)
            
            # Connect to API
            check, reason = self.api.connect()
            if not check:
                self.logger.error(f"Connection failed: {reason}")
                return False
            
            # Change to practice account if needed
            self.api.change_balance(self.account_type)
            
            self.logger.info("✅ Successfully connected to IQ Option")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            return False

    def fetch_and_save_data(self):
        """Fetch current prices and save to database"""
        if not self.api:
            self.logger.error("Not connected to API")
            return
        
        for asset in self.monitored_assets:
            if self.stop_event.is_set():
                break
                
            for timeframe in self.timeframes:
                try:
                    # Get candle data
                    candles = self.api.get_candles(asset, timeframe * 60, 1, time.time())
                    
                    if candles and len(candles) > 0:
                        candle = candles[0]
                        
                        # Prepare data for database
                        price_data = {
                            'time': candle['from'],
                            'open': candle['open'],
                            'max': candle['max'],
                            'min': candle['min'],
                            'close': candle['close'],
                            'volume': candle.get('volume', 0)
                        }
                        
                        # Save to database
                        success = self.db.save_price_data(asset, price_data, timeframe)
                        
                        if success:
                            self.logger.debug(f"Saved {asset} M{timeframe}: {candle['close']:.5f}")
                        else:
                            self.logger.error(f"Failed to save {asset} M{timeframe}")
                    
                    # Small delay between requests
                    time.sleep(0.2)
                    
                except Exception as e:
                    self.logger.error(f"Error fetching {asset} M{timeframe}: {e}")
                    continue

    def start_fetching(self, interval=30):
        """Start continuous data fetching"""
        if not self.connect():
            self.logger.error("Failed to connect. Exiting.")
            return
        
        self.logger.info(f"Starting data fetching every {interval} seconds...")
        self.logger.info(f"Monitoring {len(self.monitored_assets)} assets")
        self.logger.info(f"Timeframes: {self.timeframes}")
        
        try:
            while not self.stop_event.is_set():
                start_time = time.time()
                
                # Fetch and save data
                self.fetch_and_save_data()
                
                # Log status
                stats = self.db.get_database_stats()
                self.logger.info(f"Database records: {stats.get('total_records', 0):,}")
                
                # Wait for next interval
                elapsed = time.time() - start_time
                sleep_time = max(1, interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            self.logger.info("Data fetching stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop data fetching"""
        self.stop_event.set()
        self.logger.info("Data fetcher stopped")

    def get_database_info(self):
        """Get database information"""
        return self.db.get_database_stats()

def main():
    """Main function to run the data fetcher"""
    # Configuration - replace with your credentials
    EMAIL = "mamora5027@gmail.com"
    PASSWORD = "Medramy12345*-"
    ACCOUNT_TYPE = "PRACTICE"  # or "REAL"
    FETCH_INTERVAL = 30  # seconds
    
    print("=" * 50)
    print("IQ Option Data Fetcher")
    print("=" * 50)
    
    # Create and start fetcher
    fetcher = DataFetcher(EMAIL, PASSWORD, ACCOUNT_TYPE)
    
    try:
        # Start fetching data
        fetcher.start_fetching(FETCH_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping data fetcher...")
        fetcher.stop()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Show final database stats
        stats = fetcher.get_database_info()
        print("\n" + "=" * 50)
        print("FINAL DATABASE STATISTICS")
        print("=" * 50)
        print(f"Total records: {stats.get('total_records', 0):,}")
        print(f"Unique assets: {stats.get('unique_assets', 0)}")
        
        if stats['date_range'][0] and stats['date_range'][1]:
            start = datetime.fromtimestamp(stats['date_range'][0])
            end = datetime.fromtimestamp(stats['date_range'][1])
            print(f"Data range: {start} to {end}")

if __name__ == "__main__":
    main()
