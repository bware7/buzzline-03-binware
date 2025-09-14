"""
csv_consumer_binware.py

Consume stock price JSON messages from a Kafka topic and perform real-time market analytics.

Monitors stock prices and alerts on significant price movements, volume spikes, and market trends.

Example message format:
{"timestamp": "2025-01-15T10:30:00Z", "symbol": "AAPL", "price": 175.25, "volume": 1500000}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque, defaultdict

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_price_alert_threshold() -> float:
    """Fetch price change alert threshold from environment or use default."""
    threshold = float(os.getenv("STOCK_PRICE_ALERT_THRESHOLD", 2.0))
    logger.info(f"Price change alert threshold: {threshold}%")
    return threshold


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("STOCK_ROLLING_WINDOW_SIZE", 10))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Stock Market Analytics Setup
#####################################

# Track price history for each stock (rolling window)
price_windows = defaultdict(lambda: deque(maxlen=get_rolling_window_size()))

# Track stock statistics
stock_stats = defaultdict(lambda: {
    "total_volume": 0,
    "message_count": 0,
    "min_price": float('inf'),
    "max_price": 0,
    "price_changes": []
})

# Volume tracking for spike detection
volume_windows = defaultdict(lambda: deque(maxlen=5))  # Last 5 volume readings


#####################################
# Market Analysis Functions
#####################################


def detect_price_spike(symbol: str, current_price: float, change_percent: float) -> bool:
    """
    Detect significant price movements.
    
    Args:
        symbol (str): Stock symbol
        current_price (float): Current stock price
        change_percent (float): Price change percentage
    
    Returns:
        bool: True if significant price movement detected
    """
    threshold = get_price_alert_threshold()
    return abs(change_percent) >= threshold


def detect_volume_spike(symbol: str, current_volume: int) -> bool:
    """
    Detect volume spikes (volume significantly higher than recent average).
    
    Args:
        symbol (str): Stock symbol
        current_volume (int): Current trading volume
    
    Returns:
        bool: True if volume spike detected
    """
    window = volume_windows[symbol]
    if len(window) < 3:  # Need some history
        return False
    
    avg_volume = sum(window) / len(window)
    # Volume spike if current volume is 50% higher than recent average
    return current_volume > avg_volume * 1.5


def analyze_price_trend(symbol: str) -> str:
    """
    Analyze price trend based on recent price history.
    
    Args:
        symbol (str): Stock symbol
    
    Returns:
        str: Trend description ('rising', 'falling', 'stable', 'insufficient_data')
    """
    window = price_windows[symbol]
    if len(window) < 5:
        return "insufficient_data"
    
    prices = list(window)
    recent_prices = prices[-3:]  # Last 3 prices
    older_prices = prices[-6:-3]  # Previous 3 prices
    
    recent_avg = sum(recent_prices) / len(recent_prices)
    older_avg = sum(older_prices) / len(older_prices)
    
    change_percent = ((recent_avg - older_avg) / older_avg) * 100
    
    if change_percent > 1.0:
        return "rising"
    elif change_percent < -1.0:
        return "falling"
    else:
        return "stable"


def update_stock_statistics(symbol: str, price: float, volume: int, change_percent: float) -> None:
    """Update running statistics for a stock."""
    stats = stock_stats[symbol]
    
    stats["total_volume"] += volume
    stats["message_count"] += 1
    stats["min_price"] = min(stats["min_price"], price)
    stats["max_price"] = max(stats["max_price"], price)
    stats["price_changes"].append(change_percent)
    
    # Keep only recent price changes (last 20)
    if len(stats["price_changes"]) > 20:
        stats["price_changes"] = stats["price_changes"][-20:]


#####################################
# Function to process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a JSON stock price message and perform market analytics.

    Args:
        message (str): JSON message received from Kafka.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        symbol = data.get("symbol")
        price = data.get("price")
        volume = data.get("volume")
        timestamp = data.get("timestamp")
        change_percent = data.get("change_percent", 0)
        
        logger.info(f"Processed stock data: {data}")

        # Ensure required fields are present
        if not all([symbol, price, volume, timestamp]):
            logger.error(f"Invalid stock message format: {message}")
            return

        # Update price and volume windows
        price_windows[symbol].append(price)
        volume_windows[symbol].append(volume)
        
        # Update statistics
        update_stock_statistics(symbol, price, volume, change_percent)

        # Check for price spikes
        if detect_price_spike(symbol, price, change_percent):
            direction = "UP" if change_percent > 0 else "DOWN"
            logger.info(f"📈 PRICE ALERT! {symbol} moved {direction} {abs(change_percent):.2f}% to ${price:.2f}")

        # Check for volume spikes
        if detect_volume_spike(symbol, volume):
            logger.info(f"📊 VOLUME SPIKE! {symbol} trading volume: {volume:,} shares")

        # Analyze trends (every 10th message for each stock)
        stats = stock_stats[symbol]
        if stats["message_count"] % 10 == 0:
            trend = analyze_price_trend(symbol)
            avg_volume = stats["total_volume"] / stats["message_count"]
            price_range = stats["max_price"] - stats["min_price"]
            
            logger.info(f"=== {symbol} ANALYSIS ===")
            logger.info(f"Current: ${price:.2f} | Trend: {trend} | Range: ${price_range:.2f}")
            logger.info(f"Avg Volume: {avg_volume:,.0f} | Min: ${stats['min_price']:.2f} | Max: ${stats['max_price']:.2f}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for stock message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing stock message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the stock market consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes stock price messages from the Kafka topic.
    """
    logger.info("START stock market consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    alert_threshold = get_price_alert_threshold()
    
    logger.info(f"Stock Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")
    logger.info(f"Price alert threshold: {alert_threshold}%")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling stock price messages from topic '{topic}'...")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    message_str: str = msg.value
                    logger.debug(f"Received stock message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Stock consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming stock messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Stock Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()