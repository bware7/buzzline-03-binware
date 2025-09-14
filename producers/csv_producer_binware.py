"""
csv_producer_binware.py

Stream stock price data to a Kafka topic as JSON messages.

Simulates real-time stock price updates for major tech companies.
CSV data is converted to JSON format for streaming.

Example message format:
{"timestamp": "2025-01-15T10:30:00Z", "symbol": "AAPL", "price": 175.25, "volume": 1500000}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
import json  # work with JSON data
import random
from datetime import datetime, timedelta
from typing import Dict, Any

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Stock Data Configuration
#####################################

# Major tech stocks with realistic base prices (as of early 2025)
STOCK_SYMBOLS = {
    "AAPL": {"base_price": 175.0, "volatility": 0.02},  # Apple
    "MSFT": {"base_price": 420.0, "volatility": 0.018}, # Microsoft
    "GOOGL": {"base_price": 165.0, "volatility": 0.025}, # Google
    "AMZN": {"base_price": 180.0, "volatility": 0.022},  # Amazon
    "TSLA": {"base_price": 250.0, "volatility": 0.04},   # Tesla (higher volatility)
    "NVDA": {"base_price": 900.0, "volatility": 0.035},  # NVIDIA
    "META": {"base_price": 550.0, "volatility": 0.028},  # Meta
    "NFLX": {"base_price": 650.0, "volatility": 0.03}    # Netflix
}

# Track current prices to simulate realistic market movement
current_prices = {symbol: data["base_price"] for symbol, data in STOCK_SYMBOLS.items()}

#####################################
# Stock Price Generator
#####################################


def generate_stock_price(symbol: str) -> Dict[str, Any]:
    """
    Generate realistic stock price movement using random walk.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL')
    
    Returns:
        dict: Stock price data with timestamp, symbol, price, and volume
    """
    stock_info = STOCK_SYMBOLS[symbol]
    volatility = stock_info["volatility"]
    
    # Random walk: price change between -volatility and +volatility
    price_change_percent = random.uniform(-volatility, volatility)
    price_change = current_prices[symbol] * price_change_percent
    
    # Update current price (with some bounds checking)
    new_price = current_prices[symbol] + price_change
    
    # Prevent prices from going negative or too far from base
    base_price = stock_info["base_price"]
    min_price = base_price * 0.5  # Don't drop below 50% of base
    max_price = base_price * 2.0  # Don't rise above 200% of base
    
    new_price = max(min_price, min(max_price, new_price))
    current_prices[symbol] = new_price
    
    # Generate realistic volume (higher volume during big price moves)
    base_volume = random.randint(500000, 2000000)
    if abs(price_change_percent) > volatility * 0.7:  # Big move
        volume = int(base_volume * random.uniform(1.5, 3.0))
    else:
        volume = base_volume
    
    stock_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "symbol": symbol,
        "price": round(new_price, 2),
        "volume": volume,
        "change_percent": round(price_change_percent * 100, 3)
    }
    
    return stock_data


def generate_messages():
    """
    Generate stock price messages continuously, cycling through symbols.
    
    Yields:
        dict: Stock price data formatted as JSON-ready dictionary
    """
    symbols = list(STOCK_SYMBOLS.keys())
    symbol_index = 0
    
    while True:
        try:
            # Cycle through stocks
            symbol = symbols[symbol_index]
            symbol_index = (symbol_index + 1) % len(symbols)
            
            stock_message = generate_stock_price(symbol)
            logger.debug(f"Generated stock data: {stock_message}")
            yield stock_message
            
        except Exception as e:
            logger.error(f"Unexpected error in stock message generation: {e}")
            sys.exit(3)


#####################################
# Define main function for this module
#####################################


def main():
    """
    Main entry point for the stock price producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams stock price messages to the Kafka topic.
    """

    logger.info("START stock price CSV producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Initialize starting message
    logger.info("Initializing stock prices...")
    for symbol, data in STOCK_SYMBOLS.items():
        logger.info(f"{symbol}: ${data['base_price']:.2f} (volatility: {data['volatility']*100:.1f}%)")

    # Generate and send messages
    logger.info(f"Starting stock price data production to topic '{topic}'...")
    try:
        for stock_message in generate_messages():
            producer.send(topic, value=stock_message)
            logger.info(f"Sent stock update to topic '{topic}': {stock_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Stock producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during stock message production: {e}")
    finally:
        producer.close()
        logger.info("Stock Kafka producer closed.")

    logger.info("END stock price CSV producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()