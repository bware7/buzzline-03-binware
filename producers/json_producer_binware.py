"""
json_producer_binware.py

Stream NBA player performance JSON data to a Kafka topic.

Example JSON message
{"player": "LeBron James", "team": "Lakers", "points": 28, "assists": 7, "rebounds": 9, "game_date": "2025-01-15"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data
from typing import Generator, Dict, Any
import random
from datetime import datetime, timedelta

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
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# NBA Data Generator
#####################################

# Sample NBA players and teams for realistic data
NBA_PLAYERS = [
    {"player": "LeBron James", "team": "Lakers"},
    {"player": "Stephen Curry", "team": "Warriors"},
    {"player": "Kevin Durant", "team": "Suns"},
    {"player": "Giannis Antetokounmpo", "team": "Bucks"},
    {"player": "Luka Doncic", "team": "Mavericks"},
    {"player": "Jayson Tatum", "team": "Celtics"},
    {"player": "Joel Embiid", "team": "76ers"},
    {"player": "Nikola Jokic", "team": "Nuggets"},
    {"player": "Damian Lillard", "team": "Bucks"},
    {"player": "Anthony Davis", "team": "Lakers"}
]

def generate_nba_performance() -> Dict[str, Any]:
    """Generate realistic NBA player performance data."""
    player_data = random.choice(NBA_PLAYERS)
    
    # Generate realistic stats
    points = random.randint(8, 45)
    assists = random.randint(0, 15)
    rebounds = random.randint(2, 18)
    
    # Add some correlation - better players tend to have higher stats
    if player_data["player"] in ["LeBron James", "Giannis Antetokounmpo", "Luka Doncic"]:
        points += random.randint(5, 15)
        assists += random.randint(2, 8)
        rebounds += random.randint(3, 7)
    
    game_date = datetime.now().strftime("%Y-%m-%d")
    
    performance = {
        "player": player_data["player"],
        "team": player_data["team"],
        "points": points,
        "assists": assists,
        "rebounds": rebounds,
        "game_date": game_date,
        "timestamp": datetime.now().isoformat()
    }
    
    return performance


def generate_messages() -> Generator[Dict[str, Any], None, None]:
    """
    Generate NBA performance messages continuously.

    Yields:
        dict: A dictionary containing NBA player performance data.
    """
    while True:
        try:
            nba_performance = generate_nba_performance()
            logger.debug(f"Generated NBA performance: {nba_performance}")
            yield nba_performance
            
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated NBA JSON messages to the Kafka topic.
    """

    logger.info("START NBA JSON producer.")
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

    # Generate and send messages
    logger.info(f"Starting NBA performance data production to topic '{topic}'...")
    try:
        for message_dict in generate_messages():
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=message_dict)
            logger.info(f"Sent NBA performance to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("NBA producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during NBA message production: {e}")
    finally:
        producer.close(timeout=None)
        logger.info("NBA Kafka producer closed.")

    logger.info("END NBA JSON producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()