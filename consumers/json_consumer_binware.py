"""
json_consumer_binware.py

Consume NBA performance JSON messages from a Kafka topic and perform real-time analytics.

Tracks player performance and alerts on exceptional performances (triple-doubles, high scoring games).

Example JSON message format:
{"player": "LeBron James", "team": "Lakers", "points": 28, "assists": 7, "rebounds": 9, "game_date": "2025-01-15"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting player stats

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Stores for NBA Analytics
#####################################

# Track player performance statistics
player_stats: defaultdict[str, dict] = defaultdict(lambda: {
    "games_played": 0,
    "total_points": 0,
    "total_assists": 0,
    "total_rebounds": 0,
    "triple_doubles": 0,
    "high_scoring_games": 0  # 30+ points
})

# Track team performance
team_stats: defaultdict[str, dict] = defaultdict(lambda: {
    "total_games": 0,
    "total_team_points": 0
})


#####################################
# NBA Analytics Functions
#####################################


def check_triple_double(points: int, assists: int, rebounds: int) -> bool:
    """Check if a performance qualifies as a triple-double (double digits in 3 categories)."""
    categories_double_digits = sum([points >= 10, assists >= 10, rebounds >= 10])
    return categories_double_digits >= 3


def check_high_scoring_game(points: int) -> bool:
    """Check if a performance qualifies as a high-scoring game (30+ points)."""
    return points >= 30


def analyze_player_performance(player_data: dict) -> None:
    """Analyze and update player performance statistics."""
    player = player_data.get("player", "unknown")
    team = player_data.get("team", "unknown")
    points = player_data.get("points", 0)
    assists = player_data.get("assists", 0)
    rebounds = player_data.get("rebounds", 0)
    
    # Update player stats
    stats = player_stats[player]
    stats["games_played"] += 1
    stats["total_points"] += points
    stats["total_assists"] += assists
    stats["total_rebounds"] += rebounds
    
    # Check for special achievements
    if check_triple_double(points, assists, rebounds):
        stats["triple_doubles"] += 1
        logger.info(f"🏀 TRIPLE-DOUBLE ALERT! {player} ({team}): {points}pts, {assists}ast, {rebounds}reb")
    
    if check_high_scoring_game(points):
        stats["high_scoring_games"] += 1
        logger.info(f"🔥 HIGH-SCORING GAME! {player} ({team}) scored {points} points!")
    
    # Update team stats
    team_stats[team]["total_games"] += 1
    team_stats[team]["total_team_points"] += points
    
    # Calculate and log averages
    avg_points = stats["total_points"] / stats["games_played"]
    avg_assists = stats["total_assists"] / stats["games_played"]
    avg_rebounds = stats["total_rebounds"] / stats["games_played"]
    
    logger.info(f"{player} season averages: {avg_points:.1f}pts, {avg_assists:.1f}ast, {avg_rebounds:.1f}reb")
    logger.info(f"{player} career: {stats['triple_doubles']} triple-doubles, {stats['high_scoring_games']} high-scoring games")


#####################################
# Function to process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a single NBA performance JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        from typing import Any
        message_dict: dict[str, Any] = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed NBA performance: {message_dict}")

        # Perform NBA analytics
        analyze_player_performance(message_dict)

        # Log current league leaders (every 10 games)
        total_messages = sum(stats["games_played"] for stats in player_stats.values())
        if total_messages % 10 == 0:
            logger.info("=== CURRENT LEAGUE LEADERS ===")
            
            # Points leader
            points_leader = max(player_stats.items(), 
                              key=lambda x: x[1]["total_points"] / x[1]["games_played"] if x[1]["games_played"] > 0 else 0)
            avg_points = points_leader[1]["total_points"] / points_leader[1]["games_played"]
            logger.info(f"Scoring Leader: {points_leader[0]} ({avg_points:.1f} PPG)")
            
            # Triple-double leader
            td_leader = max(player_stats.items(), key=lambda x: x[1]["triple_doubles"])
            logger.info(f"Triple-Double Leader: {td_leader[0]} ({td_leader[1]['triple_doubles']} triple-doubles)")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing NBA message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the NBA consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs real-time NBA analytics on messages from the Kafka topic.
    """
    logger.info("START NBA JSON consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"NBA Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling NBA performance messages from topic '{topic}'...")
    try:
        while True:
            # poll returns a dict: {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    # value_deserializer in utils_consumer already decoded this to str
                    message_str: str = msg.value
                    logger.debug(f"Received NBA message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("NBA consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming NBA messages: {e}")
    finally:
        consumer.close()
        
    logger.info(f"NBA Kafka consumer for topic '{topic}' closed.")
    logger.info(f"END NBA consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()