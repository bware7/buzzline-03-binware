# buzzline-03-binware

Streaming data does not have to be simple text.
Many of us are familiar with streaming video content and audio (e.g. music) files.

Streaming data can be structured (e.g. csv files) or
semi-structured (e.g. json data).

We'll work with two different types of data, and so we'll use two different Kafka topic names.
See [.env](.env).

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first.
**Python 3.11 is required.**

## Second, Copy This Example Project & Rename

1. Once the tools are installed, copy/fork this project into your GitHub account
   and create your own version of this project to run and experiment with.
2. Name it `buzzline-03-yourname` where yourname is something unique to you.

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>.

Use your README.md to record your workflow and commands.

---

## Task 0. If Windows, Start WSL

- Be sure you have completed the installation of WSL as shown in [https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md](https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md).

- We can keep working in **Windows VS Code** for editing, Git, and GitHub.
- When you need to run Kafka or Python commands, just open a **WSL terminal** from VS Code.

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window. 

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground.
5. Keep this terminal open - Kafka will run here
6. Watch for "started (kafka.server.KafkaServer)" message

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment
2. Activate the virtual environment
3. Upgrade pip
4. Install from requirements.txt

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Start Original Example Producers and Consumers

### JSON Example (Original)

Start a Kafka JSON Producer:

```shell
# Windows
.venv\Scripts\activate
py -m producers.json_producer_case

# Mac/Linux
source .venv/bin/activate
python3 -m producers.json_producer_case
```

Start a Kafka JSON Consumer (new terminal):

```shell
# Windows
.venv\Scripts\activate
py -m consumers.json_consumer_case

# Mac/Linux
source .venv/bin/activate
python3 -m consumers.json_consumer_case
```

### CSV Example (Original)

Start a Kafka CSV Producer:

```shell
# Windows
.venv\Scripts\activate
py -m producers.csv_producer_case

# Mac/Linux
source .venv/bin/activate
python3 -m producers.csv_producer_case
```

Start a Kafka CSV Consumer (new terminal):

```shell
# Windows
.venv\Scripts\activate
py -m consumers.csv_consumer_case

# Mac/Linux
source .venv/bin/activate
python3 -m consumers.csv_consumer_case
```

---

## Custom Implementations - NBA and Stock Market Streaming

### Custom JSON Producer and Consumer - NBA Player Performance

**NBA JSON Producer** (`json_producer_binware.py`):
- Generates realistic NBA player performance data
- Includes players from major teams (Lakers, Warriors, Celtics, etc.)
- Simulates game statistics: points, assists, rebounds
- Adds realistic correlation for star players

**NBA JSON Consumer** (`json_consumer_binware.py`):
- Tracks player performance analytics in real-time
- Detects triple-doubles (double digits in 3+ categories)
- Alerts on high-scoring games (30+ points)
- Calculates season averages and displays league leaders
- Provides comprehensive player and team statistics

Start the custom NBA streaming:

```shell
# NBA Producer (Windows)
.venv\Scripts\activate
py -m producers.json_producer_binware

# NBA Consumer (Windows - new terminal)
.venv\Scripts\activate
py -m consumers.json_consumer_binware
```

### Custom CSV Producer and Consumer - Stock Market Data

**Stock CSV Producer** (`csv_producer_binware.py`):
- Simulates real-time stock prices for 8 major tech companies
- Uses realistic price volatility for each stock
- Implements random walk price movement
- Generates volume data correlated with price movements

**Stock CSV Consumer** (`csv_consumer_binware.py`):
- Performs real-time market analytics
- Detects significant price movements (2%+ threshold)
- Identifies volume spikes (50% above recent average)
- Analyzes price trends (rising, falling, stable)
- Tracks market statistics (min/max prices, average volume)

Start the custom stock streaming:

```shell
# Stock Producer (Windows)
.venv\Scripts\activate
py -m producers.csv_producer_binware

# Stock Consumer (Windows - new terminal)
.venv\Scripts\activate
py -m consumers.csv_consumer_binware
```

### Custom Analytics Features

**NBA Analytics:**
- Triple-double detection and alerts
- High-scoring game notifications
- Season average calculations
- League leader tracking
- Team performance metrics

**Stock Market Analytics:**
- Price spike alerts (configurable threshold)
- Volume surge detection
- Trend analysis using rolling windows
- Market statistics and ranges
- Real-time market monitoring

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## About the Original Examples

### Smart Smoker (CSV Example)

A food stall occurs when the internal temperature of food plateaus or
stops rising during slow cooking, typically between 150°F and 170°F.
This happens due to evaporative cooling as moisture escapes from the
surface of the food. The plateau can last for hours, requiring
adjustments like wrapping the food or raising the cooking temperature to
overcome it. Cooking should continue until the food reaches the
appropriate internal temperature for safe and proper doneness.

The producer simulates a smart food thermometer, sending a temperature
reading every 15 seconds. The consumer monitors these messages and
maintains a time window of the last 5 readings.
If the temperature varies by less than 2 degrees, the consumer alerts
the BBQ master that a stall has been detected. This time window helps
capture recent trends while filtering out minor fluctuations.

### Buzz Messages (JSON Example)

The original JSON example demonstrates basic message streaming with author tracking and message counting analytics.

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Project Structure

```
buzzline-03-binware/
├── producers/
│   ├── json_producer_case.py          # Original JSON example
│   ├── csv_producer_case.py           # Original CSV example
│   ├── json_producer_binware.py       # Custom NBA data producer
│   └── csv_producer_binware.py        # Custom stock data producer
├── consumers/
│   ├── json_consumer_case.py          # Original JSON example
│   ├── csv_consumer_case.py           # Original CSV example
│   ├── json_consumer_binware.py       # Custom NBA analytics consumer
│   └── csv_consumer_binware.py        # Custom stock analytics consumer
├── data/
├── utils/
├── scripts/
└── README.md
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.