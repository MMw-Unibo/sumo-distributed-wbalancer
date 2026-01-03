#!/usr/bin/env bash

set -e  # exit on error

### CONFIG ###
VENV_DIR=".venv"

echo "=== environment setup ==="

### 1. Check and install SUMO ###
if ! command -v sumo >/dev/null 2>&1; then
    echo "[INFO] SUMO not found. Installing..."
    sudo add-apt-repository -y ppa:sumo/stable
    sudo apt-get update
    sudo apt-get install -y sumo sumo-tools sumo-doc
else
    echo "[OK] SUMO is already installed."
fi

### 2. Check and install pip ###
if ! command -v pip3 >/dev/null 2>&1; then
    echo "[INFO] pip not found. Installing..."
    sudo apt-get update
    sudo apt-get install -y python3-pip
else
    echo "[OK] pip is already installed."
fi

### 3. Check and install python venv ###
if ! dpkg -s python3-venv >/dev/null 2>&1; then
    echo "[INFO] python3-venv not found. Installing..."
    sudo apt-get install -y python3-venv
else
    echo "[OK] python3-venv is already installed."
fi

### 6. Set permanent environment variables ###
BASHRC="$HOME/.bashrc"

if ! grep -q "SUMO_HOME" "$BASHRC"; then
    echo "[INFO] Setting SUMO_HOME permanently..."
    echo 'export SUMO_HOME="/usr/share/sumo"' >> "$BASHRC"
fi

if ! grep -q "PROJECT_FOLDER" "$BASHRC"; then
    echo "[INFO] Setting PROJECT_FOLDER permanently..."
    echo "export PROJECT_FOLDER=\"$(pwd)\"" >> "$BASHRC"
fi

if ! grep -q "SIM_FOLD" "$BASHRC"; then
    echo "[INFO] Setting SIM_FOLD permanently..."
    echo "export SIM_FOLD=\"$(pwd)/simulations\"" >> "$BASHRC"
fi

# Apply variables for current session
export SUMO_HOME="/usr/share/sumo"
export PROJECT_FOLDER="$(pwd)"
export SIM_FOLD="$PROJECT_FOLDER/simulations"

SIM_RESULTS_FOLDER="$SIM_FOLDER/simulation_results"
LOG_FOLDER="$PROJECT_FOLDER/logs"

if [ ! -d "$SIM_RESULTS_FOLDER" ]; then
    echo "[INFO] Creating simulation_results directory..."
    mkdir -p "$SIM_RESULTS_FOLDER"
fi

if [ ! -d "$LOG_FOLDER" ]; then
    echo "[INFO] Creating log directory..."
    mkdir -p "$LOG_FOLDER"
fi

### 7. Create Python virtual environment ###
if [ ! -d "$VENV_DIR" ]; then
    echo "[INFO] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "[OK] Virtual environment already exists."
fi

### 8. Activate venv and install requirements ###
echo "[INFO] Activating virtual environment and installing requirements..."
source "$VENV_DIR/bin/activate"

pip install --upgrade pip

if [ -f requirements.txt ]; then
    pip install -r requirements.txt
else
    echo "[WARN] requirements.txt not found."
fi

echo "=== Setup complete ==="
echo "Please run 'source ~/.bashrc' or open a new terminal to apply environment variables."
