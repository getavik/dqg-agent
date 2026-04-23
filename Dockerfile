FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for potential future visual libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download NLTK data for Profiling (avoids runtime download errors)
RUN python -c "import nltk; nltk.download(['punkt', 'stopwords', 'averaged_perceptron_tagger', 'punkt_tab', 'averaged_perceptron_tagger_eng'])"

# Copy source code
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Run instructions
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
