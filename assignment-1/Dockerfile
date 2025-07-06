FROM python:3.10-slim

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y openjdk-11-jre && rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --upgrade pip && pip install -r requirements.txt pyspark

# Copy the assignment-1 code
COPY . /app

CMD ["python", "-m", "code_elevate_app.main", "info_transportes.csv"]
