# Step 1: Use an official lightweight Python image
FROM python:3.12-slim

# Step 2: Set working directory inside the container
WORKDIR /app

# Step 3: Copy your app files into the container
COPY . /app

# Step 4: Install system dependencies (if you need psycopg2, gdal, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Step 5: Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 6: Expose the port your app runs on
EXPOSE 8000

# Step 7: Set environment variables (you can override these later)
ENV PORT=8000

# Step 8: Run your app
CMD ["gunicorn", "app.app:server", "--bind", "0.0.0.0:8000"]
