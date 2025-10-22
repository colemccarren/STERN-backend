# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Tell Cloud Run what command to run to start the server
# It runs "gunicorn" which serves your Flask "app" from the "main.py" file
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "main:app"]
