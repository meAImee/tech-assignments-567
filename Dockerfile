# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy all files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 6543 for FastAPI
EXPOSE 6543

# Run FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "6543", "--reload"]
