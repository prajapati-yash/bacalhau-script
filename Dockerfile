# Use an official Python runtime as a parent image
FROM python:3.8

COPY ./requirements.txt /requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run fetch_delegates_addresses.py when the container launches
CMD ["python", "fetch_delegates_addresses.py"]
