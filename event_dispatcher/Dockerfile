# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory to /app
WORKDIR /app

# Copy only the requirements.txt for now into the container at /app
ADD ./requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Define environment variables
ENV NAME event_dispatcher
ENV TZ Asia/Manila
ENV PYTHONIOENCODING UTF-8
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONPATH "/app/common_lib/"

# Copy the current directory contents into the container at /app
ADD . /app

# Use Nameko to run the micro-service when the container launches
CMD ["python", "-u", "event_dispatcher.py"]
