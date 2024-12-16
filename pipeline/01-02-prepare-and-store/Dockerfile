FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /code

# Copy the current directory contents into the container at /code
COPY . /code

# Copy the mappings folder into the container at /code/mappings
COPY mappings /code/mappings

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install jq
RUN apt-get update && apt-get install -y jq

# Copy rabbitmqadmin from the current folder to /usr/local/bin
COPY rabbitmqadmin /usr/local/bin/rabbitmqadmin

# Make rabbitmqadmin executable
RUN chmod +x /usr/local/bin/rabbitmqadmin

# Make the scripts executable
RUN chmod +x /code/record.sh /code/main.sh