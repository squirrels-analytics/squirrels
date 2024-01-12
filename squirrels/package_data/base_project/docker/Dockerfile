# Change here to use different python version (ex. 3.12-slim for version 3.12)
FROM python:3.11-slim
WORKDIR /app

# Needed if any python dependencies are installed from git, or for 
# "squirrels deps" if there are modules in "squirrels.yml"
RUN apt-get update && apt-get install -y git

COPY requirements-lock.txt .
RUN pip install --no-cache-dir -r requirements-lock.txt

COPY . .
RUN squirrels deps

CMD ["squirrels", "run", "--host", "0.0.0.0", "--port", "4465"]
