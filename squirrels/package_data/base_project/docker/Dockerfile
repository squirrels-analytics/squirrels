# Change here to use different python version (ex. 3.11-slim for version 3.11)
FROM python:3.12-slim
WORKDIR /app

COPY . .

# Needed if any python dependencies are installed from git, or for 
# "squirrels deps" command if there are packages defined in "squirrels.yml"
RUN apt-get update && apt-get install -y git

RUN pip install --no-cache-dir -r requirements-lock.txt

RUN squirrels deps

EXPOSE 4465
CMD ["squirrels", "run", "--host", "0.0.0.0", "--port", "4465"]
