# Change here to use different python version (ex. 3.11-slim for version 3.11)
FROM python:3.12-slim
WORKDIR /app

COPY . .

# Only needed if there are python dependencies installed using git, or for the
# "sqrl deps" command if there are packages defined in "squirrels.yml"
RUN apt-get update && apt-get install -y git

RUN pip install --no-cache-dir -r requirements.txt

RUN sqrl deps

EXPOSE 4465
CMD ["sqrl", "run", "--build", "--host", "0.0.0.0", "--port", "4465"]
