FROM python:3.9

# Install dependencies required for installing Poetry
RUN apt-get update && apt-get install -y curl && apt-get clean

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python -

RUN pip wheel --no-cache-dir --no-use-pep517 "psycopg2==2.9.9"

# Make sure Poetry is available in the PATH
ENV PATH="${PATH}:/root/.local/bin"

# Copy only the necessary files for Poetry to install dependencies
COPY . .

# Install dependencies using Poetry
RUN poetry install --no-root --no-interaction

# Set the command to run the application
CMD ["poetry", "run", "python", "core/main.py"]
