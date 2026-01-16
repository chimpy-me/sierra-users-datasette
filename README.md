# sierra-db-users-datasette

## A Datasette project for exploring Sierra PostgreSQL database users tables

This project uses [Datasette](https://datasette.io/) to provide a web interface for exploring the users tables in a Sierra PostgreSQL database. It is configured to connect to the Sierra database and includes settings to handle long-running queries.

## Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourusername/sierra-db-users-datasette.git
    cd sierra-db-users-datasette

    ```

2. **Use uv to manage your environment**:

   ```bash
   uv sync
   uv activate
   ```

3. **Create a `.env` file**:
    Copy the provided `.env.sample` to `.env` and update the `SIERRA_PG_URL` with your actual database connection string.

    ```bash
    cp .env.sample .env
    ```

4. **Generate the database and serve the Datasette instance**:

    ```bash
    uv run python main.py
    datasette serve --config datasette.yml
    ```
