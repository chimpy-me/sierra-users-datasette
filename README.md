# sierra-db-users-datasette

## A Datasette project for exploring Sierra PostgreSQL database users tables

This project uses [Datasette](https://datasette.io/) to provide a web interface for exploring the users tables in a Sierra PostgreSQL database. It is configured to connect to the Sierra database and includes settings to handle long-running queries.

Running the main script will generate a SQLite database from the Sierra PostgreSQL database's users tables, which can then be served using Datasette.

## Setup

### Requirements

- uv (for environment management)

1. **Clone the repository**:

   ```bash
   git clone git@github.com:chimpy-me/sierra-users-datasette.git
   cd sierra-users-datasette/
   ```

2. **Use uv to manage your environment**:

   ```bash
   uv sync
   ```

3. **Create a `.env` file**:
    Copy the provided `.env.sample` to `.env` and update the `SIERRA_PG_URL` with your actual database connection string.

    NOTE: don't forget to set username and password in the connection string.

    ```bash
    cp .env.sample .env
    ```

4. **Generate the database and serve the Datasette instance**:

    ```bash
    uv run python main.py
    uv run datasette .
    ```


## TODO/Improvements

- Foreign key relationships between users tables for better navigation within Datasette.
- Add more views and canned queries for common user data explorations.
- Add data definitions and documentation for the users tables.
