# Spring Boot Delta Lake Example API

This is a simple Spring Boot application demonstrating how to read data from Parquet and Delta Lake formats using Apache Spark.

---

## Overview

The application initializes by creating a sample Parquet file with example data. It exposes two REST API endpoints:

- One endpoint reads and returns data directly from the Parquet file.
- The other endpoint reads data from a Delta Lake table.

This example serves as a basic evaluation of querying Delta Lake data in a Spring Boot context, using Spark for data processing.

---

## API Endpoints

### 1. Get Parquet Data

`GET /api/parquet-data`

Returns a JSON array with the content read from the Parquet file.

**Sample Response:**

```json
[
  {
    "id": 1,
    "nome": "João",
    "idade": 30
  },
  {
    "id": 2,
    "nome": "Maria",
    "idade": 25
  }
]
```

### 2. Get Delta Data

`GET /api/delta-data`

Returns a JSON array with the content read from the Delta Lake table.

**Sample Response:**

```json
[
  {
    "id": 1,
    "nome": "João",
    "idade": 30
  },
  {
    "id": 2,
    "nome": "Maria",
    "idade": 25
  }
]
```

---

## Implementation Notes

- On application startup, the service checks if the sample Parquet and Delta tables exist; if not, it creates them automatically.
- Apache Spark is used as the underlying engine to read both Parquet and Delta data formats.
- The REST APIs leverage Spark to load the data, convert it to JSON, and return it to the client.

---

## How to Run

- Ensure you have Java 11+ and Maven installed.
- Clone this repository.
- Build and run the application:

```bash
mvn clean spring-boot:run
```

- Access the APIs via HTTP:

```
http://localhost:8080/api/parquet-data
http://localhost:8080/api/delta-data
```

---

## Dependencies

- Spring Boot
- Apache Spark (with Delta Lake integration)
- Jackson (for JSON serialization)
