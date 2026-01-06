# FinanceLake - Data Storage Layer Installation Guide

## Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for Apache Spark)  - Download from: https://adoptium.net/
  - Set `JAVA_HOME` environment variable
  - Add Java bin directory to PATH- Access to AWS S3 (for production deployment)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aminotma/FinanceLake.git
   cd FinanceLake
   ```

2. **Install Python packages:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```bash
   python -c "from pyspark.sql import SparkSession; from delta import DeltaTable; print('Installation successful!')"
   ```

## Required Packages

- **pyspark==4.0.1**: Apache Spark Python API for distributed data processing
- **delta-spark==4.0.0**: Delta Lake integration for ACID transactions on data lakes

## Configuration

### Environment Variables
Set the following environment variables for AWS S3 access:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### Spark Configuration
The code automatically configures Spark with Delta Lake support. For custom configurations, modify the `SparkSession` builder in `data_storage.py`.

## Usage

See `data_storage.py` for complete implementation examples including:
- Delta Lake table operations
- Data partitioning and optimization
- Time travel queries
- Backup and maintenance operations

## Architecture Documentation

See `data_storage_design.md` for detailed design documentation and architecture diagrams.

## Testing

Run the example code:
```bash
python data_storage.py
```

Test type annotations:
```bash
python test_types.py
```

## Troubleshooting

### Common Issues

1. **Java not found and JAVA_HOME environment variable is not set**
   - **Solution**: Install Java 8+ and set environment variables:
     ```bash
     # Windows
     set JAVA_HOME=C:\path\to\java
     set PATH=%PATH%;%JAVA_HOME%\bin

     # Linux/macOS
     export JAVA_HOME=/path/to/java
     export PATH=$PATH:$JAVA_HOME/bin
     ```
   - Verify installation: `java -version`

2. **PySpark import errors**
   - Ensure packages are installed in the correct Python environment
   - Try reinstalling: `pip uninstall pyspark delta-spark && pip install -r requirements.txt`

3. **S3 access denied**
   - Verify AWS credentials and permissions
   - Check IAM roles and bucket policies

4. **Delta Lake table not found**
   - Ensure the table path exists
   - Check file permissions for local paths
   - Verify S3 bucket access for cloud paths

- Increase Spark executor memory: `spark.executor.memory=4g`
- Adjust parallelism: `spark.sql.shuffle.partitions=200`
- Enable adaptive query execution: `spark.sql.adaptive.enabled=true`