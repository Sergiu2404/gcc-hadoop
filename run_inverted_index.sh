#!/bin/bash

set -e  # stop on errorrs

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOKS_DIR="$PROJECT_DIR/books"
JAVA_FILE="$PROJECT_DIR/InvertedIndex.java"
STOPWORDS_FILE="$PROJECT_DIR/stopwords.txt"
CONTAINER="namenode"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

info "Checking requirements..."

[ -f "$JAVA_FILE" ]      || error "InvertedIndex.java not found at $JAVA_FILE"
[ -f "$STOPWORDS_FILE" ] || error "stopwords.txt not found at $STOPWORDS_FILE"
[ -d "$BOOKS_DIR" ]      || error "input/ directory not found at $BOOKS_DIR. Create it and put your .txt books inside."

BOOK_COUNT=$(ls "$BOOKS_DIR"/*.txt 2>/dev/null | wc -l)
[ "$BOOK_COUNT" -gt 0 ] || error "No .txt files found in $BOOKS_DIR"
info "Found $BOOK_COUNT book(s) in input/"

docker ps --filter "name=^${CONTAINER}$" --filter "status=running" | grep -q "$CONTAINER" \
  || error "Container '$CONTAINER' is not running. Start the cluster first with: docker compose up -d"

info "All checks passed."

info "Copying InvertedIndex.java and stopwords.txt into container..."
docker cp "$JAVA_FILE" "$CONTAINER":/tmp/InvertedIndex.java
docker cp "$STOPWORDS_FILE" "$CONTAINER":/tmp/stopwords.txt

info "Copying book files into container..."
docker exec "$CONTAINER" bash -c "mkdir -p /tmp/books && ls -ld /tmp/books"
for book in "$BOOKS_DIR"/*.txt; do
  bookname=$(basename "$book")
  info "  -> $bookname"
  docker cp "$book" "$CONTAINER":/tmp/books/"$bookname"
done

info "Compiling InvertedIndex.java inside the container..."
docker exec "$CONTAINER" bash -c '
  cd /tmp
  rm -rf classes InvertedIndex.jar
  mkdir -p classes
  javac -classpath $(hadoop classpath) -d classes InvertedIndex.java
  jar -cvf InvertedIndex.jar -C classes .
  echo "Compilation done."
'
info "Setting up HDFS directories..."
docker exec "$CONTAINER" bash -c '
  hdfs dfs -rm -r -f /user/root/input
  hdfs dfs -rm -r -f /user/root/output
  hdfs dfs -rm -r -f /user/root/stopwords
  hdfs dfs -mkdir -p /user/root/input
  hdfs dfs -mkdir -p /user/root/stopwords
'

info "Uploading books to HDFS..."
docker exec "$CONTAINER" bash -c '
  hdfs dfs -put /tmp/books/* /user/root/input/
  hdfs dfs -put /tmp/stopwords.txt /user/root/stopwords/stopwords.txt
  echo "Files in HDFS input:"
  hdfs dfs -ls /user/root/input/
'

info "Running InvertedIndex MapReduce job..."
docker exec "$CONTAINER" bash -c '
  hadoop jar /tmp/InvertedIndex.jar InvertedIndex \
    /user/root/input \
    /user/root/output \
    /user/root/stopwords/stopwords.txt
'
info "Job complete! First 50 lines of output:"
echo "-----------------------------------------------"
docker exec "$CONTAINER" bash -c 'hdfs dfs -cat /user/root/output/part-r-00000 | head -50'
echo "-----------------------------------------------"
info "To see the full output run:"
echo "  docker exec $CONTAINER hdfs dfs -cat /user/root/output/part-r-00000"
info "To copy the output to your local machine:"
echo "  docker exec $CONTAINER hdfs dfs -get /user/root/output/part-r-00000 /tmp/result.txt"
echo "  docker cp $CONTAINER:/tmp/result.txt $PROJECT_DIR/result.txt"