FROM python:3.9
WORKDIR /app
RUN apt-get update && apt-get install -y \
    libzbar0 \
    tesseract-ocr \
    libtesseract-dev \
    libleptonica-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
EXPOSE 8950
CMD ["python3", "main.py"]