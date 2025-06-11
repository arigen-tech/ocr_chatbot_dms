# AI Model: Search OCR & Chatbot

## Overview
This project integrates two AI models into a single FastAPI backend framework:

1. **Search OCR for PDF Documents** - Extracts and searches text from Documents in multiple file extensions.
2. **Chatbot** - A documentation-based chatbot leveraging pdfplumber

All functionalities are exposed through API gateways, including:
- Chat History
- Documentation Updates
- Chat Messaging
- Database Clearing
- Search OCR (Single Document & Multiple Documents)

## Features
- **OCR Processing:** Converts scanned Documents into searchable text.
- **Chatbot:** Provides intelligent responses based on documentation.
- **API Integration:** All models are accessible via FastAPI endpoints.
- **Database Support:** Enables document updates and search functionalities.

## Installation
### Prerequisites
- Python 3.11+
- Tesseract OCR ([Installation Guide](https://github.com/tesseract-ocr/tesseract))
- Install zbar (required for non-Windows systems):
    macOS: brew install zbar
    Linux: sudo apt-get install libzbar0
    Windows: No extra installation needed; pyzbar includes zbar.

### Setup
1. **Clone the repository**
   ```sh
   git clone <repo-url>
   cd <project-directory>
   ```

2. **Create a virtual environment**
   ```sh
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```sh
   pip install -r requirements.txt
   ```

## Running the Project
1. **Start FastAPI server**
   ```sh
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```
2. **Access API documentation**
   - Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)
   - ReDoc: [http://localhost:8000/redoc](http://localhost:8000/redoc)

## API Endpoints
### OCR Endpoints
- `POST /search/selected` - Search within a single PDF document.
- `POST /search/all` - Search across multiple PDFs.
- `DELETE /clean` - Clear OCR database.

### Chatbot Endpoints
- `POST /chat/message` - Send a message and receive a response.
- `GET /chat/history` - Retrieve chat history.
- `POST /chat/upload` - Update documentation for chatbot.
- `DELETE /chat/clean` - Clear chatbot database.

## Technologies Used
- **FastAPI** - API Framework
- **Tesseract OCR** - Text Extraction

## License
MIT License

## Contribution
Feel free to open issues and submit pull requests to enhance this project.

